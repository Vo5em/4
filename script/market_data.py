import asyncio
import json
import logging
from time import monotonic

import aiohttp
import websockets

from .engine import EngineMode, TradingEngine
from .orderbook import LocalOrderBook
from .scanner import find_nearby_levels, find_walls
from .settings import PairSettings
from .trade_flow import TradeFlow
from .wall_tracker import WallTracker


BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"
BYBIT_REST_URL = "https://api.bybit.com/v5/market/orderbook"
DEFAULT_SYMBOL = "BTCUSDT"
CATEGORY = "linear"
logger = logging.getLogger(__name__)


async def get_orderbook_snapshot(symbol: str) -> dict:
    params = {"category": CATEGORY, "symbol": symbol, "limit": 1000}
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(BYBIT_REST_URL, params=params) as response:
            response.raise_for_status()
            data = await response.json()
    if data.get("retCode") != 0:
        raise RuntimeError(f"Bybit REST returned retCode={data.get('retCode')}")
    return data["result"]


def _check_subscription(payload: dict) -> None:
    if payload.get("op") == "subscribe" and payload.get("success") is False:
        raise RuntimeError("Bybit rejected the WebSocket subscription")


class SnapshotRetryRequired(RuntimeError):
    """The REST snapshot is older than the buffered delta stream."""


def find_first_delta_after_snapshot(snapshot: dict, buffer: list[dict]) -> int | None:
    """Return the first delta after a matching snapshot, or request a fresh snapshot."""
    if buffer and snapshot["seq"] < buffer[0]["seq"]:
        raise SnapshotRetryRequired("REST snapshot is older than the first buffered delta")
    for index, delta in enumerate(buffer):
        if delta["seq"] < snapshot["seq"]:
            continue
        if delta["seq"] == snapshot["seq"]:
            if delta["u"] != snapshot["u"]:
                raise SnapshotRetryRequired(
                    "Snapshot and WebSocket delta disagree at the same seq"
                )
            return index + 1
        raise SnapshotRetryRequired(
            "Buffered deltas passed the snapshot without a matching seq"
        )
    return None


def apply_orderbook_delta(orderbook: LocalOrderBook, delta: dict) -> None:
    """Apply the next full-depth update, ignore stale updates, or request resync."""
    if delta["u"] == 1:
        raise RuntimeError("Orderbook update ID reset; resynchronization required")
    if delta["u"] <= orderbook.update_id:
        return
    if delta["u"] != orderbook.update_id + 1:
        raise RuntimeError("Orderbook update ID gap; resynchronization required")
    orderbook.apply_delta(delta)


def _update_flow(trade_flow: TradeFlow, payload: dict) -> float | None:
    last_price = None
    for trade in payload.get("data", []):
        last_price = float(trade["p"])
        trade_flow.add_trade(
            price=last_price,
            quantity=float(trade["v"]),
            side=trade["S"],
        )
    return last_price


async def _fetch_snapshot_while_buffering(
    websocket,
    symbol: str,
    trade_flow: TradeFlow,
    buffered_deltas: list[dict],
) -> tuple[dict, float | None]:
    snapshot_task = asyncio.create_task(get_orderbook_snapshot(symbol))
    last_price = None
    try:
        while not snapshot_task.done():
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            payload = json.loads(message)
            _check_subscription(payload)
            topic = payload.get("topic")
            if topic == f"orderbook.full.{symbol}":
                buffered_deltas.append(payload["data"])
                if len(buffered_deltas) > 10000:
                    raise RuntimeError("Too many buffered deltas during snapshot sync")
            elif topic == f"publicTrade.{symbol}":
                last_price = _update_flow(trade_flow, payload) or last_price
        return await snapshot_task, last_price
    finally:
        if not snapshot_task.done():
            snapshot_task.cancel()
            await asyncio.gather(snapshot_task, return_exceptions=True)


async def _run_connection(
    symbol: str,
    engine: TradingEngine,
    pair_settings: PairSettings,
) -> None:
    topics = [f"orderbook.full.{symbol}", f"publicTrade.{symbol}"]
    trade_flow = TradeFlow(window_seconds=10)
    wall_tracker = WallTracker(
        min_age_seconds=pair_settings.min_wall_age_seconds,
        consumed_ratio=pair_settings.consumed_ratio,
    )
    last_price = None
    stable_walls = []
    pending_reconciled = False

    async with websockets.connect(
        BYBIT_WS_URL,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=5,
    ) as websocket:
        await websocket.send(json.dumps({"op": "subscribe", "args": topics}))
        buffered_deltas = []
        snapshot, snapshot_last_price = await _fetch_snapshot_while_buffering(
            websocket, symbol, trade_flow, buffered_deltas
        )
        last_price = snapshot_last_price or last_price
        snapshot_refreshes = 0
        while True:
            try:
                first_index = find_first_delta_after_snapshot(snapshot, buffered_deltas)
            except SnapshotRetryRequired:
                snapshot_refreshes += 1
                if snapshot_refreshes > 5:
                    raise RuntimeError(
                        "Could not obtain a REST snapshot aligned with the WebSocket stream"
                    )
                snapshot, snapshot_last_price = await _fetch_snapshot_while_buffering(
                    websocket, symbol, trade_flow, buffered_deltas
                )
                last_price = snapshot_last_price or last_price
                continue
            if first_index is not None:
                break
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            payload = json.loads(message)
            _check_subscription(payload)
            topic = payload.get("topic")
            if topic == f"publicTrade.{symbol}":
                last_price = _update_flow(trade_flow, payload) or last_price
            elif topic == f"orderbook.full.{symbol}":
                buffered_deltas.append(payload["data"])
                if len(buffered_deltas) > 10000:
                    raise RuntimeError("Too many buffered deltas during snapshot sync")

        orderbook = LocalOrderBook()
        orderbook.load_snapshot(snapshot)
        for delta in buffered_deltas[first_index:]:
            apply_orderbook_delta(orderbook, delta)

        flow = trade_flow.get_flow()
        engine.set_feed_status(symbol, "connected")
        while (
            engine.mode not in {EngineMode.STOPPING, EngineMode.STOPPED}
            and engine.should_run_market_feed(symbol)
        ):
            payload = json.loads(await websocket.recv())
            _check_subscription(payload)
            topic = payload.get("topic")

            if topic == f"publicTrade.{symbol}":
                last_price = _update_flow(trade_flow, payload) or last_price
                flow = trade_flow.get_flow()
                if last_price is not None:
                    candidates = None
                    wall_events = ()
                    if not pending_reconciled:
                        pair_settings = engine.pair_settings.get(symbol, pair_settings)
                        nearby = find_nearby_levels(
                            orderbook, last_price, pair_settings.max_wall_distance_bps
                        )
                        candidates = find_walls(
                            nearby,
                            flow,
                            min_wall_ratio=pair_settings.min_wall_ratio,
                            min_wall_notional=pair_settings.min_wall_notional_usdt,
                        )
                        wall_events = wall_tracker.update(candidates, orderbook, flow)
                        stable_walls = wall_tracker.get_nearest_stable_walls(last_price)
                        pending_reconciled = True
                    events = await engine.process_market_update(
                        symbol, last_price, stable_walls, wall_events, candidates
                    )
                    for event in events:
                        print(f"{symbol} {event['event']} {event['trade'].get('signal_id')}")
                continue

            if topic != f"orderbook.full.{symbol}":
                continue

            pair_settings = engine.pair_settings.get(symbol, pair_settings)
            wall_tracker.min_age_seconds = pair_settings.min_wall_age_seconds
            wall_tracker.consumed_ratio = pair_settings.consumed_ratio
            delta = payload["data"]
            try:
                apply_orderbook_delta(orderbook, delta)
            except RuntimeError as error:
                raise RuntimeError(
                    f"Orderbook sequence issue for {symbol}; resynchronizing"
                ) from error
            # The REST ticker is best-effort during initial synchronization. Until
            # the trade stream provides a price, keep applying book deltas but do
            # not try to calculate price-relative wall distance with None.
            if last_price is None:
                continue
            nearby = find_nearby_levels(
                orderbook, last_price, pair_settings.max_wall_distance_bps
            )
            walls = find_walls(
                nearby,
                flow,
                min_wall_ratio=pair_settings.min_wall_ratio,
                min_wall_notional=pair_settings.min_wall_notional_usdt,
            )
            wall_events = wall_tracker.update(walls, orderbook, flow)
            stable_walls = wall_tracker.get_nearest_stable_walls(last_price)
            if last_price is not None:
                pending_reconciled = True
                events = await engine.process_market_update(
                    symbol, last_price, stable_walls, wall_events, walls
                )
                for event in events:
                    print(f"{symbol} {event['event']} {event['trade'].get('signal_id')}")


async def market_data_worker(
    symbol: str,
    engine: TradingEngine,
    pair_settings: PairSettings,
) -> None:
    delay = 1.0
    last_log_at = 0.0
    while (
        engine.mode not in {EngineMode.STOPPING, EngineMode.STOPPED}
        and engine.should_run_market_feed(symbol)
    ):
        engine.set_feed_status(symbol, "connecting")
        try:
            await _run_connection(symbol, engine, pair_settings)
            delay = 1.0
            if engine.mode in {EngineMode.STOPPING, EngineMode.STOPPED}:
                engine.set_feed_status(symbol, "stopped")
            elif not engine.pair_settings.get(symbol, pair_settings).enabled:
                engine.set_feed_status(symbol, "stopped")
        except asyncio.CancelledError:
            engine.set_feed_status(symbol, "stopped")
            raise
        except Exception as error:
            engine.set_feed_status(symbol, "reconnecting", error)
            now = monotonic()
            if now - last_log_at >= 30:
                logger.warning(
                    "Market feed %s disconnected; reconnecting in %.1fs",
                    symbol,
                    delay,
                    exc_info=(type(error), error, error.__traceback__),
                )
                last_log_at = now
            if engine.mode in {EngineMode.STOPPING, EngineMode.STOPPED}:
                break
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)
    engine.set_feed_status(symbol, "stopped")


async def main() -> None:
    """Standalone market-data entry point retained for local exploration."""
    from .engine import TradingEngine
    from .settings import TradingSettings

    settings = PairSettings(symbol=DEFAULT_SYMBOL)
    engine = TradingEngine(
        {DEFAULT_SYMBOL: settings},
        TradingSettings(),
    )
    engine.start()
    await market_data_worker(DEFAULT_SYMBOL, engine, settings)


if __name__ == "__main__":
    asyncio.run(main())
