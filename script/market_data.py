import asyncio
import json
import itertools
import logging
from datetime import datetime, timezone
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
# `orderbook.full` requires the matching full-depth REST stream; the regular
# `/orderbook` endpoint's `u` belongs to the 1000-level orderbook feed.
BYBIT_REST_URL = "https://api.bybit.com/v5/market/full_orderbook"
DEFAULT_SYMBOL = "BTCUSDT"
CATEGORY = "linear"
logger = logging.getLogger(__name__)
_last_snapshot_log_at: dict[str, float] = {}
_connection_generations = itertools.count(1)


async def get_orderbook_snapshot(symbol: str) -> dict:
    params = {"category": CATEGORY, "symbol": symbol}
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
    """The REST snapshot and buffered full-depth updates do not yet align."""


class OrderbookDeltaBuffer:
    """One WebSocket connection's delta generation and continuity cursor."""

    def __init__(self, generation: int) -> None:
        self.generation = generation
        self.updates: list[dict] = []
        self.last_u: int | None = None
        self.last_seq: int | None = None

    def __bool__(self) -> bool:
        return bool(self.updates)

    def __len__(self) -> int:
        return len(self.updates)

    def __getitem__(self, index):
        return self.updates[index]

    def __iter__(self):
        return iter(self.updates)

    def discard_included_by_snapshot(self, snapshot_seq: int) -> int:
        """Discard deltas already covered by REST, retaining the u cursor."""
        count = 0
        while self.updates and self.updates[0]["seq"] < snapshot_seq:
            self.updates.pop(0)
            count += 1
        return count


def find_first_delta_after_snapshot(snapshot: dict, buffer: list[dict]) -> int | None:
    """Find the matching Bybit update; seq is ordered but not consecutive."""
    updates = buffer.updates if isinstance(buffer, OrderbookDeltaBuffer) else buffer
    if updates and snapshot["seq"] < updates[0]["seq"]:
        raise SnapshotRetryRequired("REST snapshot is older than the first buffered delta")
    for index, delta in enumerate(updates):
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


def discard_deltas_included_by_snapshot(snapshot: dict, buffer) -> int:
    """Drop old deltas while preserving the connection's continuity cursor."""
    if isinstance(buffer, OrderbookDeltaBuffer):
        return buffer.discard_included_by_snapshot(snapshot["seq"])
    first_not_included = next(
        (index for index, delta in enumerate(buffer) if delta["seq"] >= snapshot["seq"]),
        len(buffer),
    )
    del buffer[:first_not_included]
    return first_not_included


def buffer_orderbook_delta(
    buffer, delta: dict, counters: dict | None = None
) -> str:
    """Keep a contiguous Bybit `u` segment, dropping stale `seq` updates.

    Returns ``appended``, ``ignored`` or ``reset``. Bybit's `seq` is a
    cross-sequence and may skip values; only `u` must be consecutive.
    """
    updates = buffer.updates if isinstance(buffer, OrderbookDeltaBuffer) else buffer
    previous_seq = (
        buffer.last_seq if isinstance(buffer, OrderbookDeltaBuffer)
        else updates[-1]["seq"] if updates else None
    )
    previous_u = (
        buffer.last_u if isinstance(buffer, OrderbookDeltaBuffer)
        else updates[-1]["u"] if updates else None
    )
    if previous_seq is not None and delta["seq"] < previous_seq:
        if counters is not None:
            counters["discarded"] += 1
        return "ignored"
    if delta["u"] == 1:
        if counters is not None:
            counters["discarded"] += len(updates)
            counters["resets"] += 1
            counters["accepted"] += 1
        updates.clear()
        updates.append(delta)
        if isinstance(buffer, OrderbookDeltaBuffer):
            buffer.last_u, buffer.last_seq = delta["u"], delta["seq"]
        return "reset"
    if previous_u is not None:
        if delta["u"] <= previous_u:
            if counters is not None:
                counters["discarded"] += 1
            return "ignored"
        if delta["u"] != previous_u + 1:
            if counters is not None:
                counters["discarded"] += len(updates)
                counters["resets"] += 1
                counters["accepted"] += 1
            updates.clear()
            updates.append(delta)
            if isinstance(buffer, OrderbookDeltaBuffer):
                buffer.last_u, buffer.last_seq = delta["u"], delta["seq"]
            return "reset"
    updates.append(delta)
    if isinstance(buffer, OrderbookDeltaBuffer):
        buffer.last_u, buffer.last_seq = delta["u"], delta["seq"]
    if counters is not None:
        counters["accepted"] += 1
    return "appended"


def apply_orderbook_delta(orderbook: LocalOrderBook, delta: dict) -> None:
    """Apply the next full-depth update, ignoring stale data and rejecting gaps."""
    if delta["u"] == 1:
        orderbook.invalidate()
        raise RuntimeError("Orderbook update ID reset; resynchronization required")
    if delta["seq"] < orderbook.sequence or delta["u"] < orderbook.update_id:
        return
    if delta["u"] == orderbook.update_id:
        return
    if delta["u"] != orderbook.update_id + 1:
        orderbook.invalidate()
        raise RuntimeError("Orderbook update ID gap; resynchronization required")
    orderbook.apply_delta(delta)


def build_synchronized_orderbook(snapshot: dict, buffered_deltas: list[dict]):
    """Build a local book only when REST and buffered WS updates align."""
    discard_deltas_included_by_snapshot(snapshot, buffered_deltas)
    first_index = find_first_delta_after_snapshot(snapshot, buffered_deltas)
    if first_index is None:
        raise SnapshotRetryRequired("WebSocket stream has not reached the REST snapshot")
    orderbook = LocalOrderBook()
    orderbook.load_snapshot(snapshot)
    for delta in buffered_deltas[first_index:]:
        apply_orderbook_delta(orderbook, delta)
    if not orderbook.synchronized:
        raise RuntimeError("Orderbook lost synchronization while applying buffered deltas")
    return orderbook, first_index


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


def analyze_synced_orderbook(
    orderbook: LocalOrderBook,
    last_price: float,
    flow: dict,
    pair_settings: PairSettings,
    wall_tracker: WallTracker,
) -> dict:
    """Run wall scanning only against a successfully synchronized local book."""
    if not orderbook.synchronized:
        raise RuntimeError("Cannot analyze an unsynchronized orderbook")
    nearby = find_nearby_levels(
        orderbook, last_price, pair_settings.max_wall_distance_bps
    )
    filter_counts = {}
    candidates = find_walls(
        nearby,
        flow,
        min_wall_ratio=pair_settings.min_wall_ratio,
        min_wall_notional=pair_settings.min_wall_notional_usdt,
        diagnostics=filter_counts,
    )
    wall_events = wall_tracker.update(candidates, orderbook, flow)
    stable_walls = wall_tracker.get_nearest_stable_walls(last_price)
    return {
        "nearby_levels": nearby,
        "candidates": candidates,
        "filter_counts": filter_counts,
        "wall_events": wall_events,
        "stable_walls": stable_walls,
    }


def _log_analysis_if_due(
    symbol: str,
    orderbook: LocalOrderBook,
    last_price: float,
    result: dict,
    last_log_at: float,
) -> float:
    now = monotonic()
    if now - last_log_at >= 30:
        rejected = {
            key: value for key, value in result["filter_counts"].items()
            if key not in {"input_levels", "candidates"} and value
        }
        if not result["nearby_levels"]:
            rejected["no_levels_within_search_distance"] = 1
        logger.info(
            "%s wall analysis complete at %s: price=%s depth(bid=%d,ask=%d) "
            "nearby=%d candidates=%d stable=%d rejected=%s",
            symbol,
            datetime.now(timezone.utc).isoformat(timespec="seconds"),
            last_price,
            len(orderbook.bids),
            len(orderbook.asks),
            len(result["nearby_levels"]),
            len(result["candidates"]),
            len(result["stable_walls"]),
            rejected,
        )
        return now
    return last_log_at


def _log_snapshot_if_due(symbol: str, snapshot: dict, buffered_count: int) -> None:
    now = monotonic()
    last = _last_snapshot_log_at.get(symbol, 0.0)
    if now - last < 30:
        return
    logger.info(
        "%s REST orderbook snapshot received at %s: seq=%s u=%s bids=%d asks=%d "
        "buffered=%d",
        symbol,
        datetime.now(timezone.utc).isoformat(timespec="seconds"),
        snapshot.get("seq"),
        snapshot.get("u"),
        len(snapshot.get("b", [])),
        len(snapshot.get("a", [])),
        buffered_count,
    )
    _last_snapshot_log_at[symbol] = now


async def _fetch_snapshot_while_buffering(
    websocket,
    symbol: str,
    trade_flow: TradeFlow,
    buffered_deltas: list[dict],
    sync_counters: dict | None = None,
) -> tuple[dict, float | None]:
    last_price = None
    last_wait_log_at = 0.0
    # Bybit full-depth has no initial WS snapshot. Start the REST request only
    # after the first delta is buffered, so a fast REST response cannot race
    # ahead of the stream and make the first buffered update appear newer.
    while not buffered_deltas:
        try:
            message = await asyncio.wait_for(websocket.recv(), timeout=0.5)
        except asyncio.TimeoutError:
            now = monotonic()
            if now - last_wait_log_at >= 30:
                logger.warning(
                    "%s waiting for the first full-depth WebSocket delta; "
                    "REST snapshot has not been requested",
                    symbol,
                )
                last_wait_log_at = now
            continue
        payload = json.loads(message)
        _check_subscription(payload)
        topic = payload.get("topic")
        if topic == f"orderbook.full.{symbol}":
            buffer_orderbook_delta(buffered_deltas, payload["data"], sync_counters)
        elif topic == f"publicTrade.{symbol}":
            last_price = _update_flow(trade_flow, payload) or last_price

    snapshot_task = asyncio.create_task(get_orderbook_snapshot(symbol))
    logger.debug(
        "%s generation=%s REST snapshot requested after WS delta",
        symbol,
        getattr(buffered_deltas, "generation", "unknown"),
    )
    last_rest_wait_log_at = 0.0
    try:
        while not snapshot_task.done():
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=0.5)
            except asyncio.TimeoutError:
                now = monotonic()
                if now - last_rest_wait_log_at >= 30:
                    first = buffered_deltas[0] if buffered_deltas else {}
                    logger.warning(
                        "%s waiting for REST snapshot; first buffered update seq=%s u=%s",
                        symbol,
                        first.get("seq"),
                        first.get("u"),
                    )
                    last_rest_wait_log_at = now
                continue
            payload = json.loads(message)
            _check_subscription(payload)
            topic = payload.get("topic")
            if topic == f"orderbook.full.{symbol}":
                buffer_orderbook_delta(
                    buffered_deltas, payload["data"], sync_counters
                )
                if len(buffered_deltas) > 10000:
                    raise RuntimeError("Too many buffered deltas during snapshot sync")
            elif topic == f"publicTrade.{symbol}":
                last_price = _update_flow(trade_flow, payload) or last_price
        snapshot = await snapshot_task
        _log_snapshot_if_due(symbol, snapshot, len(buffered_deltas))
        return snapshot, last_price
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
    sync_counters = {"accepted": 0, "discarded": 0, "resets": 0}
    last_analysis_log_at = 0.0
    last_alignment_warning_at = 0.0
    last_alignment_wait_log_at = 0.0

    async with websockets.connect(
        BYBIT_WS_URL,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=5,
    ) as websocket:
        logger.info("%s WebSocket connected", symbol)
        await websocket.send(json.dumps({"op": "subscribe", "args": topics}))
        logger.info("%s WebSocket subscription requested: %s", symbol, ",".join(topics))
        buffered_deltas = OrderbookDeltaBuffer(next(_connection_generations))
        logger.info("%s orderbook buffer generation=%d started", symbol, buffered_deltas.generation)
        snapshot, snapshot_last_price = await _fetch_snapshot_while_buffering(
            websocket, symbol, trade_flow, buffered_deltas, sync_counters
        )
        last_price = snapshot_last_price or last_price
        while True:
            dropped = discard_deltas_included_by_snapshot(snapshot, buffered_deltas)
            sync_counters["discarded"] += dropped
            try:
                first_index = find_first_delta_after_snapshot(snapshot, buffered_deltas)
            except SnapshotRetryRequired as error:
                # The REST response can lag the earliest buffered update. Keep
                # buffering and request a newer snapshot as prescribed by V5.
                now = monotonic()
                if now - last_alignment_warning_at >= 30:
                    first = buffered_deltas[0] if buffered_deltas else {}
                    logger.warning(
                        "%s generation=%d orderbook alignment retry: %s; snapshot(seq=%s,u=%s), "
                        "first_buffered(seq=%s,u=%s), buffered=%d",
                        symbol,
                        buffered_deltas.generation,
                        error,
                        snapshot.get("seq"),
                        snapshot.get("u"),
                        first.get("seq"),
                        first.get("u"),
                        len(buffered_deltas),
                    )
                    last_alignment_warning_at = now
                snapshot, snapshot_last_price = await _fetch_snapshot_while_buffering(
                    websocket, symbol, trade_flow, buffered_deltas, sync_counters
                )
                last_price = snapshot_last_price or last_price
                continue
            if first_index is not None:
                break
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=0.5)
            except asyncio.TimeoutError:
                now = monotonic()
                if now - last_alignment_wait_log_at >= 30:
                    latest = buffered_deltas[-1] if buffered_deltas else {}
                    logger.warning(
                        "%s waiting for a WS update aligned with REST snapshot "
                        "seq=%s u=%s; latest buffered seq=%s u=%s",
                        symbol,
                        snapshot.get("seq"),
                        snapshot.get("u"),
                        latest.get("seq"),
                        latest.get("u"),
                    )
                    last_alignment_wait_log_at = now
                continue
            payload = json.loads(message)
            _check_subscription(payload)
            topic = payload.get("topic")
            if topic == f"publicTrade.{symbol}":
                last_price = _update_flow(trade_flow, payload) or last_price
            elif topic == f"orderbook.full.{symbol}":
                buffer_result = buffer_orderbook_delta(
                    buffered_deltas, payload["data"], sync_counters
                )
                if len(buffered_deltas) > 10000:
                    raise RuntimeError("Too many buffered deltas during snapshot sync")
                if buffer_result == "reset":
                    snapshot, snapshot_last_price = await _fetch_snapshot_while_buffering(
                        websocket, symbol, trade_flow, buffered_deltas, sync_counters
                    )
                    last_price = snapshot_last_price or last_price

        orderbook, _ = build_synchronized_orderbook(
            snapshot, buffered_deltas
        )

        logger.info(
            "%s generation=%d orderbook synchronized at %s: snapshot(seq=%s,u=%s), current(seq=%s,u=%s), "
            "buffer accepted=%d discarded=%d resets=%d, depth bids=%d asks=%d",
            symbol,
            buffered_deltas.generation,
            datetime.now(timezone.utc).isoformat(timespec="seconds"),
            snapshot["seq"], snapshot["u"], orderbook.sequence, orderbook.update_id,
            sync_counters["accepted"], sync_counters["discarded"],
            sync_counters["resets"], len(orderbook.bids), len(orderbook.asks),
        )

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
                        result = analyze_synced_orderbook(
                            orderbook, last_price, flow, pair_settings, wall_tracker
                        )
                        candidates = result["candidates"]
                        wall_events = result["wall_events"]
                        stable_walls = result["stable_walls"]
                        last_analysis_log_at = _log_analysis_if_due(
                            symbol, orderbook, last_price, result, last_analysis_log_at
                        )
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
            result = analyze_synced_orderbook(
                orderbook, last_price, flow, pair_settings, wall_tracker
            )
            walls = result["candidates"]
            wall_events = result["wall_events"]
            stable_walls = result["stable_walls"]
            last_analysis_log_at = _log_analysis_if_due(
                symbol, orderbook, last_price, result, last_analysis_log_at
            )
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
