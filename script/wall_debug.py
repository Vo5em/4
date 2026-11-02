"""Live diagnostics for the production wall scanner pipeline.

Run with ``python -m script.wall_debug``. The command consumes the normal
Bybit public feed and the same ``market_data_worker`` and analyzer as the bot.
It runs normal runtime initialization to load configured pairs; while sampling,
virtual fills and market-state repository writes are disabled.
"""

import argparse
import asyncio
import logging
from datetime import datetime

from script.engine import EngineMode


def _signature(data: dict) -> tuple:
    walls = []
    for side in ("bid", "ask"):
        walls.extend(
            (side, item.get("price"), item.get("status"))
            for item in data.get(side, [])
        )
    return (
        data.get("feed_state"), data.get("book_synchronized"),
        data.get("stable_count"), tuple(walls), data.get("reason"),
        data.get("planner_decision"),
    )


def _render(symbol: str, data: dict, health: dict) -> str:
    update_at = data.get("last_update_at")
    book_at = health.get("book_received_at")
    price = data.get("price")
    lines = [
        f"{symbol}: websocket={data.get('feed_state', 'нет данных')} "
        f"book_sync={'yes' if data.get('book_synchronized') else 'no'} "
        f"generation={health.get('active_connection_generation') or health.get('connection_generation') or 'нет данных'}",
        f"  last_valid_book_update={book_at.isoformat(timespec='seconds') if isinstance(book_at, datetime) else 'нет данных'} "
        f"book_age={health.get('book_age_seconds'):.2f}s" if health.get("book_age_seconds") is not None
        else "  last_valid_book_update=нет данных",
        f"  scan_at={update_at.isoformat(timespec='seconds') if isinstance(update_at, datetime) else 'нет данных'} "
        f"price={price if price is not None else 'нет данных'}",
    ]
    for side, label in (("bid", "BID"), ("ask", "ASK")):
        items = data.get(side, [])
        if not items:
            lines.append(f"  {label}: кандидатов нет")
            continue
        for wall in items:
            lines.append(
                f"  {label} {wall['status']} price={wall['price']:g} "
                f"notional={wall['notional']:.2f}USDT distance={wall['distance_bps']:.2f}bps "
                f"age={wall['age_seconds']:.1f}/{wall['stability_seconds']:.0f}s "
                f"progress={wall['progress_percent']:.0f}% "
                f"volume_change={wall['change_notional']:+.2f}USDT"
            )
    lines.append(f"  stable_walls={data.get('stable_count', 0)} · reason={data.get('reason', 'нет данных')}")
    rejected = data.get("rejected") or {}
    lines.append(f"  rejected={rejected or 'нет'}")
    lines.append(f"  trade_planner={data.get('planner_decision', 'нет данных')}")
    return "\n".join(lines)


async def _run(symbols: list[str], interval: float) -> None:
    from run import build_runtime
    from database.models import async_close

    runtime, _initialized = await build_runtime()
    try:
        configured = set(runtime.engine.pair_settings)
        missing = sorted(set(symbols).difference(configured))
        if missing:
            raise RuntimeError(
                "Пары не настроены в текущем хранилище: " + ", ".join(missing)
            )

        engine = runtime.engine
        # The diagnostic uses the exact production market workers, scanner,
        # tracker, and strategy evaluators, while avoiding market-state writes
        # and virtual fills once the persisted settings have been loaded.
        engine.diagnostic_only = True
        engine.diagnostic_symbols = set(symbols)
        engine.repository = None
        engine.mode = EngineMode.RUNNING
        await runtime._sync_workers()
        last_signatures = {}
        last_periodic = 0.0
        loop = asyncio.get_running_loop()
        while True:
            now = loop.time()
            for symbol in symbols:
                data = engine.get_wall_diagnostics(symbol).get(symbol, {})
                signature = _signature(data)
                if last_signatures.get(symbol) != signature:
                    logging.info("STATE CHANGE\n%s", _render(
                        symbol, data, engine.get_market_data_health(symbol)
                    ))
                    last_signatures[symbol] = signature
            if now - last_periodic >= interval:
                for symbol in symbols:
                    data = engine.get_wall_diagnostics(symbol).get(symbol, {})
                    logging.info("WALL SCAN REPORT\n%s", _render(
                        symbol, data, engine.get_market_data_health(symbol)
                    ))
                last_periodic = now
            await asyncio.sleep(min(0.5, interval))
    finally:
        try:
            await runtime.shutdown()
        finally:
            await async_close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Live read-only diagnostics for Bybit wall scanning")
    parser.add_argument(
        "--symbols", nargs="+", default=["BTCUSDT", "ETHUSDT"],
        help="Configured symbols to inspect (default: BTCUSDT ETHUSDT)",
    )
    parser.add_argument("--interval", type=float, default=5.0, help="Periodic report interval in seconds")
    args = parser.parse_args()
    if args.interval < 1:
        parser.error("--interval must be at least 1 second")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    symbols = list(dict.fromkeys(item.strip().upper() for item in args.symbols))
    try:
        asyncio.run(_run(symbols, args.interval))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
