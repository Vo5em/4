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
        tuple(
            (
                wall.get("wall_id"),
                tuple((trace.get("strategy_id"), trace.get("signal_formed"),
                       trace.get("signal_accepted"), trace.get("pending_created"),
                       tuple((item.get("condition"), item.get("passed"))
                             for item in trace.get("conditions", [])),
                       tuple(trace.get("reasons", [])[:1]))
                      for trace in wall.get("strategies", [])),
            )
            for wall in data.get("planner_wall_decisions", [])
        ),
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
    scanner = data.get("scanner_diagnostics") or {}
    filters = data.get("scanner_filters") or {}
    lines.append(
        "  active_filters="
        f"ratio>={filters.get('min_wall_ratio', 'нет данных')}x "
        f"notional>={filters.get('min_wall_notional_usdt', 'нет данных')}USDT "
        f"distance<={filters.get('max_wall_distance_bps', 'нет данных')}bps"
    )
    medians = scanner.get("side_medians") or {}
    lines.append(
        "  side_median_notional="
        f"BID:{medians.get('buy') if medians.get('buy') is not None else 'нет'} "
        f"ASK:{medians.get('sell') if medians.get('sell') is not None else 'нет'} USDT"
    )
    sweep = scanner.get("ratio_sweep") or {}
    thresholds = (5, 10, 15, 20, 30, 50, 100)
    lines.append("  ratio_threshold_sweep candidates BID/ASK=" + " ".join(
        f"{threshold}:{sweep.get('buy', {}).get(threshold, 0)}/"
        f"{sweep.get('sell', {}).get(threshold, 0)}" for threshold in thresholds
    ))
    lines.append("  largest_nearby_levels: side price notional ratio distance rejection")
    for level in (scanner.get("largest_levels") or [])[:8]:
        side = "BID" if level.get("side") == "buy" else "ASK"
        ratio = level.get("wall_ratio")
        lines.append(
            f"    {side} {level['price']:g} {level['notional']:.4g}USDT "
            f"ratio={ratio:.2f}x distance={level['distance_bps']:.2f}bps "
            f"reject={level['rejection'] or 'нет'}" if ratio is not None else
            f"    {side} {level['price']:g} {level['notional']:.4g}USDT ratio=нет "
            f"distance={level['distance_bps']:.2f}bps reject={level['rejection'] or 'нет'}"
        )
    lines.append(f"  trade_planner={data.get('planner_decision', 'нет данных')}")
    planner_walls = data.get("planner_wall_decisions") or []
    strategies = data.get("planner_strategies") or []
    lines.append(f"  planner_strategies={','.join(strategies) if strategies else 'нет'}; оценено стен={len(planner_walls)}")
    for wall in planner_walls:
        side = "BID" if wall.get("side") == "buy" else "ASK"
        lines.append(
            f"  planner {side} {wall.get('price', 0):g} "
            f"{'CONSUMED ' if wall.get('event') == 'CONSUMED' else ''}"
            f"{wall.get('notional', 0):.2f}USDT age={wall.get('age_seconds', 0):.1f}s"
        )
        for trace in wall.get("strategies", []):
            strategy = trace.get("strategy_id", "?")
            if not trace.get("evaluated"):
                reason = (trace.get("reasons") or ["не оценивалась"])[0]
                lines.append(f"    {strategy}: не оценивалась — {reason}")
                continue
            failed = [item for item in trace.get("conditions", []) if not item.get("passed")]
            facts = []
            for item in failed[:2]:
                name = item.get("condition", "условие")
                if "actual_bps" in item:
                    value = f"{item['actual_bps']:.2f}/{item.get('threshold_bps', '—')}bps"
                elif "actual_seconds" in item:
                    value = f"{item['actual_seconds']:.1f}/{item.get('threshold_seconds', '—')}s"
                elif "elapsed_seconds" in item:
                    value = f"{item['elapsed_seconds']:.1f}/{item.get('confirmation_seconds', '—')}s"
                else:
                    value = str(item.get("actual", "не выполнено"))
                facts.append(f"{name}={value}")
            state = "signal" if trace.get("signal_formed") else "no-signal"
            if trace.get("pending_created"):
                state += "/PENDING"
            elif trace.get("trade_planner_reason"):
                state += "/rejected-by-TradeManager"
            reason = (trace.get("reasons") or [])[:1]
            lines.append(
                f"    {strategy}: {state}; "
                f"{'; '.join(facts) if facts else 'условия пройдены'}"
                f"{'; ' + reason[0] if reason else ''}"
            )
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
