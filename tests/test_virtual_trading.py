import os
import json
import unittest
from types import SimpleNamespace
from dataclasses import replace
from datetime import datetime, timezone
from datetime import timedelta
from math import nan
from concurrent.futures import ThreadPoolExecutor
from time import monotonic, time
from unittest.mock import AsyncMock, patch
from aiogram.types import Chat, Message, User

os.environ.setdefault("ADMIN_USER_ID", "4242")

from script.engine import EngineMode, TradingEngine
import script.engine as engine_module
from script.formulas import (
    calculate_entry_price,
    calculate_stop_loss_price,
    calculate_x2_price,
)
from script.market_data import (
    BYBIT_REST_URL,
    OrderbookDeltaBuffer,
    SnapshotRetryRequired,
    PriceTick,
    TradeEventCursor,
    TickerPriceCursor,
    _update_flow,
    _update_ticker_price,
    _delivery_lag_ms,
    _fetch_snapshot_while_buffering,
    analyze_synced_orderbook,
    apply_orderbook_delta,
    buffer_orderbook_delta,
    build_synchronized_orderbook,
    discard_deltas_included_by_snapshot,
    find_first_delta_after_snapshot,
    get_orderbook_snapshot,
    market_data_worker,
)
from script.orderbook import LocalOrderBook
from script.scanner import find_nearby_levels, find_walls
from script.settings import PairSettings, TradingSettings, normalize_pair_symbol
from script.strategies import (
    BOUNCE_STRATEGY_ID,
    BREAKOUT_STRATEGY_ID,
    BounceStrategy,
    BreakoutStrategy,
    StrategyContext,
    StrategyRegistry,
    TradeSignal,
)
from script.trade_manager import TradeManager as CoreTradeManager
from script.trade_levels import calculate_trade_levels
from script.trade_flow import TradeFlow
from script.wall_tracker import WallTracker
from app.runtime import RuntimeController
from app.admin import (
    Admin,
    _load_admin_user_ids,
    cancel_input,
    dashboard_text,
    dayid,
    pair_settings_text,
    pair_statistics_text,
    trade_level_pair_text,
    trade_level_pair_keyboard,
    trade_levels_text,
    trade_levels_keyboard,
    stable_walls_text,
    stable_walls_keyboard,
    navigate_stable_walls,
    WALLS_PAGE_SIZE,
    TELEGRAM_TEXT_LIMIT,
    _deal_card,
    _deal_list_text,
    _save_pair_leverage,
    close_open_trades as close_open_trades_callback,
    edit_message_if_changed,
    set_pair_strategy,
)
from run import _symbols_from_environment, build_runtime


class TradeManager(CoreTradeManager):
    """Legacy unit-test adapter: models the engine supplying prepared levels."""

    def create_trade(self, signal, pair_settings, *, stake_usdt=None):
        data = signal if isinstance(signal, dict) else dict(signal.__dict__)
        if any(data.get(key) is None for key in ("entry_price", "stop_loss", "take_profit")):
            strategy_id = data.get("strategy_id")
            if strategy_id not in {BOUNCE_STRATEGY_ID, BREAKOUT_STRATEGY_ID}:
                strategy_id = BOUNCE_STRATEGY_ID
            levels = calculate_trade_levels(
                strategy_id=strategy_id,
                wall_side=data.get("wall_side", "buy"),
                wall_price=data["wall_price"],
                tick_size="0.00001",
                take_profit_bps=(
                    pair_settings.take_profit_bps
                    if pair_settings.take_profit_bps is not None
                    else self.trading_settings.default_take_profit_bps
                ),
                stop_loss_bps=(
                    pair_settings.stop_loss_bps
                    if pair_settings.stop_loss_bps is not None
                    else self.trading_settings.default_stop_loss_bps
                ),
            )
            data = {**data, **levels}
            if not data.get("entry_trigger_direction"):
                data["entry_trigger_direction"] = levels["entry_trigger_direction"]
            if data.get("reference_price") is None:
                data["reference_price"] = float(levels["entry_price"]) * (
                    1.001 if data["wall_side"] == "buy" else 0.999
                )
        return super().create_trade(data, pair_settings, stake_usdt=stake_usdt)


engine_module.TradeManager = TradeManager


def run_immediate(coroutine):
    """Drive coroutines whose test doubles complete without suspending."""
    try:
        coroutine.send(None)
    except StopIteration as completed:
        return completed.value
    finally:
        coroutine.close()
    raise AssertionError("Coroutine suspended; it needs a running event loop")


class FakeRepository:
    def __init__(self):
        self.fail_trade_saves = False
        self.fail_pair_saves = False
        self.saved_trade_batches = []
        self.saved_prices = []
        self.saved_pair_settings = []
        self.saved_enabled_strategies = []
        self.saved_admin_user_ids = []
        self.fail_strategy_saves = False
        self.released_manual_blocks = []
        self.fail_manual_block_releases = False

    async def save_trades(self, trades, balance_usdt, engine_mode=None):
        if self.fail_trade_saves:
            raise OSError("temporary database failure")
        self.saved_trade_batches.append((trades, balance_usdt, engine_mode))

    async def save_market_price(self, symbol, price):
        self.saved_prices.append((symbol, price))

    async def release_manual_plan_blocks(self, signal_ids):
        if self.fail_manual_block_releases:
            raise OSError("temporary manual block release failure")
        self.released_manual_blocks.extend(signal_ids)

    async def save_pair_settings(self, settings):
        if self.fail_pair_saves:
            raise OSError("temporary settings failure")
        self.saved_pair_settings.append(settings)

    async def save_enabled_strategies(self, symbol, strategy_ids):
        if self.fail_strategy_saves:
            raise OSError("temporary strategy settings failure")
        self.saved_enabled_strategies.append((symbol, set(strategy_ids)))

    async def save_enabled_strategies_with_trades(
        self, symbol, strategy_ids, trades, balance_usdt, engine_mode,
        admin_user_id=None, expected_version=None,
    ):
        if self.fail_strategy_saves or (trades and self.fail_trade_saves):
            raise OSError("temporary strategy transaction failure")
        self.saved_enabled_strategies.append((symbol, set(strategy_ids)))
        if admin_user_id is not None:
            self.saved_admin_user_ids.append(admin_user_id)
        if trades:
            self.saved_trade_batches.append((trades, balance_usdt, engine_mode))

    async def save_pair_settings_with_trades(
        self, settings, trades, balance_usdt, engine_mode
    ):
        if self.fail_pair_saves or self.fail_trade_saves:
            raise OSError("temporary pair transaction failure")
        self.saved_pair_settings.append(settings)
        if trades:
            self.saved_trade_batches.append((trades, balance_usdt, engine_mode))


class FakeTask:
    def done(self):
        return False

    def add_done_callback(self, callback):
        self.done_callback = callback


class WallTrackerTests(unittest.TestCase):
    def test_wall_qualification_transition_counts_measure_filter_flapping(self):
        book = LocalOrderBook()
        book.bids[100.0] = 100.0
        tracker = WallTracker(min_age_seconds=0)
        wall = {"side": "buy", "price": 100.0, "notional": 10000.0}
        tracker.update([wall], book, {})
        tracker.update([], book, {})
        tracker.update([wall], book, {})
        tracker.update([], book, {})
        self.assertEqual(tracker.qualification_transitions, {"lost": 2, "restored": 1})

    def test_scanner_qualification_loss_resets_the_single_stability_period(self):
        book = LocalOrderBook()
        book.bids[100.0] = 100.0
        tracker = WallTracker(min_age_seconds=40)
        wall = {"side": "buy", "price": 100.0, "notional": 10000.0,
                "wall_ratio": 900.0}

        tracker.update([wall], book, {})
        tracked = tracker.walls[("buy", 100.0)]
        tracked["first_seen"] = monotonic() - 41
        tracker.update([wall], book, {})
        self.assertEqual(tracker.get_stable_walls()[0]["status"], "stable")

        # A complete Scanner result without this level represents a confirmed
        # ratio/notional/distance qualification failure; the level is still in
        # the synchronized book, so this is not a physical-removal event.
        prior_first_seen = tracked["first_seen"]
        tracker.update([], book, {})
        self.assertEqual(tracker.get_stable_walls(), [])
        self.assertEqual(tracked["status"], "candidate")
        self.assertFalse(tracked["qualifies"])
        self.assertGreater(tracked["first_seen"], prior_first_seen)

        tracker.update([wall], book, {})
        self.assertEqual(tracked["status"], "candidate")
        self.assertTrue(tracked["qualifies"])
        self.assertLess(monotonic() - tracked["first_seen"], 40)
        self.assertEqual(tracker.get_stable_walls(), [])

        tracked["first_seen"] = monotonic() - 41
        tracker.update([wall], book, {})
        self.assertEqual(len(tracker.get_stable_walls()), 1)

    def test_continuous_scanner_qualification_requires_40_seconds(self):
        book = LocalOrderBook()
        book.bids[100.0] = 100.0
        tracker = WallTracker(min_age_seconds=40)
        wall = {"side": "buy", "price": 100.0, "notional": 10000.0}
        clock = [100.0]
        with patch("script.wall_tracker.time.monotonic", side_effect=lambda: clock[0]):
            tracker.update([wall], book, {})
            clock[0] = 139.0
            tracker.update([wall], book, {})
            self.assertEqual(tracker.get_stable_walls(), [])
            clock[0] = 140.0
            tracker.update([wall], book, {})
            self.assertEqual(len(tracker.get_stable_walls()), 1)

    def test_short_lived_candidate_disappearance_is_reported_as_removed(self):
        book = LocalOrderBook()
        book.bids[100.0] = 100.0
        tracker = WallTracker(min_age_seconds=60, consumed_ratio=0.9)
        wall = {"side": "buy", "price": 100.0, "notional": 10000.0}

        tracker.update([wall], book, {})
        book.bids.pop(100.0)
        events = tracker.update([], book, {})

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event"], "REMOVED")
        self.assertLess(events[0]["age"], 60)
        self.assertEqual(tracker.get_stable_walls(), [])

    def test_candidate_moves_to_stable_after_required_age(self):
        book = LocalOrderBook()
        book.bids[100.0] = 100.0
        tracker = WallTracker(min_age_seconds=40)
        tracked = {"side": "buy", "price": 100.0, "notional": 10000.0}
        tracker.update([tracked], book, {})
        tracker.walls[("buy", 100.0)]["first_seen"] = monotonic() - 41
        tracker.update([tracked], book, {})
        self.assertEqual(tracker.get_stable_walls()[0]["status"], "stable")

    def test_candidate_disappearing_before_forty_seconds_never_becomes_stable(self):
        book = LocalOrderBook()
        book.bids[100.0] = 100.0
        tracker = WallTracker(min_age_seconds=40)
        tracked = {"side": "buy", "price": 100.0, "notional": 10000.0}
        tracker.update([tracked], book, {})
        tracker.walls[("buy", 100.0)]["first_seen"] = monotonic() - 39
        book.bids.clear()
        events = tracker.update([], book, {})
        self.assertEqual(events[0]["event"], "REMOVED")
        self.assertEqual(tracker.get_stable_walls(), [])

    def test_removed_and_consumed_events(self):
        book = LocalOrderBook()
        book.bids[100.0] = 100.0
        tracker = WallTracker(min_age_seconds=0, consumed_ratio=0.9)
        wall = {"side": "buy", "price": 100.0, "notional": 10000.0}

        tracker.update([wall], book, {})
        book.bids.pop(100.0)
        self.assertEqual(tracker.update([], book, {})[0]["event"], "REMOVED")

        book.bids[100.0] = 100.0
        tracker.update([wall], book, {})
        book.bids[100.0] = 10.0
        flow = {"sell_volume_by_price_total": {100.0: 9000}}
        tracker.update([], book, flow)
        book.bids.pop(100.0)
        event = tracker.update([], book, flow)[0]
        self.assertEqual(event["event"], "CONSUMED")

    def test_delayed_trade_message_is_counted_after_book_size_change(self):
        book = LocalOrderBook()
        book.bids[100.0] = 100.0
        tracker = WallTracker(min_age_seconds=0, consumed_ratio=0.9)
        wall = {"side": "buy", "price": 100.0, "notional": 10000.0}
        tracker.update([wall], book, {})

        book.bids[100.0] = 10.0
        tracker.update([], book, {})
        flow = {"sell_volume_by_price_total": {100.0: 9000}}
        tracker.update([], book, flow)
        book.bids.pop(100.0)
        event = tracker.update([], book, flow)[0]
        self.assertEqual(event["event"], "CONSUMED")

    def test_recreated_price_volume_counter_is_counted_after_cache_eviction(self):
        book = LocalOrderBook()
        book.bids[100.0] = 100.0
        tracker = WallTracker(min_age_seconds=0, consumed_ratio=0.9)
        wall = {"side": "buy", "price": 100.0, "notional": 10000.0}
        flow = {"sell_volume_by_price_total": {100.0: 1000.0}}
        tracker.update([wall], book, flow)

        # The bounded per-price cache evicted this key; new trades start it from zero.
        flow = {"sell_volume_by_price_total": {100.0: 25.0}}
        tracker.update([], book, flow)

        tracked = tracker.walls[("buy", 100.0)]
        self.assertEqual(tracked["aggressive_volume"], 25.0)


class BybitOrderbookSyncTests(unittest.TestCase):
    def test_snapshot_alignment_discards_older_and_matching_delta(self):
        snapshot = {"seq": 20, "u": 50}
        buffered = [
            {"seq": 18, "u": 48},
            {"seq": 19, "u": 49},
            {"seq": 20, "u": 50},
            {"seq": 27, "u": 51},
        ]
        self.assertEqual(find_first_delta_after_snapshot(snapshot, buffered), 3)

    def test_snapshot_caught_between_buffered_seq_values_prunes_included_updates(self):
        buffer = OrderbookDeltaBuffer(generation=1)
        for delta in (
            {"seq": 100, "u": 10},
            {"seq": 200, "u": 11},
            {"seq": 300, "u": 12},
        ):
            buffer_orderbook_delta(buffer, delta)

        dropped = discard_deltas_included_by_snapshot({"seq": 250}, buffer)

        self.assertEqual(dropped, 2)
        self.assertEqual(buffer.updates, [{"seq": 300, "u": 12}])
        with self.assertRaisesRegex(SnapshotRetryRequired, "older than the first"):
            find_first_delta_after_snapshot({"seq": 250}, buffer)

    def test_repeated_alignment_retries_do_not_accumulate_old_deltas(self):
        buffer = OrderbookDeltaBuffer(generation=7)
        for index in range(760):
            delta = {"seq": (index + 1) * 100, "u": index + 1}
            buffer_orderbook_delta(buffer, delta)
            snapshot = {"seq": delta["seq"] - 1, "u": delta["u"] - 1}
            discard_deltas_included_by_snapshot(snapshot, buffer)
            with self.assertRaises(SnapshotRetryRequired):
                find_first_delta_after_snapshot(snapshot, buffer)
            self.assertLessEqual(len(buffer), 1)

    def test_pruning_snapshot_covered_deltas_keeps_update_id_gap_detection(self):
        buffer = OrderbookDeltaBuffer(generation=5)
        buffer_orderbook_delta(buffer, {"seq": 100, "u": 40})
        buffer_orderbook_delta(buffer, {"seq": 200, "u": 41})
        discard_deltas_included_by_snapshot({"seq": 150}, buffer)

        self.assertEqual(buffer.updates, [{"seq": 200, "u": 41}])
        self.assertEqual(buffer.last_u, 41)
        self.assertEqual(
            buffer_orderbook_delta(buffer, {"seq": 300, "u": 43}),
            "reset",
        )
        self.assertEqual(buffer.updates, [{"seq": 300, "u": 43}])

    def test_new_websocket_generation_has_no_previous_connection_deltas(self):
        first = OrderbookDeltaBuffer(generation=1)
        second = OrderbookDeltaBuffer(generation=2)
        buffer_orderbook_delta(first, {"seq": 100, "u": 500})
        buffer_orderbook_delta(second, {"seq": 10, "u": 1})

        self.assertEqual(first.updates, [{"seq": 100, "u": 500}])
        self.assertEqual(second.updates, [{"seq": 10, "u": 1}])
        self.assertEqual(second.last_u, 1)

    def test_rest_snapshot_uses_full_depth_endpoint_and_matching_params(self):
        payload = {"retCode": 0, "result": {"seq": 123, "u": 45, "b": [], "a": []}}
        calls = []

        class FakeResponse:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

            def raise_for_status(self):
                return None

            async def json(self):
                return payload

        class FakeSession:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

            def get(self, url, params):
                calls.append((url, params))
                return FakeResponse()

        with patch("script.market_data.aiohttp.ClientSession", return_value=FakeSession()):
            snapshot = run_immediate(get_orderbook_snapshot("BTCUSDT"))

        self.assertEqual(BYBIT_REST_URL, "https://api.bybit.com/v5/market/full_orderbook")
        self.assertEqual(calls, [(BYBIT_REST_URL, {"category": "linear", "symbol": "BTCUSDT"})])
        self.assertEqual((snapshot["seq"], snapshot["u"]), (123, 45))

    def test_snapshot_and_buffer_build_a_current_local_book(self):
        snapshot = {"seq": 20, "u": 50, "b": [[100, 1]], "a": [[101, 2]]}
        buffered = [
            {"seq": 18, "u": 48, "b": [[99, 1]], "a": []},
            {"seq": 19, "u": 49, "b": [], "a": []},
            {"seq": 20, "u": 50, "b": [], "a": []},
            {"seq": 27, "u": 51, "b": [[100, 3]], "a": [[101, 0]]},
        ]
        book, _ = build_synchronized_orderbook(snapshot, buffered)
        self.assertTrue(book.synchronized)
        self.assertEqual(book.update_id, 51)
        self.assertEqual(book.bids, {100.0: 3.0})
        self.assertEqual(book.asks, {})

    def test_snapshot_waits_until_stream_reaches_snapshot_sequence(self):
        result = find_first_delta_after_snapshot(
            {"seq": 30, "u": 80}, [{"seq": 28, "u": 78}]
        )
        self.assertIsNone(result)

    def test_same_sequence_with_different_update_id_is_rejected(self):
        with self.assertRaises(RuntimeError):
            find_first_delta_after_snapshot(
                {"seq": 30, "u": 80}, [{"seq": 30, "u": 81}]
            )

    def test_snapshot_behind_first_delta_requires_refetch(self):
        with self.assertRaises(SnapshotRetryRequired):
            find_first_delta_after_snapshot(
                {"seq": 20, "u": 50}, [{"seq": 21, "u": 51}]
            )

    def test_reset_inside_initial_delta_buffer_forces_resynchronization(self):
        snapshot = {"seq": 20, "u": 50, "b": [[100, 1]], "a": []}
        buffer = [
            {"seq": 20, "u": 50, "b": [], "a": []},
            {"seq": 21, "u": 1, "b": [], "a": []},
            {"seq": 22, "u": 2, "b": [[100, 2]], "a": []},
        ]
        first_index = find_first_delta_after_snapshot(snapshot, buffer)
        book = LocalOrderBook()
        book.load_snapshot(snapshot)

        with self.assertRaisesRegex(RuntimeError, "reset"):
            apply_orderbook_delta(book, buffer[first_index])

        self.assertEqual(book.bids, {100.0: 1.0})


    def test_stale_orderbook_delta_is_ignored_without_resynchronization(self):
        book = LocalOrderBook()
        book.load_snapshot({"u": 50, "seq": 500, "b": [[100, 1]], "a": []})

        apply_orderbook_delta(
            book,
            {"u": 50, "seq": 500, "b": [[100, 9]], "a": []},
        )

        self.assertEqual(book.update_id, 50)
        self.assertEqual(book.bids, {100.0: 1.0})

    def test_nonconsecutive_cross_sequence_is_valid_when_update_id_is_contiguous(self):
        buffer = [{"seq": 100, "u": 10}]
        result = buffer_orderbook_delta(buffer, {"seq": 145, "u": 11})
        self.assertEqual(result, "appended")
        self.assertEqual([delta["u"] for delta in buffer], [10, 11])

    def test_update_id_gap_discards_buffer_and_requires_fresh_snapshot(self):
        buffer = [{"seq": 100, "u": 10}, {"seq": 105, "u": 11}]
        result = buffer_orderbook_delta(buffer, {"seq": 110, "u": 13})
        self.assertEqual(result, "reset")
        self.assertEqual(buffer, [{"seq": 110, "u": 13}])

    def test_decreased_cross_sequence_delta_is_discarded(self):
        buffer = [{"seq": 110, "u": 10}]
        result = buffer_orderbook_delta(buffer, {"seq": 109, "u": 11})
        self.assertEqual(result, "ignored")
        self.assertEqual(buffer, [{"seq": 110, "u": 10}])

    def test_live_update_id_gap_invalidates_book(self):
        book = LocalOrderBook()
        book.load_snapshot({"u": 50, "seq": 500, "b": [[100, 1]], "a": []})
        with self.assertRaisesRegex(RuntimeError, "gap"):
            apply_orderbook_delta(book, {"u": 52, "seq": 503, "b": [], "a": []})
        self.assertFalse(book.synchronized)
        self.assertEqual(book.bids, {100.0: 1.0})

    def test_stale_snapshot_alignment_does_not_build_book(self):
        with self.assertRaises(SnapshotRetryRequired):
            build_synchronized_orderbook(
                {"seq": 20, "u": 50, "b": [], "a": []},
                [{"seq": 21, "u": 51, "b": [], "a": []}],
            )

    def test_wall_analyzer_rejects_an_unsynchronized_book(self):
        with self.assertRaisesRegex(RuntimeError, "unsynchronized"):
            analyze_synced_orderbook(
                LocalOrderBook(),
                100,
                {},
                PairSettings("BTCUSDT"),
                WallTracker(),
            )

    def test_synchronized_book_is_transferred_to_wall_analyzer(self):
        settings = PairSettings(
            "BTCUSDT", min_wall_ratio=50, max_wall_distance_bps=100,
            min_wall_age_seconds=0,
        )
        snapshot = {
            "seq": 100, "u": 10,
            "b": [[100, 1], [99.9, 1], [99.8, 1], [99.7, 1], [99.6, 100]],
            "a": [],
        }
        book, _ = build_synchronized_orderbook(
            snapshot, [{"seq": 100, "u": 10, "b": [], "a": []}]
        )
        flow = {
            "buy_volume_per_second": 0,
            "sell_volume_per_second": 0,
            "buy_volume_by_price_total": {},
            "sell_volume_by_price_total": {},
        }
        result = analyze_synced_orderbook(
            book, 100, flow, settings, WallTracker(min_age_seconds=0)
        )
        self.assertGreater(len(result["nearby_levels"]), 0)
        self.assertEqual(len(result["candidates"]), 1)
        self.assertEqual(result["candidates"][0]["price"], 99.6)
        engine = TradingEngine({"BTCUSDT": settings})
        engine.start()
        events = engine.on_market_update(
            "BTCUSDT", 100, result["stable_walls"],
            result["wall_events"], result["candidates"],
        )
        # A stable wall is market information only; the legacy strategy must
        # not turn stability alone into a pending entry.
        self.assertNotIn("TRADE_CREATED", [event["event"] for event in events])

    def test_telegram_diagnostics_use_the_live_scanner_result(self):
        settings = PairSettings(
            "BTCUSDT", min_wall_ratio=50, max_wall_distance_bps=100,
            min_wall_age_seconds=0,
        )
        book, _ = build_synchronized_orderbook(
            {"seq": 100, "u": 10, "b": [[100, 1], [99.9, 1], [99.8, 1], [99.7, 1], [99.6, 100]], "a": []},
            [{"seq": 100, "u": 10, "b": [], "a": []}],
        )
        flow = {
            "buy_volume_per_second": 0, "sell_volume_per_second": 0,
            "buy_volume_by_price_total": {}, "sell_volume_by_price_total": {},
        }
        result = analyze_synced_orderbook(book, 100, flow, settings, WallTracker(min_age_seconds=0))
        engine = TradingEngine({"BTCUSDT": settings})
        engine.market_data_health["BTCUSDT"]["book_synchronized"] = True
        engine.record_wall_analysis("BTCUSDT", 100, result)
        runtime = RuntimeController(engine)

        diagnostic = engine.get_wall_diagnostics("BTCUSDT")["BTCUSDT"]
        text = stable_walls_text(runtime)

        self.assertEqual(diagnostic["stable_count"], len(result["stable_walls"]))
        self.assertEqual(diagnostic["bid"][0]["price"], result["candidates"][0]["price"])
        self.assertIn("99.6", text)
        self.assertIn("STABLE", text)

    def test_diagnostic_mode_reports_stable_wall_planner_rejection_without_creating_trade(self):
        engine = TradingEngine(
            {"BTCUSDT": PairSettings("BTCUSDT")},
            enabled_strategies={"BTCUSDT": {BOUNCE_STRATEGY_ID}},
            diagnostic_only=True,
            diagnostic_symbols={"BTCUSDT"},
        )
        engine.start()
        result = engine.on_market_update(
            "BTCUSDT", 100,
            [{"side": "buy", "price": 99.9, "age": 40, "wall_ratio": 900}],
        )
        self.assertEqual(result, [])
        self.assertEqual(engine.trade_manager.trades, [])
        diagnostic = engine.wall_diagnostics["BTCUSDT"]
        self.assertIn("сформировали бы 1 сигнал", diagnostic["planner_decision"])
        bounce_trace = diagnostic["planner_wall_decisions"][0]["strategies"][0]
        entry_distance = next(
            item for item in bounce_trace["conditions"]
            if item["condition"] == "max_wall_distance_bps"
        )
        self.assertTrue(entry_distance["passed"])
        self.assertAlmostEqual(entry_distance["actual_bps"], 10.0)
        self.assertEqual(entry_distance["threshold_bps"], 100.0)

    def test_unsynchronized_status_is_explained_in_wall_panel(self):
        engine = TradingEngine({"BTCUSDT": PairSettings("BTCUSDT")})
        engine.mark_orderbook_unsynchronized("BTCUSDT", generation=4)
        text = stable_walls_text(RuntimeController(engine))
        self.assertIn("стакан не синхронизирован", text)
        self.assertIn("анализ стен заблокирован", text)

    def test_valid_book_with_no_matching_walls_is_successful_analysis(self):
        settings = PairSettings("BTCUSDT", min_wall_ratio=888)
        book, _ = build_synchronized_orderbook(
            {"seq": 100, "u": 10, "b": [[100, 1], [99.9, 1]], "a": []},
            [{"seq": 100, "u": 10, "b": [], "a": []}],
        )
        flow = {
            "buy_volume_per_second": 0,
            "sell_volume_per_second": 0,
            "buy_volume_by_price_total": {},
            "sell_volume_by_price_total": {},
        }
        result = analyze_synced_orderbook(
            book, 100, flow, settings, WallTracker()
        )
        self.assertTrue(book.synchronized)
        self.assertEqual(result["candidates"], [])
        self.assertEqual(result["filter_counts"]["below_min_wall_ratio"], 2)
        self.assertEqual(result["scanner_diagnostics"]["ratio_sweep"]["buy"][5], 0)
        self.assertEqual(result["scanner_diagnostics"]["ratio_sweep"]["buy"][100], 0)
        self.assertTrue(result["scanner_diagnostics"]["largest_levels"])


class MarketDataSnapshotHandshakeTests(unittest.TestCase):
    def test_rest_snapshot_is_requested_after_first_ws_delta_is_buffered(self):
        class FakeWebSocket:
            received = 0

            async def recv(self):
                self.received += 1
                return json.dumps({
                    "topic": "orderbook.full.BTCUSDT",
                    "data": {"seq": 100, "u": 10, "b": [], "a": []},
                })

        websocket = FakeWebSocket()
        requested_after = []

        async def snapshot(symbol):
            requested_after.append(websocket.received)
            return {"seq": 100, "u": 10, "b": [], "a": []}

        async def immediate_wait_for(coroutine, timeout):
            return run_immediate(coroutine)

        class CompletedTask:
            def __init__(self, coroutine):
                self.result = run_immediate(coroutine)

            def done(self):
                return True

            def __await__(self):
                if False:
                    yield None
                return self.result

        with patch("script.market_data.get_orderbook_snapshot", side_effect=snapshot), \
             patch("script.market_data.asyncio.wait_for", side_effect=immediate_wait_for), \
             patch("script.market_data.asyncio.create_task", side_effect=CompletedTask):
            result, last_price = run_immediate(
                _fetch_snapshot_while_buffering(
                    websocket, "BTCUSDT", TradeFlow(window_seconds=10), []
                )
            )

        self.assertEqual(requested_after, [1])
        self.assertEqual((result["seq"], result["u"]), (100, 10))
        self.assertIsNone(last_price)


class MarketDataReconnectTests(unittest.TestCase):
    def test_reconnection_reuses_worker_and_clears_error_after_sync(self):
        engine = TradingEngine({"BTCUSDT": PairSettings("BTCUSDT")})
        engine.start()
        attempts = []

        async def connection(symbol, current_engine, pair_settings):
            attempts.append(symbol)
            if len(attempts) == 1:
                raise ConnectionError("temporary disconnect")
            current_engine.set_feed_status(symbol, "connected")
            current_engine.mode = EngineMode.STOPPED

        async def no_sleep(_delay):
            return None

        with patch("script.market_data._run_connection", side_effect=connection), \
             patch("script.market_data.asyncio.sleep", side_effect=no_sleep):
            run_immediate(
                market_data_worker("BTCUSDT", engine, engine.pair_settings["BTCUSDT"])
            )

        self.assertEqual(attempts, ["BTCUSDT", "BTCUSDT"])
        self.assertEqual(engine.feed_status["BTCUSDT"], "stopped")
        self.assertIsNone(engine.feed_errors["BTCUSDT"])


class WallStrategyTests(unittest.TestCase):
    def setUp(self):
        self.settings = PairSettings(
            "BTCUSDT", entry_distance_bps=2, stop_distance_bps=2,
            bounce_reversal_bps=1, breakout_confirm_seconds=5,
            breakout_buffer_bps=0.5,
        )

    def wall(self, side="buy", price=100, identity="wall-1", age=40):
        return {
            "symbol": "BTCUSDT", "side": side, "price": price,
            "first_seen_at": identity, "current_size": 1000,
            "initial_size": 1000, "wall_ratio": 900, "age": age,
        }

    def context(self, strategy, price, now, walls=None, events=()):
        return StrategyContext(
            "BTCUSDT", price, tuple(walls or ()), tuple(events), self.settings, now
        )

    def test_bounce_support_creates_plan_immediately_without_reversal_confirmation(self):
        strategy = BounceStrategy()
        wall = self.wall()
        signal, = strategy.evaluate(self.context(strategy, 100.01, 1, [wall]))
        self.assertEqual(signal.side, "long")
        self.assertEqual(signal.entry_mode, "cross")
        self.assertEqual(signal.entry_trigger_direction, "cross_down")
        repeated, = strategy.evaluate(self.context(strategy, 100.03, 3, [wall]))
        self.assertEqual(repeated.wall_id, signal.wall_id)

    def test_bounce_resistance_is_short_and_wall_loss_clears_state(self):
        strategy = BounceStrategy()
        wall = self.wall("sell", identity="old")
        signal, = strategy.evaluate(self.context(strategy, 99.99, 1, [wall]))
        self.assertEqual(signal.side, "short")
        self.assertEqual(signal.entry_trigger_direction, "cross_up")

    def test_bounce_requires_full_stability_and_skips_bad_reward_obstacle(self):
        strategy = BounceStrategy()
        unstable = self.wall(age=39)
        self.assertEqual(strategy.evaluate(self.context(strategy, 100.01, 1, [unstable])), [])
        wall = self.wall()
        strategy.evaluate(self.context(strategy, 100.01, 2, [wall]))
        obstacle = self.wall("sell", 100.3, "obstacle")
        signals = strategy.evaluate(self.context(strategy, 100.2, 3, [wall, obstacle]))
        self.assertEqual({signal.wall_side for signal in signals}, {"buy", "sell"})

    def test_all_qualified_stable_supports_are_bounce_signal_candidates(self):
        strategy = BounceStrategy()
        nearest = self.wall("buy", 100.0, "nearest")
        farther = self.wall("buy", 99.98, "farther")
        strategy.evaluate(self.context(strategy, 100.01, 1, [nearest, farther]))
        signals = strategy.evaluate(self.context(strategy, 100.02, 2, [nearest, farther]))
        self.assertEqual({signal.wall_id for signal in signals}, {
            "BTCUSDT:buy:100:nearest", "BTCUSDT:buy:99.98:farther",
        })
        self.assertTrue(all(signal.side == "long" for signal in signals))

    def test_all_qualified_stable_resistances_are_bounce_signal_candidates(self):
        strategy = BounceStrategy()
        nearest = self.wall("sell", 100.0, "nearest")
        farther = self.wall("sell", 100.02, "farther")
        strategy.evaluate(self.context(strategy, 99.99, 1, [nearest, farther]))
        signals = strategy.evaluate(self.context(strategy, 99.98, 2, [nearest, farther]))
        self.assertEqual({signal.wall_id for signal in signals}, {
            "BTCUSDT:sell:100:nearest", "BTCUSDT:sell:100.02:farther",
        })
        self.assertTrue(all(signal.side == "short" for signal in signals))

    def test_nearest_wall_with_invalid_rr_does_not_fall_through_to_farther_wall(self):
        strategy = BounceStrategy()
        nearest = self.wall("buy", 100.0, "nearest")
        farther = self.wall("buy", 99.98, "farther")
        # The opposite wall does not clip the agreed symmetric bounce target.
        obstacle = self.wall("sell", 100.04, "obstacle")
        strategy.evaluate(self.context(strategy, 100.01, 1, [nearest, farther, obstacle]))
        signals = strategy.evaluate(self.context(strategy, 100.02, 2, [nearest, farther, obstacle]))
        self.assertEqual({signal.wall_id for signal in signals}, {
            "BTCUSDT:buy:100:nearest", "BTCUSDT:buy:99.98:farther",
            "BTCUSDT:sell:100.04:obstacle",
        })

    def test_new_nearer_wall_is_added_without_removing_old_wall_signal(self):
        strategy = BounceStrategy()
        old = self.wall("buy", 100.0, "old")
        nearer = self.wall("buy", 100.015, "new")
        signals = strategy.evaluate(self.context(strategy, 100.02, 3, [old, nearer]))
        self.assertEqual({signal.wall_id for signal in signals}, {
            "BTCUSDT:buy:100:old", "BTCUSDT:buy:100.015:new",
        })

    def test_breakout_requires_hold_above_sell_wall_and_levels_are_directional(self):
        strategy = BreakoutStrategy()
        wall = self.wall("sell", 100.05)
        signal, = strategy.evaluate(self.context(strategy, 100.01, 10, [wall]))
        self.assertEqual(signal.side, "long")
        self.assertEqual(signal.entry_trigger_direction, "cross_up")

    def test_breakout_below_buy_wall_is_short_and_single_touch_is_not_enough(self):
        strategy = BreakoutStrategy()
        wall = self.wall("buy")
        self.assertEqual(strategy.evaluate(self.context(strategy, 99.99, 1, [wall])), [])
        signal, = strategy.evaluate(self.context(strategy, 100.01, 2, [wall]))
        self.assertEqual(signal.side, "short")
        self.assertEqual(signal.entry_trigger_direction, "cross_down")

    def test_breakout_signals_each_qualified_wall_for_book_side(self):
        strategy = BreakoutStrategy()
        farther = self.wall("sell", 100.04, "farther")
        nearer = self.wall("sell", 100.02, "nearer")
        signals = strategy.evaluate(self.context(strategy, 100.01, 6.1, [farther, nearer]))
        self.assertEqual({signal.wall_id for signal in signals}, {
            "BTCUSDT:sell:100.04:farther", "BTCUSDT:sell:100.02:nearer",
        })
        self.assertTrue(all(signal.side == "long" for signal in signals))

    def test_breakout_cancelled_when_price_falls_back_or_wall_is_removed(self):
        strategy = BreakoutStrategy()
        wall = self.wall("sell", 100.02)
        signal, = strategy.evaluate(self.context(strategy, 100.01, 0, [wall]))
        self.assertEqual(signal.entry_mode, "cross")
        self.assertEqual(
            strategy.evaluate(self.context(strategy, 100.02, 9, [], [{**wall, "event": "REMOVED"}])),
            [],
        )

    def test_confirmation_ignores_book_only_refresh_and_resets_after_price_gap(self):
        strategy = BreakoutStrategy()
        wall = self.wall("sell", 100.05)
        no_price_context = StrategyContext(
            "BTCUSDT", 100.02, (wall,), (), self.settings, 1, False
        )
        self.assertEqual(len(strategy.evaluate(no_price_context)), 1)
        self.assertEqual(len(strategy.evaluate(self.context(strategy, 100.02, 1, [wall]))), 1)

    def test_confirmed_signal_creates_virtual_trade_with_strategy_levels(self):
        strategy = BounceStrategy()
        wall = self.wall()
        strategy.evaluate(self.context(strategy, 100.01, 1, [wall]))
        signal, = strategy.evaluate(self.context(strategy, 100.02, 2, [wall]))
        manager = TradeManager(TradingSettings(stake_usdt=10, initial_balance_usdt=100))
        trade = manager.create_trade(signal, self.settings)
        self.assertEqual(trade["side"], "long")
        self.assertEqual(trade["strategy_id"], BOUNCE_STRATEGY_ID)
        self.assertEqual(trade["status"], "pending")
        self.assertIsNone(trade["opened_at"])
        self.assertIsNone(manager.create_trade(signal, self.settings))
        manager.on_price("BTCUSDT", trade["entry_price"])
        self.assertEqual(trade["status"], "open")
        self.assertIsNotNone(trade["opened_at"])

    def test_breakout_creates_pending_then_activates_on_price_confirmation(self):
        strategy = BreakoutStrategy()
        wall = self.wall("sell", 100.05)
        signal, = strategy.evaluate(self.context(strategy, 100.02, 10, [wall]))
        manager = TradeManager(TradingSettings(stake_usdt=10, initial_balance_usdt=100))

        trade = manager.create_trade(signal, self.settings)

        self.assertEqual(trade["status"], "pending")
        self.assertIsNone(trade["opened_at"])
        self.assertEqual(manager.on_price("BTCUSDT", trade["entry_price"])[0]["event"], "ENTRY_ACTIVATED")
        self.assertEqual(trade["status"], "open")

    def test_one_wall_does_not_create_conflicting_strategy_trades(self):
        manager = TradeManager(TradingSettings(stake_usdt=10, initial_balance_usdt=100))
        signal = {
            "strategy_id": "bounce", "symbol": "BTCUSDT", "wall_side": "buy",
            "wall_price": 100, "wall_id": "BTCUSDT:buy:100:lifecycle-1",
            "wall_size": 1000, "wall_ratio": 900, "side": "long",
            "entry_price": 100.02, "stop_loss": 99.98, "take_profit": 100.08,
            "entry_mode": "market",
        }
        self.assertIsNotNone(manager.create_trade(signal, self.settings))
        conflicting = dict(signal, strategy_id="breakout", side="short")
        self.assertIsNone(manager.create_trade(conflicting, self.settings))

    def test_repeat_after_websocket_reconnect_does_not_reopen_signal(self):
        manager = TradeManager(TradingSettings(stake_usdt=10, initial_balance_usdt=100))
        signal = {
            "strategy_id": "bounce", "symbol": "BTCUSDT", "wall_side": "buy",
            "wall_price": 100, "wall_id": "BTCUSDT:buy:100:lifecycle-1",
            "wall_size": 1000, "wall_ratio": 900, "side": "long",
            "entry_price": 100.02, "stop_loss": 99.98, "take_profit": 100.08,
            "entry_mode": "market",
        }
        first = manager.create_trade(signal, self.settings)
        # A feed reconnect does not replace the in-memory execution manager.
        second = manager.create_trade(signal, self.settings)
        self.assertIsNotNone(first)
        self.assertIsNone(second)
        self.assertEqual(len(manager.trades), 1)

    def test_concurrent_duplicate_signal_is_inserted_once(self):
        from concurrent.futures import ThreadPoolExecutor

        manager = TradeManager(TradingSettings(stake_usdt=10, initial_balance_usdt=100))
        signal = {
            "strategy_id": "bounce", "symbol": "BTCUSDT", "wall_side": "buy",
            "wall_price": 100, "wall_id": "BTCUSDT:buy:100:lifecycle-1",
            "wall_size": 1000, "wall_ratio": 900, "side": "long",
            "entry_price": 100.02, "stop_loss": 99.98, "take_profit": 100.08,
            "entry_mode": "market",
        }
        with ThreadPoolExecutor(max_workers=8) as pool:
            outcomes = list(pool.map(
                lambda _: manager.create_trade(signal, self.settings), range(16)
            ))
        self.assertEqual(sum(result is not None for result in outcomes), 1)
        self.assertEqual(len(manager.trades), 1)

    def test_restored_trade_prevents_reopening_same_persisted_signal(self):
        manager = TradeManager(TradingSettings(stake_usdt=10, initial_balance_usdt=100))
        signal = {
            "strategy_id": "bounce", "symbol": "BTCUSDT", "wall_side": "buy",
            "wall_price": 100, "wall_id": "BTCUSDT:buy:100:lifecycle-1",
            "wall_size": 1000, "wall_ratio": 900, "side": "long",
            "entry_price": 100.02, "stop_loss": 99.98, "take_profit": 100.08,
            "entry_mode": "market",
        }
        created = manager.create_trade(signal, self.settings)
        restored = TradeManager(manager.trading_settings)
        restored.restore_state([dict(created)], manager.balance_usdt)
        self.assertIsNone(restored.create_trade(signal, self.settings))

    def test_restart_reuses_persisted_wall_lifecycle_id_before_reconsidering_signal(self):
        manager = TradeManager(TradingSettings(stake_usdt=10, initial_balance_usdt=100))
        signal = {
            "strategy_id": "bounce", "symbol": "BTCUSDT", "wall_side": "buy",
            "wall_price": 100, "wall_id": "BTCUSDT:buy:100:persisted-life",
            "wall_size": 1000, "wall_ratio": 900, "side": "long",
            "entry_price": 100.02, "stop_loss": 99.98, "take_profit": 100.08,
            "entry_mode": "market",
        }
        trade = manager.create_trade(signal, self.settings)
        restored = TradeManager(manager.trading_settings)
        restored.restore_state([dict(trade)], manager.balance_usdt)
        tracker = WallTracker(min_age_seconds=40)
        tracker.seed_lifecycle_identities(restored.get_wall_lifecycle_identities())
        wall = {
            "side": "buy", "price": 100.0, "notional": 1000.0,
            "wall_ratio": 900.0,
        }
        book = LocalOrderBook()
        book.bids[100.0] = 10.0
        tracker.update([wall], book, {"sell_volume_by_price_total": {}})
        tracked, = tracker.walls.values()
        self.assertEqual(tracked["first_seen_at"], "persisted-life")
        self.assertIsNone(restored.create_trade(signal, self.settings))
        book.bids.clear()
        tracker.update([], book, {"sell_volume_by_price_total": {}})
        book.bids[100.0] = 10.0
        tracker.update([wall], book, {"sell_volume_by_price_total": {}})
        recreated, = tracker.walls.values()
        self.assertNotEqual(recreated["first_seen_at"], "persisted-life")

    def test_distinct_pair_signals_are_not_blocked_by_open_trade_caps(self):
        manager = TradeManager(TradingSettings(
            stake_usdt=10, initial_balance_usdt=100, max_open_trades=2
        ))
        btc = {
            "strategy_id": "bounce", "symbol": "BTCUSDT", "wall_side": "buy",
            "wall_price": 100, "wall_id": "btc-life", "signal_id": "bounce:btc-life",
            "wall_size": 1000, "wall_ratio": 900, "side": "long",
            "entry_price": 100.02, "stop_loss": 99.98, "take_profit": 100.08,
            "entry_mode": "market",
        }
        eth = dict(btc, symbol="ETHUSDT", wall_id="eth-life", signal_id="bounce:eth-life")
        self.assertIsNotNone(manager.create_trade(btc, PairSettings("BTCUSDT", max_open_trades=1)))
        self.assertIsNotNone(manager.create_trade(eth, PairSettings("ETHUSDT", max_open_trades=1)))
        self.assertEqual(len(manager.get_open_trades()), 2)

    def test_bounce_trace_exposes_price_distance_and_approach_gate(self):
        strategy = BounceStrategy()
        wall = self.wall(price=100, age=55)
        _signals, traces = strategy.evaluate_with_diagnostics(
            self.context(strategy, 100.1, 1, [wall])
        )
        trace = traces[0]
        self.assertTrue(trace["wall_passed"])
        distance = next(c for c in trace["conditions"] if c["condition"] == "max_wall_distance_bps")
        self.assertTrue(distance["passed"])
        self.assertEqual(distance["threshold_bps"], self.settings.max_wall_distance_bps)
        self.assertTrue(trace["signal_formed"])

    def test_breakout_trace_exposes_buffer_and_hold_confirmation(self):
        strategy = BreakoutStrategy()
        wall = self.wall(side="sell", price=100.05, age=55)
        _signals, traces = strategy.evaluate_with_diagnostics(
            self.context(strategy, 100.046, 1, [wall])
        )
        trace = traces[0]
        side_condition = next(c for c in trace["conditions"] if c["condition"] == "wall_on_valid_side_of_price")
        self.assertTrue(side_condition["passed"])
        self.assertTrue(trace["signal_formed"])

    def test_signal_forming_for_each_qualified_wall_is_accepted(self):
        strategy = BounceStrategy()
        near = self.wall(price=100, identity="near")
        far = self.wall(price=99.998, identity="far")
        signals, traces = strategy.evaluate_with_diagnostics(self.context(strategy, 100.005, 1, [near, far]))
        self.assertEqual(len(signals), 2)
        far_trace = next(trace for trace in traces if trace["wall_id"].endswith(":far"))
        self.assertTrue(far_trace["signal_formed"])
        self.assertTrue(far_trace["signal_accepted"])


class StrategyPipelineTests(unittest.TestCase):
    def test_trade_manager_duplicate_rejection_is_attached_to_wall_trace(self):
        settings = PairSettings("BTCUSDT", entry_distance_bps=2, bounce_reversal_bps=1)
        engine = TradingEngine(
            {"BTCUSDT": settings},
            enabled_strategies={"BTCUSDT": {BOUNCE_STRATEGY_ID}},
        )
        engine.instrument_tick_sizes["BTCUSDT"] = 0.01
        wall_id = "BTCUSDT:buy:100:diagnostic-life"
        prepared_levels = calculate_trade_levels(
            strategy_id=BOUNCE_STRATEGY_ID, wall_side="buy", wall_price=100,
            tick_size=0.01,
            take_profit_bps=engine.trading_settings.default_take_profit_bps,
            stop_loss_bps=engine.trading_settings.default_stop_loss_bps,
        )
        existing = {
            "strategy_id": "bounce", "symbol": "BTCUSDT", "wall_side": "buy",
            "wall_price": 100, "wall_id": wall_id, "signal_id": "already-pending",
            "wall_size": 1000, "wall_ratio": 900, **prepared_levels,
            "entry_mode": "limit",
        }
        self.assertIsNotNone(engine.trade_manager.create_trade(existing, settings))
        engine.start()
        wall = {
            "side": "buy", "price": 100.0, "first_seen_at": "diagnostic-life",
            "current_size": 1000.0, "initial_size": 1000.0,
            "wall_ratio": 900.0, "age": 55.0,
        }
        engine.on_market_update("BTCUSDT", 100.01, [wall])
        engine.on_market_update("BTCUSDT", 100.02, [wall])

        diagnostic = engine.wall_diagnostics["BTCUSDT"]
        trace = diagnostic["planner_wall_decisions"][0]["strategies"][0]
        self.assertTrue(trace["signal_formed"])
        self.assertTrue(trace["signal_accepted"])
        self.assertTrue(trace["pending_created"])
        self.assertEqual(len(engine.trade_manager.get_pending_trades()), 1)
        self.assertEqual(engine.trade_manager.get_pending_trades()[0]["signal_id"], "already-pending")

    def test_unknown_strategy_configuration_fails_before_worker_start(self):
        with self.assertRaisesRegex(KeyError, "Unknown strategies"):
            TradingEngine(
                {"BTCUSDT": PairSettings("BTCUSDT")},
                enabled_strategies={"BTCUSDT": {"missing_strategy"}},
            )

    def test_strategy_configuration_for_unknown_pair_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "unknown pairs"):
            TradingEngine(
                {"BTCUSDT": PairSettings("BTCUSDT")},
                enabled_strategies={"ETHUSDT": set()},
            )

    def test_strategy_registry_validates_persisted_id_length(self):
        registry = StrategyRegistry()

        class TooLongStrategy:
            strategy_id = "s" * 61

            def evaluate(self, context):
                return []

        class MissingIdStrategy:
            def evaluate(self, context):
                return []

        with self.assertRaisesRegex(ValueError, "up to 60"):
            registry.register(TooLongStrategy())
        with self.assertRaisesRegex(ValueError, "must start with"):
            registry.register(MissingIdStrategy())

        class UnusableIdStrategy:
            strategy_id = "strategy with spaces"

            def evaluate(self, context):
                return []

        with self.assertRaisesRegex(ValueError, "must start with"):
            registry.register(UnusableIdStrategy())

    def test_stable_wall_alone_does_not_create_a_trade_plan(self):
        book = LocalOrderBook()
        for index in range(20):
            price = 99.70 + index * 0.01
            book.bids[round(price, 2)] = 1.0
        book.bids[99.95] = 1000.0
        for index in range(20):
            price = 100.10 + index * 0.01
            book.asks[round(price, 2)] = 1.0

        settings = PairSettings(
            "BTCUSDT", max_wall_distance_bps=40, min_wall_age_seconds=0
        )
        nearby = find_nearby_levels(book, 100.0, settings.max_wall_distance_bps)
        flow = {
            "buy_volume_per_second": 0.0,
            "sell_volume_per_second": 0.0,
            "buy_volume_by_price_total": {},
            "sell_volume_by_price_total": {},
        }
        candidates = find_walls(
            nearby, flow, min_wall_ratio=settings.min_wall_ratio
        )
        tracker = WallTracker(
            min_age_seconds=settings.min_wall_age_seconds,
            consumed_ratio=settings.consumed_ratio,
        )
        wall_events = tracker.update(candidates, book, flow)
        stable_walls = tracker.get_nearest_stable_walls(100.0)
        engine = TradingEngine({"BTCUSDT": settings})
        engine.start()

        created = engine.on_market_update(
            "BTCUSDT", 100.0, stable_walls, wall_events, candidates
        )
        repeated = engine.on_market_update(
            "BTCUSDT", 100.0, stable_walls, (), candidates
        )

        self.assertTrue(any(wall["side"] == "buy" for wall in stable_walls))
        self.assertEqual(created, [])
        self.assertEqual(repeated, [])
        self.assertEqual(len(engine.trade_manager.get_pending_trades()), 0)

    def test_removed_and_consumed_wall_events_cancel_matching_pending_entry(self):
        for event_type in ("REMOVED", "CONSUMED"):
            with self.subTest(event=event_type):
                settings = PairSettings("BTCUSDT", min_wall_age_seconds=0)
                engine = TradingEngine({"BTCUSDT": settings})
                engine.start()
                identity = f"lifecycle-{event_type}"
                signal = {
                    "strategy_id": "stable_wall_v1",
                    "symbol": "BTCUSDT",
                    "wall_side": "buy",
                    "wall_price": 100.0,
                    "wall_id": f"BTCUSDT:buy:100:{identity}",
                    "wall_size": 1000.0,
                    "wall_ratio": 900.0,
                }
                trade = engine.trade_manager.create_trade(signal, settings)

                events = engine.on_market_update(
                    "BTCUSDT",
                    100.0,
                    [],
                    [{"event": event_type, "side": "buy", "price": 100.0,
                      "first_seen_at": identity}],
                )

                self.assertEqual(trade["status"], "cancelled")
                self.assertEqual(trade["close_reason"], "NO_STABLE_WALL")
                self.assertEqual(events[0]["event"], "ENTRY_CANCELLED")

    def test_custom_strategy_can_be_registered_and_enabled_per_pair(self):
        class CustomStrategy:
            strategy_id = "custom_test_strategy"

            def evaluate(self, context):
                return [
                    TradeSignal(
                        strategy_id=self.strategy_id,
                        symbol=context.symbol,
                        wall_side="buy",
                        wall_price=100.0,
                        wall_id="custom-wall-id",
                        wall_size=1000.0,
                        wall_ratio=900.0,
                        side="long", entry_price=100.5,
                        stop_loss=99.5, take_profit=101.5,
                        entry_mode="market",
                    )
                ]

        registry = StrategyRegistry()
        registry.register(CustomStrategy())
        engine = TradingEngine(
            {"BTCUSDT": PairSettings("BTCUSDT")},
            strategy_registry=registry,
            enabled_strategies={"BTCUSDT": {"custom_test_strategy"}},
        )
        engine.start()

        events = engine.on_market_update("BTCUSDT", 101.0, [])

        self.assertEqual([event["event"] for event in events], ["TRADE_CREATED"])
        self.assertEqual(events[0]["trade"]["strategy_id"], "custom_test_strategy")

    def test_custom_strategy_is_not_enabled_without_pair_opt_in(self):
        calls = []

        class CustomStrategy:
            strategy_id = "custom_opt_in_strategy"

            def evaluate(self, context):
                calls.append(context.symbol)
                return []

        registry = StrategyRegistry()
        registry.register(CustomStrategy())
        engine = TradingEngine(
            {"BTCUSDT": PairSettings("BTCUSDT")},
            strategy_registry=registry,
        )
        engine.start()

        self.assertEqual(engine.on_market_update("BTCUSDT", 100.0, []), [])
        self.assertEqual(calls, [])

    def test_strategy_failure_is_isolated_and_reported(self):
        class FailingStrategy:
            strategy_id = "failing_strategy"

            def evaluate(self, context):
                raise RuntimeError("strategy implementation failed")

        class WorkingStrategy:
            strategy_id = "working_strategy"

            def evaluate(self, context):
                return [
                    TradeSignal(
                        strategy_id=self.strategy_id,
                        symbol=context.symbol,
                        wall_side="buy",
                        wall_price=100.0,
                        wall_id="working-strategy-wall",
                        wall_size=1000.0,
                        wall_ratio=900.0,
                        side="long", entry_price=100.5,
                        stop_loss=99.5, take_profit=101.5,
                        entry_mode="market",
                    )
                ]

        registry = StrategyRegistry()
        registry.register(FailingStrategy())
        registry.register(WorkingStrategy())
        engine = TradingEngine(
            {"BTCUSDT": PairSettings("BTCUSDT")},
            strategy_registry=registry,
            enabled_strategies={
                "BTCUSDT": {"failing_strategy", "working_strategy"}
            },
        )
        engine.start()

        events = engine.on_market_update("BTCUSDT", 100.1, [])

        self.assertEqual([event["event"] for event in events], ["TRADE_CREATED"])
        self.assertEqual(
            engine.get_status()["strategy_errors"],
            {"BTCUSDT": {"failing_strategy": "RuntimeError"}},
        )


class AdminAuthorizationTests(unittest.TestCase):
    def test_cancel_input_clears_fsm_and_returns_to_dashboard(self):
        class FakeState:
            cleared = False

            async def clear(self):
                self.cleared = True

        class FakeMessage:
            text = None
            reply_markup = None

            async def edit_text(self, text, reply_markup=None):
                self.text = text
                self.reply_markup = reply_markup

        class FakeCallback:
            def __init__(self):
                self.message = FakeMessage()
                self.answer_text = None

            async def answer(self, text):
                self.answer_text = text

        runtime = RuntimeController(
            TradingEngine({"BTCUSDT": PairSettings("BTCUSDT")})
        )
        state = FakeState()
        callback = FakeCallback()

        run_immediate(cancel_input(callback, runtime, state))

        self.assertTrue(state.cleared)
        self.assertIn("Админ-панель", callback.message.text)
        self.assertEqual(callback.answer_text, "Ввод отменён")
        self.assertIsNotNone(callback.message.reply_markup)

    def test_dashboard_displays_floating_pnl_and_equity(self):
        engine = TradingEngine({"BTCUSDT": PairSettings("BTCUSDT")})
        engine.cancelled_trade_count = 34000
        engine.last_cancelled_cleanup_at = datetime(2026, 11, 3, 10, 30, tzinfo=timezone.utc)
        engine.last_cancelled_cleanup_deleted = 500
        runtime = RuntimeController(engine)

        text = dashboard_text(runtime)

        self.assertIn("Плавающий P/L: +0.00 USDT", text)
        self.assertIn("оценка баланса: 1000.00 USDT", text)
        self.assertIn("Доходность: +0.00%", text)
        self.assertIn("прибыльных сделок: 0.0%", text)
        self.assertIn("Доступно для новых сделок: 1000.00 USDT", text)
        self.assertIn("Общий лимит сделок не задан", text)
        self.assertIn("CANCELLED в PostgreSQL: 34000", text)
        self.assertIn("удалено 500", text)

    def test_dashboard_shows_algorithm_feed_analysis_and_database_status(self):
        engine = TradingEngine({"BTCUSDT": PairSettings("BTCUSDT")})
        runtime = RuntimeController(engine)
        text = dashboard_text(runtime)
        self.assertIn("Алгоритм: остановлен", text)
        self.assertIn("Возраст данных: BTCUSDT: возраст данных нет данных", text)
        self.assertIn("Поиск и анализ стен: остановлен", text)
        self.assertIn("Последнее обновление данных: нет данных", text)
        self.assertIn("PostgreSQL: не настроена", text)

        engine.start()
        engine.set_feed_status("BTCUSDT", "connected")
        engine.last_market_update_at["BTCUSDT"] = engine._utc_now() - timedelta(seconds=5)
        self.assertEqual(engine.get_status()["algorithm_status"], "waiting")
        engine._record_market_analysis("BTCUSDT", [], [])
        engine.last_market_update_at["BTCUSDT"] = engine._utc_now()
        engine.last_algorithm_update_at["BTCUSDT"] = engine._utc_now()
        self.assertEqual(engine.get_status()["algorithm_status"], "working")

        engine.set_feed_status("BTCUSDT", "reconnecting", OSError("socket closed"))
        status = engine.get_status()
        self.assertEqual(status["algorithm_status"], "error")
        self.assertEqual(status["feed_errors"]["BTCUSDT"]["message"], "socket closed")

    def test_pair_statistics_card_uses_pair_settings_and_marks_missing_metrics(self):
        settings = replace(
            PairSettings("BTCUSDT"),
            min_wall_ratio=12.5,
            min_wall_notional_usdt=2500,
            max_wall_distance_bps=80,
        )
        runtime = RuntimeController(TradingEngine({"BTCUSDT": settings}))
        text = pair_statistics_text(runtime, "  btcusdt ")
        self.assertIn("BTCUSDT", text)
        self.assertIn("12.5×", text)
        self.assertIn("2500 USDT", text)
        self.assertIn("80 б.п.", text)
        self.assertIn("Потенциальные стены: нет данных", text)
        self.assertIn("Цена: нет данных", text)
        self.assertIn("Причина отсутствия входа:", text)

    def test_unknown_pair_reports_configured_pairs(self):
        runtime = RuntimeController(
            TradingEngine({"BTCUSDT": PairSettings("BTCUSDT"), "ETCUSDT": PairSettings("ETCUSDT")})
        )
        text = pair_statistics_text(runtime, " unknown ")
        self.assertIn("BTCUSDT, ETCUSDT", text)

    def test_pair_metrics_count_candidates_separately_from_stable_walls(self):
        engine = TradingEngine({"BTCUSDT": PairSettings("BTCUSDT")})
        candidates = [{"side": "buy", "price": 100.0}, {"side": "sell", "price": 101.0}]
        engine._record_market_analysis("BTCUSDT", [candidates[0]], candidates)
        metrics = engine.get_status()["market_analysis"]["BTCUSDT"]
        self.assertEqual(metrics["potential_wall_count"], 2)
        self.assertEqual(metrics["stable_wall_count"], 1)
        self.assertIsNotNone(metrics["updated_at"])

    def test_unchanged_telegram_panel_is_not_edited(self):
        class FakeMessage:
            text = "same"
            reply_markup = None
            edits = 0

            async def edit_text(self, text, reply_markup=None):
                self.edits += 1

        message = FakeMessage()
        self.assertFalse(run_immediate(edit_message_if_changed(message, "same")))
        self.assertEqual(message.edits, 0)

    def test_pair_settings_screen_displays_consumption_threshold_and_strategy(self):
        runtime = RuntimeController(
            TradingEngine({"BTCUSDT": PairSettings("BTCUSDT")})
        )

        text = pair_settings_text(runtime)

        self.assertIn("порог потребления 90%", text)
        self.assertIn("стратегии stable_wall_v1", text)
        self.assertNotIn("лимит сделок", text)

    def test_admin_ids_load_one_or_many_with_whitespace_and_deduplication(self):
        with patch.dict(os.environ, {"ADMIN_USER_ID": "7319"}):
            self.assertEqual(_load_admin_user_ids(), {7319})
        with patch.dict(os.environ, {"ADMIN_USER_ID": " 7319, 8420,7319, 9650 "}):
            self.assertEqual(_load_admin_user_ids(), {7319, 8420, 9650})

    def test_empty_or_invalid_admin_id_configuration_fails_closed(self):
        with patch.dict(os.environ, {"ADMIN_USER_ID": ""}):
            with self.assertRaisesRegex(RuntimeError, "Set ADMIN_USER_ID"):
                _load_admin_user_ids()
        for value in ("not-a-number", "7319,,8420", "7319,", "0", "-7319"):
            with self.subTest(value=value), patch.dict(
                os.environ, {"ADMIN_USER_ID": value}
            ):
                with self.assertRaisesRegex(RuntimeError, "positive integers"):
                    _load_admin_user_ids()

    @staticmethod
    def make_message(user_id, chat_id, chat_type):
        return Message(
            message_id=1,
            date=datetime.now(timezone.utc),
            chat=Chat(id=chat_id, type=chat_type),
            from_user=User(id=user_id, is_bot=False, first_name="Test"),
        )

    def test_only_configured_admin_in_private_chat_passes_filter(self):
        admin_filter = Admin()
        first_admin = self.make_message(7319, 7319, "private")
        second_admin = self.make_message(8420, 8420, "private")
        non_admin = self.make_message(4243, 4243, "private")
        admin_in_group = self.make_message(7319, -10042, "supergroup")

        with patch("app.admin.ADMIN_USER_IDS", {7319, 8420}):
            self.assertTrue(run_immediate(admin_filter(first_admin)))
            self.assertTrue(run_immediate(admin_filter(second_admin)))
            self.assertFalse(run_immediate(admin_filter(non_admin)))
            self.assertFalse(run_immediate(admin_filter(admin_in_group)))

    def test_id_command_works_for_non_admin_without_granting_admin_access(self):
        user_id = 4243
        admin_filter = Admin()
        answers = []
        message = self.make_message(user_id, user_id, "private")

        async def answer(_message, text):
            answers.append(text)

        with patch("app.admin.ADMIN_USER_IDS", {7319, 8420}), patch.object(
            Message, "answer", answer
        ):
            run_immediate(dayid(message))
            self.assertEqual(answers, [str(user_id)])
            self.assertFalse(run_immediate(admin_filter(message)))


class StrategyCallbackTests(unittest.TestCase):
    class FakeMessage:
        def __init__(self, edit_error=None):
            self.text = None
            self.reply_markup = None
            self.edit_text = AsyncMock(side_effect=edit_error)
            self.answer = AsyncMock()

    class FakeCallback:
        def __init__(self, data, edit_error=None, user_id=7319):
            self.data = data
            self.from_user = User(id=user_id, is_bot=False, first_name="Admin")
            self.message = StrategyCallbackTests.FakeMessage(edit_error)
            self.answer = AsyncMock()

    def test_breakout_switch_to_off_and_repeated_selection_are_successful(self):
        repository = FakeRepository()
        engine = TradingEngine(
            {"BTCUSDT": PairSettings("BTCUSDT")},
            enabled_strategies={"BTCUSDT": {"bounce"}},
            strategy_versions={"BTCUSDT": 1}, repository=repository,
        )
        runtime = RuntimeController(engine)

        run_immediate(runtime.update_enabled_strategies("BTCUSDT", {"breakout"}))
        run_immediate(runtime.update_enabled_strategies("BTCUSDT", set()))
        self.assertEqual(engine.get_enabled_strategy_ids("BTCUSDT"), set())
        self.assertEqual(repository.saved_enabled_strategies[-2:], [
            ("BTCUSDT", {"breakout"}), ("BTCUSDT", set())
        ])
        version = engine.strategy_versions["BTCUSDT"]
        run_immediate(runtime.update_enabled_strategies(
            "BTCUSDT", set(), expected_version=version - 1
        ))
        self.assertEqual(engine.strategy_versions["BTCUSDT"], version)

    def test_large_admin_id_switch_is_persisted_and_confirmed_in_telegram(self):
        admin_user_id = 8445920246
        repository = FakeRepository()
        engine = TradingEngine(
            {"BTCUSDT": PairSettings("BTCUSDT")},
            enabled_strategies={"BTCUSDT": {"bounce"}},
            repository=repository,
        )
        callback = self.FakeCallback(
            "stratset:BTCUSDT:breakout:0", user_id=admin_user_id
        )

        run_immediate(set_pair_strategy(callback, RuntimeController(engine)))

        self.assertEqual(engine.get_enabled_strategy_ids("BTCUSDT"), {"breakout"})
        self.assertEqual(repository.saved_admin_user_ids, [admin_user_id])
        self.assertIn("сохранён", callback.message.edit_text.await_args.args[0])
        self.assertEqual(callback.answer.await_count, 1)

    def test_callback_refresh_failure_does_not_report_database_save_failure(self):
        repository = FakeRepository()
        engine = TradingEngine({"BTCUSDT": PairSettings("BTCUSDT")}, repository=repository)
        runtime = RuntimeController(engine)
        callback = self.FakeCallback("stratset:BTCUSDT:breakout:0", RuntimeError("Telegram offline"))

        run_immediate(set_pair_strategy(callback, runtime))

        self.assertEqual(engine.get_enabled_strategy_ids("BTCUSDT"), {"breakout"})
        self.assertEqual(engine.strategy_versions["BTCUSDT"], 1)
        self.assertIn("сохранён", callback.answer.await_args.args[0])
        self.assertIn("сохранён в PostgreSQL", callback.message.answer.await_args.args[0])
        self.assertEqual(repository.saved_enabled_strategies, [("BTCUSDT", {"breakout"})])

    def test_database_save_failure_keeps_previous_runtime_mode(self):
        repository = FakeRepository()
        repository.fail_strategy_saves = True
        engine = TradingEngine(
            {"BTCUSDT": PairSettings("BTCUSDT")},
            enabled_strategies={"BTCUSDT": {"bounce"}}, repository=repository,
        )
        runtime = RuntimeController(engine)
        callback = self.FakeCallback("stratset:BTCUSDT:breakout:0")

        run_immediate(set_pair_strategy(callback, runtime))

        self.assertEqual(engine.get_enabled_strategy_ids("BTCUSDT"), {"bounce"})
        self.assertIn("PostgreSQL", callback.answer.await_args.args[0])
        self.assertTrue(callback.answer.await_args.kwargs["show_alert"])
        self.assertEqual(callback.message.edit_text.await_count, 0)

    def test_stale_strategy_callback_gets_refresh_message_without_database_write(self):
        repository = FakeRepository()
        engine = TradingEngine(
            {"BTCUSDT": PairSettings("BTCUSDT")},
            enabled_strategies={"BTCUSDT": {"bounce"}},
            strategy_versions={"BTCUSDT": 2}, repository=repository,
        )
        callback = self.FakeCallback("stratset:BTCUSDT:breakout:1")

        run_immediate(set_pair_strategy(callback, RuntimeController(engine)))

        self.assertEqual(engine.get_enabled_strategy_ids("BTCUSDT"), {"bounce"})
        self.assertIn("уже изменилась", callback.answer.await_args.args[0])
        self.assertEqual(repository.saved_enabled_strategies, [])

    def test_worker_sync_error_is_reported_as_post_commit_state(self):
        from app.runtime import StrategyWorkerSyncError

        repository = FakeRepository()
        engine = TradingEngine({"BTCUSDT": PairSettings("BTCUSDT")}, repository=repository)
        runtime = RuntimeController(engine)
        runtime._sync_workers = AsyncMock(side_effect=OSError("worker unavailable"))

        with self.assertRaises(StrategyWorkerSyncError):
            run_immediate(runtime.update_enabled_strategies("BTCUSDT", {"breakout"}))

        self.assertEqual(engine.get_enabled_strategy_ids("BTCUSDT"), {"breakout"})
        self.assertEqual(repository.saved_enabled_strategies, [("BTCUSDT", {"breakout"})])
        self.assertEqual(engine.feed_status["BTCUSDT"], "error")

    def test_admin_filter_rejects_non_admin_strategy_callback(self):
        from types import SimpleNamespace

        user = User(id=999999, is_bot=False, first_name="Guest")
        callback = SimpleNamespace(
            from_user=user,
            message=SimpleNamespace(chat=Chat(id=user.id, type="private")),
        )
        self.assertFalse(run_immediate(Admin()(callback)))


class TradeLevelTelegramTests(unittest.TestCase):
    def test_global_and_pair_menus_distinguish_overrides_from_effective_values(self):
        engine = TradingEngine(
            {
                "BTCUSDT": PairSettings("BTCUSDT", take_profit_bps=50),
                "ETHUSDT": PairSettings("ETHUSDT", stop_loss_bps=25),
            },
            TradingSettings(default_take_profit_bps=40, default_stop_loss_bps=40),
        )
        runtime = RuntimeController(engine)

        global_text = trade_levels_text(runtime)
        btc_text = trade_level_pair_text(runtime, "btcusdt")
        eth_text = trade_level_pair_text(runtime, "ETHUSDT")
        self.assertIn("Глобальный TP от Entry: <b>40 б.п.</b>", global_text)
        self.assertIn("BTCUSDT: эффективно TP 50 / SL 40 б.п.", global_text)
        self.assertIn("персональное 50 б.п.", btc_text)
        self.assertIn("глобальное 40 б.п.", btc_text)
        self.assertIn("Эффективно: TP 50 / SL 40 б.п.", btc_text)
        self.assertIn("персональное 25 б.п.", eth_text)
        self.assertIn("Эффективно: TP 40 / SL 25 б.п.", eth_text)

        global_callbacks = [
            button.callback_data for row in trade_levels_keyboard().inline_keyboard
            for button in row
        ]
        pair_callbacks = [
            button.callback_data for row in trade_level_pair_keyboard(runtime, "BTCUSDT").inline_keyboard
            for button in row
        ]
        self.assertIn("levels_set:global:tp", global_callbacks)
        self.assertIn("levels_set:global:sl", global_callbacks)
        self.assertIn("levels_reset:BTCUSDT:tp", pair_callbacks)
        self.assertIn("levels_reset:BTCUSDT:sl", pair_callbacks)


class DealManagerTelegramTests(unittest.TestCase):
    def make_runtime(self):
        settings = TradingSettings(stake_usdt=10, initial_balance_usdt=100, max_open_trades=10)
        manager = TradeManager(settings)
        engine = TradingEngine(
            {"BTCUSDT": PairSettings("BTCUSDT")}, settings, trade_manager=manager
        )
        runtime = RuntimeController(engine)
        common = {
            "strategy_id": "bounce", "symbol": "BTCUSDT", "wall_side": "buy",
            "wall_price": 100, "wall_size": 5000, "wall_ratio": 900,
            "wall_id": "BTCUSDT:buy:100:lifecycle-1", "entry_price": 100.01,
            "stop_loss": 99.9, "take_profit": 100.2,
        }
        opened = manager.create_trade({**common, "signal_id": "deal-open-1", "entry_mode": "market"}, engine.pair_settings["BTCUSDT"])
        pending = manager.create_trade({**common, "wall_id": "BTCUSDT:buy:99:lifecycle-2", "wall_price": 99, "signal_id": "deal-pending-1"}, engine.pair_settings["BTCUSDT"])
        manager.on_price("BTCUSDT", 100.05, process_open=False)
        return runtime, opened, pending

    def test_open_and_pending_cards_show_required_virtual_trade_fields(self):
        runtime, opened, pending = self.make_runtime()
        open_text, open_keyboard = _deal_card(runtime, opened)
        pending_text, pending_keyboard = _deal_card(runtime, pending)
        self.assertIn("OPEN", open_text)
        self.assertIn("PnL", open_text)
        expected_pnl = runtime.engine.trade_manager.trade_unrealized_pnl(
            opened, runtime.engine.trade_manager.last_prices["BTCUSDT"]
        )
        self.assertIn(f"{expected_pnl:+.4f} USDT", open_text)
        self.assertIn("Номинал", open_text)
        self.assertIn("MAKER_MAKER_SIMULATION", open_text)
        self.assertIn("применено 0.0360% на вход/выход", open_text)
        self.assertIn("PENDING", pending_text)
        self.assertIn("не резервируется", pending_text)
        self.assertIn("ARMED", pending_text)
        self.assertLessEqual(len(open_text), 4096)
        self.assertLessEqual(len(pending_text), 4096)
        self.assertTrue(all(len(button.callback_data) <= 64 for row in open_keyboard.inline_keyboard for button in row))
        self.assertTrue(all(len(button.callback_data) <= 64 for row in pending_keyboard.inline_keyboard for button in row))

    def test_lists_paginate_and_keep_telegram_payloads_compact(self):
        runtime, opened, _ = self.make_runtime()
        manager = runtime.engine.trade_manager
        for index in range(8):
            item = dict(opened, signal_id=f"open-extra-{index}", symbol="BTCUSDT")
            manager.trades.append(item)
        page_text, keyboard = _deal_list_text(runtime, "open", 1)
        self.assertIn("стр.", page_text)
        self.assertLessEqual(len(page_text), 4096)
        self.assertTrue(all(len(button.callback_data) <= 64 for row in keyboard.inline_keyboard for button in row))


class StableWallTelegramTests(unittest.TestCase):
    def setUp(self):
        self.runtime = RuntimeController(TradingEngine({
            "BTCUSDT": PairSettings("BTCUSDT"),
            "ETHUSDT": PairSettings("ETHUSDT"),
        }))

    @staticmethod
    def walls(count: int, status: str, side: str = "buy", base: float = 100.0) -> list[dict]:
        return [{
            "side": side,
            "status": status,
            "price": base + index / 1000,
            "notional": 25000 + index,
            "distance_bps": 12.5 + index / 100,
            "age_seconds": 40.0 + index,
            "stability_seconds": 40.0,
            "progress_percent": 100.0,
            "change_notional": -125.5 + index,
        } for index in range(count)]

    def put_snapshot(self, symbol: str, stable: list[dict], candidates: list[dict]) -> None:
        self.runtime.engine.wall_diagnostics[symbol] = {
            "feed_state": "connected",
            "book_synchronized": True,
            "last_update_at": datetime.now(timezone.utc),
            "price": 100.0 if symbol == "BTCUSDT" else 200.0,
            "bid": stable + candidates,
            "ask": [],
            "stable_count": len(stable),
            "rejected": {"below_min_wall_ratio": 2},
            "reason": "Есть стабильные стены." if stable else "Кандидаты ещё ожидают устойчивости.",
            "planner_decision": "Текущая стратегия ещё не сформировала новый сигнал.",
        }

    def test_large_candidate_list_is_paginated_and_each_message_fits(self):
        self.put_snapshot("BTCUSDT", [], self.walls(1000, "CANDIDATE"))
        pages = (1000 + WALLS_PAGE_SIZE - 1) // WALLS_PAGE_SIZE
        for page in range(pages):
            text = stable_walls_text(self.runtime, "BTCUSDT", "candidate", page)
            self.assertLessEqual(len(text), TELEGRAM_TEXT_LIMIT)
            self.assertIn("CANDIDATE: 1000", text)
            self.assertIn(f"стр. {page + 1}", text)
        first = stable_walls_text(self.runtime, "BTCUSDT", "candidate", 0)
        self.assertEqual(first.count("BID <code>"), WALLS_PAGE_SIZE)

    def test_large_stable_wall_list_is_paginated_and_each_message_fits(self):
        self.put_snapshot("BTCUSDT", self.walls(1000, "STABLE"), [])
        pages = (1000 + WALLS_PAGE_SIZE - 1) // WALLS_PAGE_SIZE
        for page in range(pages):
            text = stable_walls_text(self.runtime, "BTCUSDT", "stable", page)
            self.assertLessEqual(len(text), TELEGRAM_TEXT_LIMIT)
            self.assertIn("STABLE: 1000", text)
        self.assertEqual(
            stable_walls_text(self.runtime, "BTCUSDT", "stable", pages - 1).count("BID <code>"),
            WALLS_PAGE_SIZE,
        )

    def test_empty_list_has_compact_empty_state(self):
        self.put_snapshot("BTCUSDT", [], [])
        text = stable_walls_text(self.runtime, "BTCUSDT", "stable")
        self.assertIn("Список пуст", text)
        self.assertLessEqual(len(text), TELEGRAM_TEXT_LIMIT)

    def test_pair_switch_and_section_navigation_callbacks(self):
        self.put_snapshot("BTCUSDT", self.walls(7, "STABLE"), [])
        self.put_snapshot("ETHUSDT", [], self.walls(7, "CANDIDATE", side="sell", base=200))
        btc = stable_walls_text(self.runtime, "BTCUSDT", "stable", 0)
        eth = stable_walls_text(self.runtime, "ETHUSDT", "candidate", 0)
        self.assertIn("BTCUSDT", btc)
        self.assertIn("ETHUSDT", eth)
        self.assertIn("ASK", eth)
        keyboard = stable_walls_keyboard(self.runtime, "BTCUSDT", "stable", 0)
        callbacks = [button.callback_data for row in keyboard.inline_keyboard for button in row]
        self.assertIn("walls:ETHUSDT:stable:0", callbacks)
        self.assertIn("walls:BTCUSDT:candidate:0", callbacks)
        self.assertIn("walls:BTCUSDT:diagnostics:0", callbacks)

    def test_page_callbacks_and_refresh_use_latest_wall_count(self):
        self.put_snapshot("BTCUSDT", self.walls(11, "STABLE"), [])
        keyboard = stable_walls_keyboard(self.runtime, "BTCUSDT", "stable", 0)
        callbacks = [button.callback_data for row in keyboard.inline_keyboard for button in row]
        self.assertIn("walls:BTCUSDT:stable:1", callbacks)

        class FakeMessage:
            text = "old"
            reply_markup = None

            async def edit_text(self, text, reply_markup=None):
                self.text = text
                self.reply_markup = reply_markup

        class FakeCallback:
            data = "walls:BTCUSDT:stable:1"
            message = FakeMessage()

            async def answer(self, text="", show_alert=False):
                self.answer_text = text

        callback = FakeCallback()
        run_immediate(navigate_stable_walls(callback, self.runtime))
        self.assertIn("стр. 2", callback.message.text)
        self.assertEqual(callback.message.text.count("BID <code>"), 5)

        self.put_snapshot("BTCUSDT", self.walls(12, "STABLE"), [])
        refreshed = stable_walls_text(self.runtime, "BTCUSDT", "stable", 0)
        self.assertIn("STABLE: 12", refreshed)

    def test_diagnostics_page_is_separate_and_bounded(self):
        self.put_snapshot("BTCUSDT", self.walls(10, "STABLE"), self.walls(10, "CANDIDATE"))
        data = self.runtime.engine.wall_diagnostics["BTCUSDT"]
        data["planner_strategies"] = ["bounce", "breakout"]
        data["scanner_filters"] = {
            "min_wall_ratio": 20, "min_wall_notional_usdt": 10000,
            "max_wall_distance_bps": 100,
        }
        data["scanner_diagnostics"] = {
            "side_medians": {"buy": 1000.0, "sell": 1200.0},
            "ratio_sweep": {
                "buy": {5: 7, 10: 5, 15: 4, 20: 3, 30: 2, 50: 1, 100: 0},
                "sell": {5: 6, 10: 4, 15: 3, 20: 2, 30: 1, 50: 0, 100: 0},
            },
            "largest_levels": [{
                "side": "buy", "price": 99.9, "notional": 50000,
                "wall_ratio": 50, "distance_bps": 10,
                "rejection": None,
            }],
        }
        data["planner_wall_decisions"] = [{
            "wall_id": str(index), "side": "buy", "price": 100 + index,
            "notional": 50000, "age_seconds": 55,
            "strategies": [{
                "strategy_id": "bounce", "wall_passed": True, "evaluated": True,
                "signal_formed": False, "conditions": [{
                    "condition": "entry_distance_bps", "passed": False,
                    "actual_bps": 8.2, "threshold_bps": 1.5,
                }], "reasons": ["Цена ещё не подошла к стене на допустимую дистанцию."],
            }, {
                "strategy_id": "breakout", "wall_passed": True, "evaluated": True,
                "signal_formed": False, "conditions": [{
                    "condition": "price_beyond_wall_buffer", "passed": False,
                    "actual_bps": -3.2, "threshold_bps": 0.5,
                }], "reasons": ["Цена ещё не вышла за стену на заданный буфер."],
            }],
        } for index in range(12)]
        text = stable_walls_text(self.runtime, "BTCUSDT", "diagnostics", 0)
        self.assertIn("Отклонения Scanner", text)
        self.assertIn("Trade Planner", text)
        self.assertIn("Ratio sweep (порог:BID/ASK)", text)
        self.assertIn("Крупнейшие уровни", text)
        self.assertIn("ratio 50.00x", text)
        self.assertIn("8.20/1.5 bps", text)
        self.assertLessEqual(len(text), TELEGRAM_TEXT_LIMIT)
        keyboard = stable_walls_keyboard(self.runtime, "BTCUSDT", "diagnostics", 0)
        callbacks = [button.callback_data for row in keyboard.inline_keyboard for button in row]
        self.assertIn("walls:BTCUSDT:diagnostics:1", callbacks)
        for page in range(3):
            self.assertLessEqual(
                len(stable_walls_text(self.runtime, "BTCUSDT", "diagnostics", page)),
                TELEGRAM_TEXT_LIMIT,
            )
        self.assertIn("стр. 2/3", stable_walls_text(self.runtime, "BTCUSDT", "diagnostics", 1))

    def test_edit_ignores_only_message_is_not_modified(self):
        from aiogram.exceptions import TelegramBadRequest

        class FakeMessage:
            text = "old"
            reply_markup = None

            async def edit_text(self, text, reply_markup=None):
                raise TelegramBadRequest(method=None, message="Bad Request: message is not modified")

        self.assertFalse(run_immediate(edit_message_if_changed(FakeMessage(), "new")))

        class FailingMessage(FakeMessage):
            async def edit_text(self, text, reply_markup=None):
                raise TelegramBadRequest(method=None, message="Bad Request: text is too long")

        with self.assertRaisesRegex(TelegramBadRequest, "text is too long"):
            run_immediate(edit_message_if_changed(FailingMessage(), "new"))


class PairSymbolValidationTests(unittest.TestCase):
    def test_doge_and_meme_token_symbols_are_valid_pair_symbols(self):
        self.assertEqual(normalize_pair_symbol(" dogeusdt "), "DOGEUSDT")
        self.assertEqual(normalize_pair_symbol("1000pepeusdt"), "1000PEPEUSDT")

    def test_build_runtime_restores_database_state_and_saved_run_mode(self):
        from database import request as database

        async def initialize(default_trading_settings, default_pair_settings):
            return {
                "trading_settings": TradingSettings(
                    stake_usdt=20, initial_balance_usdt=500, max_open_trades=4
                ),
                "balance_usdt": 515,
                "engine_mode": "running",
                "initialized": True,
                "pair_settings": default_pair_settings,
                "enabled_strategies": {"BTCUSDT": {"stable_wall_v1"}},
                "trades": [{"signal_id": "restored-trade", "status": "open"}],
                "last_prices": {"BTCUSDT": 101.5},
            }

        with (
            patch.object(database, "initialize_database", side_effect=initialize),
            patch.dict(os.environ, {"TRADING_SYMBOLS": "BTCUSDT"}),
        ):
            runtime, initialized = run_immediate(build_runtime())

        self.assertTrue(initialized)
        self.assertEqual(runtime.engine.mode, EngineMode.RUNNING)
        self.assertEqual(runtime.engine.trading_settings.stake_usdt, 20)
        self.assertEqual(runtime.engine.trade_manager.balance_usdt, 515)
        self.assertEqual(
            runtime.engine.trade_manager.trades[0]["signal_id"], "restored-trade"
        )
        self.assertEqual(runtime.engine.trade_manager.last_prices["BTCUSDT"], 101.5)

    def test_pair_settings_normalize_symbol_and_reject_non_linear_symbols(self):
        self.assertEqual(PairSettings(" ethusdt ").symbol, "ETHUSDT")
        with self.assertRaisesRegex(ValueError, "Linear USDT"):
            PairSettings("BTC/USD")
        with self.assertRaisesRegex(ValueError, "Linear USDT"):
            PairSettings(f"{'A' * 27}USDT")

    def test_startup_symbol_list_normalizes_deduplicates_and_rejects_invalid(self):
        with patch.dict(
            os.environ, {"TRADING_SYMBOLS": "btcusdt, ETHUSDT, BTCUSDT"}
        ):
            self.assertEqual(_symbols_from_environment(), ["BTCUSDT", "ETHUSDT"])
        with patch.dict(os.environ, {"TRADING_SYMBOLS": "BTC/USD"}):
            with self.assertRaisesRegex(RuntimeError, "invalid Bybit"):
                _symbols_from_environment()

    def test_consumed_ratio_must_be_positive(self):
        with self.assertRaisesRegex(ValueError, "greater than 0"):
            PairSettings("BTCUSDT", consumed_ratio=0)


class VirtualTradeTests(unittest.TestCase):
    def setUp(self):
        self.settings = PairSettings("BTCUSDT")
        self.trading_settings = TradingSettings(
            stake_usdt=10, initial_balance_usdt=100, max_open_trades=2
        )
        self.manager = TradeManager(self.trading_settings)
        self.signal = {
            "strategy_id": "stable_wall_v1",
            "symbol": "BTCUSDT",
            "wall_side": "buy",
            "wall_price": 100.0,
            "wall_id": "wall-1",
            "wall_size": 10000.0,
            "wall_ratio": 900.0,
            "side": "long",
        }

    def test_levels_follow_agreed_bps_and_x2(self):
        entry = calculate_entry_price(100, "buy", 1.5)
        stop = calculate_stop_loss_price(100, "buy", 2)
        target = calculate_x2_price(entry, stop, "buy")

        self.assertAlmostEqual(entry, 100.015)
        self.assertAlmostEqual(stop, 99.98)
        self.assertAlmostEqual(target - entry, entry - stop)

    def test_short_trade_uses_symmetric_entry_stop_and_target_levels(self):
        signal = dict(
            self.signal,
            wall_side="sell",
            wall_id="short-wall-1",
        )
        trade = self.manager.create_trade(signal, self.settings)

        self.assertAlmostEqual(trade["entry_price"], 99.985)
        self.assertAlmostEqual(trade["stop_loss"], 100.035)
        self.assertAlmostEqual(
            trade["entry_price"] - trade["take_profit"],
            trade["stop_loss"] - trade["entry_price"],
        )
        self.manager.on_price("BTCUSDT", 99.9)
        self.manager.on_price("BTCUSDT", 100.0)
        events = self.manager.on_price("BTCUSDT", 100.04)
        self.assertEqual(events[0]["event"], "TRADE_CLOSED")
        self.assertEqual(trade["close_reason"], "STOP_LOSS")
        self.assertLess(trade["profit_usdt"], 0)

    def test_short_trade_closes_on_take_profit_below_entry(self):
        signal = dict(self.signal, wall_side="sell", wall_id="short-tp-wall")
        trade = self.manager.create_trade(signal, self.settings)
        self.manager.on_price("BTCUSDT", 100.0)

        events = self.manager.on_price("BTCUSDT", trade["take_profit"])

        self.assertEqual([event["event"] for event in events], ["TRADE_CLOSED"])
        self.assertEqual(trade["close_reason"], "TAKE_PROFIT")
        self.assertEqual(trade["exit_price"], trade["take_profit"])

    def test_long_take_profit_overshoot_persists_actual_ticker_price(self):
        trade = self.manager.create_trade(self.signal, self.settings)
        self.manager.on_price("BTCUSDT", 100.0)
        market_price = trade["take_profit"] + 0.125

        events = self.manager.on_price("BTCUSDT", market_price)

        self.assertEqual(events[0]["event"], "TRADE_CLOSED")
        self.assertEqual(events[0]["trigger_price"], market_price)
        self.assertEqual(events[0]["fill_price"], market_price)
        self.assertEqual(events[0]["close_price"], market_price)
        self.assertEqual(trade["exit_price"], market_price)
        self.assertNotEqual(trade["exit_price"], trade["take_profit"])

    def test_short_take_profit_overshoot_persists_actual_ticker_price(self):
        signal = dict(self.signal, wall_side="sell", wall_id="short-tp-gap")
        trade = self.manager.create_trade(signal, self.settings)
        self.manager.on_price("BTCUSDT", 100.0)
        market_price = trade["take_profit"] - 0.125

        events = self.manager.on_price("BTCUSDT", market_price)

        self.assertEqual(events[0]["event"], "TRADE_CLOSED")
        self.assertEqual(trade["exit_price"], market_price)
        self.assertEqual(events[0]["trigger_price"], market_price)
        self.assertEqual(events[0]["fill_price"], market_price)

    def test_concurrent_price_updates_close_a_position_once(self):
        trade = self.manager.create_trade(self.signal, self.settings)
        self.manager.on_price("BTCUSDT", 100.0)

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(
                lambda _: self.manager.on_price("BTCUSDT", trade["take_profit"]),
                range(16),
            ))

        closed_events = [
            event for batch in results for event in batch
            if event["event"] == "TRADE_CLOSED"
        ]
        self.assertEqual(len(closed_events), 1)
        self.assertEqual(trade["close_reason"], "TAKE_PROFIT")

    def test_activation_take_profit_and_deduplication(self):
        trade = self.manager.create_trade(self.signal, self.settings)
        self.assertIsNotNone(trade)
        self.assertIsNone(self.manager.create_trade(self.signal, self.settings))
        events = self.manager.on_price("btcusdt", 100.0)
        self.assertEqual(events[0]["event"], "ENTRY_ACTIVATED")
        self.assertEqual(events[0]["fill_price"], 100.0)
        self.assertEqual(events[0]["trigger_price"], 100.0)
        self.assertIn("virtual Maker/Maker simulation", events[0]["price_source"])
        self.assertIn("BTCUSDT", self.manager.last_prices)

        close_events = self.manager.on_price("BTCUSDT", trade["take_profit"])
        self.assertEqual(trade["close_reason"], "TAKE_PROFIT")
        self.assertGreater(trade["gross_pnl_usdt"], 0)
        self.assertAlmostEqual(
            trade["profit_usdt"],
            trade["gross_pnl_usdt"] - trade["entry_fee_usdt"] - trade["exit_fee_usdt"],
        )
        self.assertEqual(close_events[0]["trigger_price"], trade["take_profit"])
        self.assertEqual(close_events[0]["fill_price"], trade["take_profit"])

    def test_statistics_include_unrealized_and_available_balance(self):
        trade = self.manager.create_trade(self.signal, self.settings)
        self.manager.on_price("BTCUSDT", 100.0)

        stats = self.manager.get_statistics()

        notional = trade["stake_usdt"] * trade["leverage"]
        fill = trade["entry_fill_price"]
        gross = (100.0 - fill) / fill * notional
        expected_unrealized = gross - trade["entry_fee_usdt"] - notional * 100.0 / fill * trade["applied_fee_rate"]
        self.assertAlmostEqual(stats["unrealized_profit_usdt"], expected_unrealized)
        self.assertAlmostEqual(stats["equity_usdt"], 100 + expected_unrealized)
        self.assertAlmostEqual(stats["available_balance_usdt"], 90 - trade["entry_fee_usdt"])

    def test_realized_pnl_and_cash_balance_include_each_fee_exactly_once(self):
        signal = dict(
            self.signal, entry_mode="market", reference_price=100.0,
            entry_price=100.0, stop_loss=99.0, take_profit=101.0,
        )
        trade = self.manager.create_trade(signal, self.settings)
        self.assertEqual(trade["status"], "open")
        opening_cash = self.manager.balance_usdt
        expected_entry_fee = trade["position_notional_usdt"] * trade["maker_fee_rate"]
        self.assertAlmostEqual(trade["entry_fee_usdt"], expected_entry_fee)
        self.assertAlmostEqual(100 - opening_cash, expected_entry_fee)

        self.manager.on_price("BTCUSDT", 100.25)
        event = self.manager.close_open_trades()[0]
        expected_net = (
            trade["gross_pnl_usdt"]
            - trade["entry_fee_usdt"]
            - trade["exit_fee_usdt"]
        )
        self.assertEqual(event["event"], "TRADE_CLOSED")
        self.assertAlmostEqual(trade["profit_usdt"], expected_net)
        self.assertAlmostEqual(
            self.manager.get_statistics()["total_profit_usdt"], expected_net
        )
        self.assertAlmostEqual(self.manager.balance_usdt, 100 + expected_net)

    def test_pair_unrealized_pnl_uses_notional_and_matches_global_fee_accounting(self):
        manager = CoreTradeManager(TradingSettings(
            stake_usdt=20, initial_balance_usdt=500, leverage=10,
        ))
        btc = dict(
            self.signal, symbol="BTCUSDT", wall_id="btc-pair-pnl",
            entry_mode="market", reference_price=100,
            entry_price=100, stop_loss=99, take_profit=101,
        )
        eth = dict(
            self.signal, symbol="ETHUSDT", wall_id="eth-pair-pnl",
            entry_mode="market", reference_price=200,
            entry_price=200, stop_loss=199, take_profit=201,
        )
        btc_trade = manager.create_trade(btc, PairSettings(
            "BTCUSDT", leverage=5, taker_fee_rate=0.0004,
        ))
        eth_trade = manager.create_trade(eth, PairSettings(
            "ETHUSDT", leverage=12, taker_fee_rate=0.0006,
        ))
        manager.on_price("BTCUSDT", 100.2, process_open=False)
        manager.on_price("ETHUSDT", 199.8, process_open=False)

        btc_pnl = manager.get_pair_statistics("BTCUSDT")["unrealized_profit_usdt"]
        eth_pnl = manager.get_pair_statistics("ETHUSDT")["unrealized_profit_usdt"]
        global_pnl = manager.get_statistics()["unrealized_profit_usdt"]

        self.assertAlmostEqual(
            btc_pnl, manager.trade_unrealized_pnl(btc_trade, 100.2)
        )
        self.assertAlmostEqual(
            eth_pnl, manager.trade_unrealized_pnl(eth_trade, 199.8)
        )
        self.assertAlmostEqual(btc_pnl + eth_pnl, global_pnl)
        self.assertNotEqual(btc_pnl, (100.2 - 100) / 100 * btc_trade["stake_usdt"])

    def test_pair_specific_leverage_is_frozen_and_margin_is_reserved_per_trade(self):
        btc = PairSettings("BTCUSDT", leverage=10)
        eth = PairSettings("ETHUSDT", leverage=20)
        eth_signal = dict(self.signal, symbol="ETHUSDT", wall_id="eth-wall")

        btc_trade = self.manager.create_trade(self.signal, btc, stake_usdt=30)
        eth_trade = self.manager.create_trade(eth_signal, eth, stake_usdt=30)

        self.assertEqual((btc_trade["leverage"], btc_trade["position_notional_usdt"]), (10, 300))
        self.assertEqual((eth_trade["leverage"], eth_trade["position_notional_usdt"]), (20, 600))
        # A pending trade reserves its stake only; it does not remove margin
        # from cash or reserve leveraged notional.
        self.assertEqual(self.manager.balance_usdt, 100)
        self.assertEqual(self.manager.get_statistics()["reserved_margin_usdt"], 0)
        self.assertEqual(self.manager.get_statistics()["available_balance_usdt"], 100)

        self.manager.on_price("BTCUSDT", 100)
        self.manager.on_price("ETHUSDT", 100)
        self.manager.trading_settings = TradingSettings(
            stake_usdt=30, initial_balance_usdt=100, max_open_trades=2, leverage=50
        )
        self.manager.on_price("BTCUSDT", btc_trade["take_profit"])
        self.manager.on_price("ETHUSDT", eth_trade["take_profit"])

        self.assertEqual(btc_trade["leverage"], 10)
        self.assertEqual(eth_trade["leverage"], 20)
        self.assertAlmostEqual(self.manager.balance_usdt, 100 + btc_trade["profit_usdt"] + eth_trade["profit_usdt"])
        self.assertEqual(self.manager.get_statistics()["reserved_margin_usdt"], 0)

    def test_new_virtual_trade_freezes_maker_maker_rates_for_long_short_and_exit_paths(self):
        for side, wall_side, reason, price in (
            ("long", "buy", "TAKE_PROFIT", 100.5),
            ("long", "buy", "STOP_LOSS", 99.5),
            ("short", "sell", "TAKE_PROFIT", 99.5),
            ("short", "sell", "STOP_LOSS", 100.5),
        ):
            manager = CoreTradeManager(TradingSettings(
                stake_usdt=30, initial_balance_usdt=500, max_open_trades=5,
                leverage=25,
            ))
            signal = {
                **self.signal,
                "side": side,
                "wall_side": wall_side,
                "wall_id": f"{side}-{reason}",
                "entry_mode": "market",
                "reference_price": 100.0,
                "entry_price": 100.0,
                "stop_loss": 99.0 if side == "long" else 101.0,
                "take_profit": 101.0 if side == "long" else 99.0,
            }
            pair = PairSettings("BTCUSDT", maker_fee_rate=0.00036, taker_fee_rate=0.001)
            trade = manager.create_trade(signal, pair)
            self.assertEqual(trade["commission_model"], "MAKER_MAKER_SIMULATION")
            self.assertEqual(trade["maker_fee_rate"], 0.00036)
            self.assertEqual(trade["taker_fee_rate"], 0.001)
            self.assertEqual(trade["applied_fee_rate"], 0.00036)
            self.assertEqual(trade["fee_rate"], 0.00036)
            self.assertAlmostEqual(trade["entry_fee_usdt"], 750 * 0.00036)
            event = manager.close_trade(trade["signal_id"], price)
            self.assertEqual(event["event"], "TRADE_CLOSED")
            expected_exit_fee = trade["position_notional_usdt"] * price / 100 * 0.00036
            self.assertAlmostEqual(trade["exit_fee_usdt"], expected_exit_fee)
            self.assertAlmostEqual(
                trade["profit_usdt"],
                trade["gross_pnl_usdt"] - trade["entry_fee_usdt"] - expected_exit_fee,
            )

    def test_legacy_open_trade_keeps_its_saved_fee_rate(self):
        legacy = {
            "signal_id": "legacy-fee", "strategy_id": "bounce", "symbol": "BTCUSDT",
            "side": "long", "wall_side": "buy", "status": "open", "wall_price": 100,
            "wall_size": 1000, "wall_ratio": 10, "stake_usdt": 30, "leverage": 25,
            "position_notional_usdt": 750, "entry_price": 100, "entry_fill_price": 100,
            "stop_loss": 99, "take_profit": 101, "fee_rate": 0.001,
            "taker_fee_rate": 0.001, "entry_fee_usdt": 0.75,
            "profit_usdt": 0, "accounting_version": 1,
        }
        manager = CoreTradeManager(TradingSettings(stake_usdt=30, initial_balance_usdt=100))
        manager.restore_state([legacy], 99.25)
        trade = manager.trades[0]
        self.assertEqual(trade["applied_fee_rate"], 0.001)
        self.assertEqual(trade["commission_model"], "LEGACY_SAVED_RATE")
        self.assertAlmostEqual(manager.trade_unrealized_pnl(trade, 100), -1.5)

    def test_legacy_trade_restore_preserves_historical_financial_values(self):
        legacy = {
            "signal_id": "legacy-1", "symbol": "BTCUSDT", "side": "long",
            "wall_side": "buy", "stake_usdt": 10, "status": "closed",
            "entry_price": 100, "exit_price": 101, "profit_usdt": 7.25,
            "profit_percent": 72.5, "leverage": None,
            "position_notional_usdt": None, "entry_fee_usdt": None,
            "exit_fee_usdt": None, "taker_fee_rate": 0.0,
        }
        self.manager.restore_state([legacy], 107.25)
        self.assertEqual(self.manager.trades[0]["profit_usdt"], 7.25)
        self.assertEqual(self.manager.get_statistics()["total_profit_usdt"], 7.25)

    def test_database_trade_hydration_restores_margin_and_fee_snapshot(self):
        from database.request import _trade_dict

        row = SimpleNamespace(
            signal_id="persisted", strategy_id="stable_wall_v1", symbol="BTCUSDT",
            side="long", wall_side="buy", status="open", wall_price=100,
            wall_size=50, wall_ratio=2, stake_usdt=30, leverage=25,
            position_notional_usdt=750, entry_fee_usdt=0.4125,
            exit_fee_usdt=None, gross_pnl_usdt=None, fee_rate=0.00055,
            accounting_version=1, entry_price=100, stop_loss=99,
            take_profit=102, exit_price=None, profit_usdt=0,
            profit_percent=0, close_reason=None, created_at=datetime.now(timezone.utc),
            opened_at=datetime.now(timezone.utc), closed_at=None,
        )
        hydrated = _trade_dict(row)
        restored = TradeManager(self.trading_settings)
        restored.restore_state([hydrated], 99.5875)

        self.assertEqual(restored.trades[0]["leverage"], 25)
        self.assertEqual(restored.trades[0]["position_notional_usdt"], 750)
        self.assertEqual(restored.trades[0]["entry_fee"], 0.4125)
        self.assertEqual(restored.trades[0]["taker_fee_rate"], 0.00055)

    def test_distinct_signals_are_not_limited_by_configured_open_trade_caps(self):
        second = dict(self.signal, wall_id="wall-2", wall_price=101.0)
        self.assertIsNotNone(self.manager.create_trade(self.signal, self.settings))
        self.assertIsNotNone(self.manager.create_trade(second, self.settings))

    def test_full_stop_closes_at_latest_price_and_cancels_pending(self):
        opened = self.manager.create_trade(self.signal, self.settings)
        self.manager.on_price("BTCUSDT", 100.0)
        pending_signal = dict(self.signal, wall_id="wall-2", wall_price=101.0)
        pair_settings = PairSettings("BTCUSDT", max_open_trades=2)
        pending = self.manager.create_trade(pending_signal, pair_settings)
        self.manager.last_prices["BTCUSDT"] = 99.9

        events = self.manager.close_all()
        self.assertEqual(opened["status"], "closed")
        self.assertEqual(opened["exit_price"], 99.9)
        self.assertEqual(opened["close_reason"], "ENGINE_STOPPED")
        self.assertEqual(pending["status"], "cancelled")
        self.assertEqual(len(events), 2)

    def test_invalid_explicit_stake_does_not_consume_signal(self):
        self.assertIsNone(
            self.manager.create_trade(self.signal, self.settings, stake_usdt=0)
        )
        self.assertIsNotNone(self.manager.create_trade(self.signal, self.settings))

    def test_non_finite_market_price_is_rejected(self):
        with self.assertRaises(ValueError):
            self.manager.on_price("BTCUSDT", nan)

    def test_signal_cannot_create_trade_for_a_different_pair(self):
        with self.assertRaisesRegex(ValueError, "does not match"):
            self.manager.create_trade(
                dict(self.signal, symbol="ETHUSDT"), self.settings
            )

    def test_restored_last_price_allows_full_stop_after_restart(self):
        self.manager.create_trade(self.signal, self.settings)
        self.manager.on_price("BTCUSDT", 100.0)
        self.manager.on_price("BTCUSDT", 100.01)

        restored = TradeManager(self.manager.trading_settings)
        restored.restore_state(
            self.manager.trades,
            self.manager.balance_usdt,
            self.manager.last_prices,
        )
        events = restored.close_all()

        self.assertEqual(restored.get_open_trades(), [])
        self.assertEqual(events[0]["trade"]["exit_price"], 100.01)
        self.assertEqual(events[0]["trade"]["close_reason"], "ENGINE_STOPPED")


class EngineControlTests(unittest.TestCase):
    def test_manual_pending_cancel_suppresses_same_wall_until_new_lifecycle(self):
        class FixedStrategy:
            strategy_id = "manual_cancel_test"

            def evaluate(self, context):
                return [TradeSignal(
                    strategy_id=self.strategy_id, symbol=context.symbol,
                    wall_side="buy", wall_price=100.0,
                    wall_id="BTCUSDT:buy:100:lifecycle-old", wall_size=1000.0,
                    wall_ratio=900.0, side="long", entry_price=100.05,
                    stop_loss=99.9, take_profit=100.2, entry_mode="cross",
                    entry_trigger_direction="cross_down",
                )]

        registry = StrategyRegistry()
        registry.register(FixedStrategy())
        settings = PairSettings("BTCUSDT")
        engine = TradingEngine(
            {"BTCUSDT": settings}, TradingSettings(initial_balance_usdt=100),
            strategy_registry=registry,
            enabled_strategies={"BTCUSDT": {"manual_cancel_test"}},
        )
        engine.start()
        engine.set_feed_status("BTCUSDT", "connected")
        wall = {"side": "buy", "price": 100.0, "notional": 1000.0, "age": 50.0}
        MarketFreshnessTests.record_market_health(engine, price_value=100.1)
        events = engine.on_market_update("BTCUSDT", 100.1, [wall], (), [wall])
        pending = next(event["trade"] for event in events if event["event"] == "TRADE_CREATED")
        run_immediate(engine.cancel_pending_trade(pending["signal_id"]))
        self.assertEqual(engine.on_market_update("BTCUSDT", 100.1, [wall], (), [wall]), [])
        self.assertEqual(engine.trade_manager.get_pending_trades(), [])
        self.assertTrue(engine.trade_manager.is_wall_manually_cancelled(
            "BTCUSDT", "manual_cancel_test", "BTCUSDT:buy:100:lifecycle-old"
        ))

    def test_single_manual_close_uses_trade_manager_and_persists_balance(self):
        repository = FakeRepository()
        engine = MarketFreshnessTests.make_engine()
        engine.repository = repository
        MarketFreshnessTests.record_market_health(engine, price_value=100.0)
        events = engine.on_market_update("BTCUSDT", 100.0, [], (), [])
        trade = next(event["trade"] for event in events if event["event"] == "TRADE_CREATED")
        MarketFreshnessTests.record_market_health(engine, price_value=100.25)
        engine.trade_manager.on_price("BTCUSDT", 100.25, process_open=False)
        event = run_immediate(engine.close_virtual_trade(trade["signal_id"]))

        self.assertEqual(event["event"], "TRADE_CLOSED")
        self.assertEqual(trade["status"], "closed")
        self.assertEqual(trade["close_reason"], "ADMIN_CLOSED")
        self.assertEqual(trade["exit_price"], 100.25)
        self.assertEqual(repository.saved_trade_batches[-1][0][0]["status"], "closed")

    def test_manual_open_level_edit_is_tick_validated_and_persisted(self):
        repository = FakeRepository()
        engine = MarketFreshnessTests.make_engine()
        engine.repository = repository
        engine.instrument_tick_sizes["BTCUSDT"] = 0.01
        MarketFreshnessTests.record_market_health(engine, price_value=100.0)
        events = engine.on_market_update("BTCUSDT", 100.0, [], (), [])
        trade = next(event["trade"] for event in events if event["event"] == "TRADE_CREATED")

        run_immediate(engine.update_open_trade_level(
            trade["signal_id"], field="tp", value=101.25
        ))
        self.assertEqual(trade["take_profit"], 101.25)
        self.assertEqual(trade["stop_loss"], 99.0)
        self.assertEqual(repository.saved_trade_batches[-1][0][0]["take_profit"], 101.25)
        with self.assertRaisesRegex(ValueError, "tickSize"):
            run_immediate(engine.update_open_trade_level(
                trade["signal_id"], field="tp", value=101.251
            ))
        with self.assertRaisesRegex(ValueError, "правильную сторону"):
            run_immediate(engine.update_open_trade_level(
                trade["signal_id"], field="tp", value=99.5
            ))

    def test_failed_manual_cancel_rolls_back_and_success_releases_reserve(self):
        repository = FakeRepository()
        settings = PairSettings("BTCUSDT")
        manager = TradeManager(TradingSettings(stake_usdt=10, initial_balance_usdt=100))
        trade = manager.create_trade({
            "signal_id": "manual-pending-1", "strategy_id": "bounce",
            "symbol": "BTCUSDT", "wall_side": "buy", "wall_price": 100,
            "wall_id": "BTCUSDT:buy:100:lifecycle-1", "wall_size": 1000,
            "wall_ratio": 900, "entry_price": 100.01, "stop_loss": 99.9,
            "take_profit": 100.2,
        }, settings)
        engine = TradingEngine({"BTCUSDT": settings}, manager.trading_settings,
                               trade_manager=manager, repository=repository)
        self.assertEqual(manager.get_statistics()["available_balance_usdt"], 100)
        repository.fail_trade_saves = True
        with self.assertRaisesRegex(RuntimeError, "восстановлено"):
            run_immediate(engine.cancel_pending_trade(trade["signal_id"]))
        trade = manager.find_trade("manual-pending-1")
        self.assertEqual(trade["status"], "pending")
        self.assertEqual(manager.get_statistics()["available_balance_usdt"], 100)

        repository.fail_trade_saves = False
        run_immediate(engine.cancel_pending_trade(trade["signal_id"]))
        self.assertEqual(trade["status"], "cancelled")
        self.assertEqual(trade["close_reason"], "MANUAL_CANCELLED")
        self.assertEqual(
            repository.saved_trade_batches[-1][0][0]["close_reason"],
            "MANUAL_CANCELLED",
        )
        self.assertEqual(manager.get_statistics()["available_balance_usdt"], 100)
        self.assertTrue(manager.is_wall_manually_cancelled(
            "BTCUSDT", "bounce", trade["wall_id"]
        ))
        restored = TradeManager(manager.trading_settings)
        restored.restore_state([trade], 100)
        self.assertTrue(restored.is_wall_manually_cancelled(
            "BTCUSDT", "bounce", trade["wall_id"]
        ))

    def test_manual_cancel_block_is_scoped_to_wall_lifecycle(self):
        manager = TradeManager(TradingSettings())
        cancelled = {
            "signal_id": "manual-cancel", "symbol": "BTCUSDT", "strategy_id": "bounce",
            "wall_id": "wall-lifecycle-old", "wall_side": "buy", "wall_price": 100,
            "status": "cancelled",
            "close_reason": "MANUAL_CANCELLED",
        }
        manager.restore_state([cancelled], 1000)
        self.assertTrue(manager.is_wall_manually_cancelled(
            "BTCUSDT", "bounce", "wall-lifecycle-old"
        ))
        self.assertFalse(manager.is_wall_manually_cancelled(
            "BTCUSDT", "bounce", "wall-lifecycle-next"
        ))

    def test_manual_wall_block_releases_only_after_complete_scan_sees_disappearance(self):
        repository = FakeRepository()
        settings = PairSettings("BTCUSDT")
        manager = TradeManager(TradingSettings(stake_usdt=10, initial_balance_usdt=100))
        trade = manager.create_trade({
            "signal_id": "manual-release-plan", "strategy_id": "bounce",
            "symbol": "BTCUSDT", "wall_side": "buy", "wall_price": 100,
            "wall_id": "wall-life", "wall_size": 1000, "wall_ratio": 900,
            "entry_price": 100.01, "stop_loss": 99.9, "take_profit": 100.2,
        }, settings)
        engine = TradingEngine({"BTCUSDT": settings}, manager.trading_settings,
                               trade_manager=manager, repository=repository)
        run_immediate(engine.cancel_pending_trade(trade["signal_id"]))
        self.assertTrue(manager.is_wall_manually_cancelled(
            "BTCUSDT", "bounce", "wall-life", "buy", 100
        ))

        run_immediate(engine.process_market_update("BTCUSDT", 100, [], wall_candidates=[]))
        self.assertEqual(repository.released_manual_blocks, [trade["signal_id"]])
        self.assertFalse(manager.is_wall_manually_cancelled(
            "BTCUSDT", "bounce", "wall-life", "buy", 100
        ))

    def test_pair_leverage_update_is_independent_and_only_changes_future_trades(self):
        repository = FakeRepository()
        btc = PairSettings("BTCUSDT", min_wall_ratio=999)
        eth = PairSettings("ETHUSDT", min_wall_ratio=777)
        engine = TradingEngine({"BTCUSDT": btc, "ETHUSDT": eth}, repository=repository)
        runtime = RuntimeController(engine)
        changed = run_immediate(_save_pair_leverage("BTCUSDT", "17.5", runtime))

        self.assertEqual(changed, 17.5)
        self.assertEqual(engine.pair_settings["BTCUSDT"].leverage, 17.5)
        self.assertEqual(engine.pair_settings["BTCUSDT"].min_wall_ratio, 999)
        self.assertEqual(engine.pair_settings["ETHUSDT"], eth)
        self.assertEqual(repository.saved_pair_settings[-1], engine.pair_settings["BTCUSDT"])
        self.assertIn("плечо 17.5x", pair_settings_text(runtime))

        btc_trade = engine.trade_manager.create_trade({
            "strategy_id": "bounce", "symbol": "BTCUSDT", "wall_side": "buy",
            "wall_price": 100, "wall_id": "btc-lev", "wall_size": 1000,
        }, engine.pair_settings["BTCUSDT"])
        eth_trade = engine.trade_manager.create_trade({
            "strategy_id": "bounce", "symbol": "ETHUSDT", "wall_side": "buy",
            "wall_price": 100, "wall_id": "eth-lev", "wall_size": 1000,
        }, engine.pair_settings["ETHUSDT"])
        self.assertEqual(btc_trade["leverage"], 17.5)
        self.assertEqual(eth_trade["leverage"], engine.trading_settings.leverage)

    def test_pair_strategy_mode_switch_is_immediate_and_pair_scoped(self):
        repository = FakeRepository()
        engine = TradingEngine(
            {"BTCUSDT": PairSettings("BTCUSDT"), "ETHUSDT": PairSettings("ETHUSDT")},
            enabled_strategies={
                "BTCUSDT": {"stable_wall_v1"},
                "ETHUSDT": {"stable_wall_v1"},
            },
            repository=repository,
        )
        runtime = RuntimeController(engine)
        run_immediate(runtime.update_enabled_strategies("BTCUSDT", {BOUNCE_STRATEGY_ID}, 7319))
        self.assertEqual(engine.get_enabled_strategy_ids("BTCUSDT"), {BOUNCE_STRATEGY_ID})
        self.assertEqual(engine.get_enabled_strategy_ids("ETHUSDT"), {"stable_wall_v1"})
        self.assertEqual(repository.saved_enabled_strategies[-1], ("BTCUSDT", {BOUNCE_STRATEGY_ID}))
        with self.assertRaisesRegex(ValueError, "only one"):
            run_immediate(runtime.update_enabled_strategies("BTCUSDT", {BOUNCE_STRATEGY_ID, BREAKOUT_STRATEGY_ID}))
        run_immediate(runtime.update_enabled_strategies("BTCUSDT", set(), 7319))
        self.assertEqual(engine.get_enabled_strategy_ids("BTCUSDT"), set())
        self.assertEqual(engine.get_enabled_strategy_ids("ETHUSDT"), {"stable_wall_v1"})

    def test_stale_strategy_button_is_rejected_and_repeat_is_idempotent(self):
        engine = TradingEngine(
            {"BTCUSDT": PairSettings("BTCUSDT")},
            enabled_strategies={"BTCUSDT": {"stable_wall_v1"}},
        )
        runtime = RuntimeController(engine)
        run_immediate(runtime.update_enabled_strategies(
            "BTCUSDT", {BOUNCE_STRATEGY_ID}, 7319, expected_version=0
        ))
        with self.assertRaisesRegex(RuntimeError, "selected pair strategy changed"):
            run_immediate(runtime.update_enabled_strategies(
                "BTCUSDT", {BREAKOUT_STRATEGY_ID}, 7319, expected_version=0
            ))
        self.assertEqual(engine.get_enabled_strategy_ids("BTCUSDT"), {BOUNCE_STRATEGY_ID})
        self.assertEqual(
            run_immediate(runtime.update_enabled_strategies(
                "BTCUSDT", {BOUNCE_STRATEGY_ID}, 7319, expected_version=0
            )),
            {BOUNCE_STRATEGY_ID},
        )
        self.assertEqual(engine.strategy_versions["BTCUSDT"], 1)

    def test_market_feed_only_runs_for_enabled_strategy_or_active_trade(self):
        settings = PairSettings("BTCUSDT")
        engine = TradingEngine(
            {"BTCUSDT": settings}, enabled_strategies={"BTCUSDT": set()}
        )
        self.assertFalse(engine.should_run_market_feed("BTCUSDT"))

        trade = engine.trade_manager.create_trade(
            {
                "strategy_id": "stable_wall_v1",
                "symbol": "BTCUSDT",
                "wall_side": "buy",
                "wall_price": 100.0,
                "wall_id": "wall-feed-keepalive",
                "wall_size": 1000.0,
                "wall_ratio": 900.0,
            },
            settings,
        )
        self.assertIsNotNone(trade)
        self.assertTrue(engine.should_run_market_feed("BTCUSDT"))

    def test_runtime_does_not_start_feed_when_all_pair_strategies_are_disabled(self):
        engine = TradingEngine(
            {"BTCUSDT": PairSettings("BTCUSDT")},
            enabled_strategies={"BTCUSDT": set()},
        )
        runtime = RuntimeController(engine)

        run_immediate(runtime.start())

        self.assertEqual(runtime._workers_by_symbol, {})
        self.assertEqual(engine.feed_status["BTCUSDT"], "stopped")

    def test_pair_strategy_configuration_is_persisted_before_runtime_change(self):
        repository = FakeRepository()
        engine = TradingEngine({"BTCUSDT": PairSettings("BTCUSDT")}, repository=repository)
        runtime = RuntimeController(engine)

        enabled = run_immediate(
            runtime.update_enabled_strategies("btcusdt", {BOUNCE_STRATEGY_ID})
        )

        self.assertEqual(enabled, {BOUNCE_STRATEGY_ID})
        self.assertEqual(engine.get_enabled_strategy_ids("BTCUSDT"), enabled)
        self.assertEqual(repository.saved_enabled_strategies, [("BTCUSDT", enabled)])

    def test_pair_strategy_database_failure_keeps_runtime_selection(self):
        repository = FakeRepository()
        repository.fail_strategy_saves = True
        engine = TradingEngine({"BTCUSDT": PairSettings("BTCUSDT")}, repository=repository)
        runtime = RuntimeController(engine)

        with self.assertRaises(RuntimeError):
            run_immediate(runtime.update_enabled_strategies("BTCUSDT", set()))

        self.assertEqual(engine.get_enabled_strategy_ids("BTCUSDT"), {"stable_wall_v1"})

    def test_disabling_strategy_cancels_its_pending_trade_and_keeps_open_trade(self):
        repository = FakeRepository()
        settings = PairSettings("BTCUSDT", max_open_trades=2)
        engine = TradingEngine({"BTCUSDT": settings}, repository=repository)
        manager = engine.trade_manager
        pending = manager.create_trade(
            {
                "strategy_id": "stable_wall_v1",
                "symbol": "BTCUSDT",
                "wall_side": "buy",
                "wall_price": 99.0,
                "wall_id": "wall-pending-strategy-off",
                "wall_size": 1000.0,
                "wall_ratio": 900.0,
            },
            settings,
        )
        opened = manager.create_trade(
            {
                "strategy_id": "stable_wall_v1",
                "symbol": "BTCUSDT",
                "wall_side": "sell",
                "wall_price": 101.0,
                "wall_id": "wall-open-strategy-off",
                "wall_size": 1000.0,
                "wall_ratio": 900.0,
            },
            settings,
        )
        manager.on_price("BTCUSDT", 101.0)
        runtime = RuntimeController(engine)

        run_immediate(runtime.update_enabled_strategies("BTCUSDT", set()))

        self.assertEqual(pending["status"], "cancelled")
        self.assertEqual(pending["close_reason"], "STRATEGY_DISABLED")
        self.assertEqual(opened["status"], "open")
        self.assertEqual(repository.saved_trade_batches[-1][0][0]["status"], "cancelled")

        engine.start()
        MarketFreshnessTests.record_market_health(
            engine, price_value=opened["take_profit"]
        )
        close_events = engine.on_market_update(
            "BTCUSDT", opened["take_profit"], [], price_updated=True
        )
        self.assertEqual(opened["status"], "closed")
        self.assertEqual(opened["close_reason"], "TAKE_PROFIT")
        self.assertIn("TRADE_CLOSED", [event["event"] for event in close_events])

    def test_add_pair_validates_persists_and_registers_symbol(self):
        repository = FakeRepository()
        engine = TradingEngine({}, repository=repository)
        runtime = RuntimeController(engine)

        settings = run_immediate(runtime.add_pair(" ethusdt "))

        self.assertEqual(settings.symbol, "ETHUSDT")
        self.assertIn("ETHUSDT", engine.pair_settings)
        self.assertEqual(repository.saved_pair_settings, [settings])
        self.assertEqual(engine.feed_status["ETHUSDT"], "stopped")

    def test_adding_pair_while_running_starts_its_worker(self):
        engine = TradingEngine({})
        engine.start()
        runtime = RuntimeController(engine)
        task = FakeTask()
        created_names = []

        def fake_create_task(coroutine, *, name):
            coroutine.close()
            created_names.append(name)
            return task

        with patch("app.runtime.asyncio.create_task", side_effect=fake_create_task):
            run_immediate(runtime.add_pair("ETHUSDT"))

        self.assertEqual(created_names, ["market-data-ethusdt"])
        self.assertIn(task, engine._workers)
        self.assertIs(runtime._workers_by_symbol["ETHUSDT"], task)

    def test_repeated_worker_sync_does_not_duplicate_market_subscription(self):
        engine = TradingEngine({"BTCUSDT": PairSettings("BTCUSDT")})
        engine.start()
        runtime = RuntimeController(engine)
        task = FakeTask()
        created = []

        def fake_create_task(coroutine, *, name):
            coroutine.close()
            created.append(name)
            return task

        with patch("app.runtime.asyncio.create_task", side_effect=fake_create_task):
            run_immediate(runtime._sync_workers())
            run_immediate(runtime._sync_workers())

        self.assertEqual(created, ["market-data-btcusdt"])


class MarketFreshnessTests(unittest.TestCase):
    def test_low_reported_age_and_synced_book_exposes_generation_failure(self):
        engine = self.make_engine()
        self.record_market_health(engine, received_age=0.25, generation=1)
        engine.mark_orderbook_unsynchronized("BTCUSDT", generation=2)
        # Model a synchronized new book generation before this connection has
        # emitted a fresh ticker update.
        engine.record_orderbook_market_data(
            "BTCUSDT", message_time_ms=int(time() * 1000),
            cts_ms=int(time() * 1000), sequence=2, update_id=2,
            received_monotonic=monotonic(), received_at=datetime.now(timezone.utc),
            delivery_lag_ms=0, synchronized=True, generation=2,
        )
        health = engine.get_market_data_health("BTCUSDT")
        self.assertLess(health["price_age_seconds"], 1.0)
        self.assertLess(health["book_age_seconds"], 1.0)
        self.assertTrue(health["book_synchronized"])
        self.assertIn("PRICE_GENERATION_MISMATCH", health["freshness_failures"])
        self.assertIn(
            "PRICE_GENERATION_MISMATCH",
            engine.get_entry_block_reason("BTCUSDT"),
        )

    def test_exchange_delivery_lag_baseline_recalibrates_after_local_clock_step(self):
        engine = SimpleNamespace(exchange_clock_offsets_ms={})
        with (
            patch("script.market_data.monotonic", side_effect=[100.0, 100.1]),
        ):
            first = _delivery_lag_ms(engine, "BTCUSDT", "tickers", 9000, 10000)
            after_clock_step = _delivery_lag_ms(
                engine, "BTCUSDT", "tickers", 10100, 13100
            )
        self.assertEqual(first, 0)
        self.assertEqual(after_clock_step, 0)
        self.assertEqual(engine.exchange_clock_offsets_ms[("BTCUSDT", "tickers")], 3000)

    @staticmethod
    def make_engine():
        class SignalStrategy:
            strategy_id = "freshness_test"

            def evaluate(self, context):
                return [TradeSignal(
                    strategy_id=self.strategy_id,
                    symbol=context.symbol,
                    wall_side="buy",
                    wall_price=99.0,
                    wall_id="freshness-wall",
                    wall_size=10000.0,
                    wall_ratio=100.0,
                    entry_mode="market",
                    entry_price=context.last_price,
                    side="long",
                    stop_loss=context.last_price * 0.99,
                    take_profit=context.last_price * 1.01,
                )]

        registry = StrategyRegistry()
        registry.register(SignalStrategy())
        engine = TradingEngine(
            {"BTCUSDT": PairSettings("BTCUSDT")},
            strategy_registry=registry,
            enabled_strategies={"BTCUSDT": {"freshness_test"}},
        )
        engine.start()
        engine.set_feed_status("BTCUSDT", "connected")
        return engine

    @staticmethod
    def record_market_health(
        engine, *, received_age=0.0, synchronized=True, price_value=100.0,
        source="Bybit V5 linear ticker lastPrice", generation=1,
    ):
        now_ms = int(time() * 1000)
        now_mono = monotonic()
        now = datetime.now(timezone.utc)
        engine.record_price_market_data(
            "BTCUSDT", event_time_ms=now_ms, message_time_ms=now_ms,
            sequence=1, trade_id="trade-1",
            received_monotonic=now_mono - received_age, received_at=now,
            delivery_lag_ms=0, price_value=price_value, source=source,
            connection_generation=generation,
        )
        engine.record_orderbook_market_data(
            "BTCUSDT", message_time_ms=now_ms, cts_ms=now_ms,
            sequence=1, update_id=1, received_monotonic=now_mono,
            received_at=now, delivery_lag_ms=0,
            synchronized=synchronized, generation=generation,
        )

    def test_entries_are_blocked_when_price_data_is_stale(self):
        engine = self.make_engine()
        self.record_market_health(engine, received_age=2.0)

        events = engine.on_market_update("BTCUSDT", 100.0, [], (), [])

        self.assertNotIn("TRADE_CREATED", [event["event"] for event in events])
        self.assertEqual(engine.trade_manager.trades, [])

    def test_stale_market_gap_disarms_pending_until_fresh_safe_side_and_recross(self):
        engine = TradingEngine(
            {"BTCUSDT": PairSettings(
                "BTCUSDT", min_wall_age_seconds=0, max_wall_distance_bps=1000,
            )},
            TradingSettings(initial_balance_usdt=100),
            enabled_strategies={"BTCUSDT": {BOUNCE_STRATEGY_ID}},
        )
        engine.instrument_tick_sizes["BTCUSDT"] = 0.01
        engine.start()
        engine.set_feed_status("BTCUSDT", "connected")
        wall = {
            "side": "buy", "price": 100.0, "first_seen_at": "stale-gap",
            "current_size": 10000.0, "initial_size": 10000.0,
            "wall_ratio": 900.0, "age": 45.0,
        }
        self.record_market_health(engine, price_value=100.1)
        engine.on_market_update("BTCUSDT", 100.1, [wall], (), [wall])
        pending, = engine.trade_manager.get_pending_trades()
        self.assertTrue(pending["entry_armed"])

        # Price crossed Entry while the stream was stale. It must not use the
        # old pre-gap price as evidence of a continuous crossing.
        self.record_market_health(engine, received_age=2.0, price_value=99.9)
        engine.on_market_update("BTCUSDT", 99.9, [wall], (), [wall])
        self.assertEqual(pending["status"], "pending")
        self.assertFalse(pending["entry_armed"])

        self.record_market_health(engine, price_value=100.1)
        engine.on_market_update("BTCUSDT", 100.1, [wall], (), [wall])
        self.assertEqual(pending["status"], "pending")
        self.assertTrue(pending["entry_armed"])

        self.record_market_health(engine, price_value=100.01)
        events = engine.on_market_update("BTCUSDT", 100.01, [wall], (), [wall])
        self.assertIn("ENTRY_ACTIVATED", [event["event"] for event in events])
        self.assertEqual(pending["status"], "open")

    def test_unsynchronized_book_blocks_new_entries(self):
        engine = self.make_engine()
        self.record_market_health(engine, synchronized=False)

        events = engine.on_market_update("BTCUSDT", 100.0, [])

        self.assertNotIn("TRADE_CREATED", [event["event"] for event in events])
        self.assertEqual(engine.get_entry_block_reason("BTCUSDT"), "Локальный стакан не синхронизирован.")

    def test_confirmed_ticker_crossing_precedes_same_tick_wall_cancellation(self):
        settings = PairSettings(
            "BTCUSDT", max_wall_distance_bps=200, min_wall_age_seconds=0,
        )
        engine = TradingEngine(
            {"BTCUSDT": settings},
            enabled_strategies={"BTCUSDT": {BOUNCE_STRATEGY_ID}},
        )
        engine.instrument_tick_sizes["BTCUSDT"] = 0.01
        engine.start()
        engine.set_feed_status("BTCUSDT", "connected")
        wall = {
            "side": "buy", "price": 100.0, "first_seen_at": "cross-priority",
            "current_size": 10000.0, "initial_size": 10000.0,
            "wall_ratio": 900.0, "age": 45.0,
        }

        self.record_market_health(engine, price_value=100.1)
        run_immediate(engine.process_market_update(
            "BTCUSDT", 100.1, [wall], (), [wall],
        ))
        pending, = engine.trade_manager.get_pending_trades()
        # One successful complete snapshot authorizes this existing plan for
        # the next ticker, not for the tick that created it.
        run_immediate(engine.process_market_update(
            "BTCUSDT", 100.1, [wall], (), [wall],
        ))
        self.assertTrue(pending["entry_armed"])

        self.record_market_health(engine, price_value=99.99)
        entry_events = run_immediate(
            engine.process_confirmed_entry_tick("BTCUSDT", 99.99)
        )
        self.assertIn("ENTRY_ACTIVATED", [event["event"] for event in entry_events])

        # The same tick's Scanner result no longer includes the wall. The
        # already-open position must survive that reconciliation.
        events = run_immediate(engine.process_market_update(
            "BTCUSDT", 99.99, [], (), [], entry_preprocessed=True,
        ))
        self.assertEqual(pending["status"], "open")
        self.assertNotIn("ENTRY_CANCELLED", [event["event"] for event in events])

    def test_orderbook_first_cancels_plan_before_later_ticker_crossing(self):
        settings = PairSettings(
            "BTCUSDT", max_wall_distance_bps=200, min_wall_age_seconds=0,
        )
        engine = TradingEngine(
            {"BTCUSDT": settings},
            enabled_strategies={"BTCUSDT": {BOUNCE_STRATEGY_ID}},
        )
        engine.instrument_tick_sizes["BTCUSDT"] = 0.01
        engine.start()
        engine.set_feed_status("BTCUSDT", "connected")
        wall = {
            "side": "buy", "price": 100.0, "first_seen_at": "book-first",
            "current_size": 10000.0, "initial_size": 10000.0,
            "wall_ratio": 900.0, "age": 45.0,
        }
        self.record_market_health(engine, price_value=100.1)
        run_immediate(engine.process_market_update(
            "BTCUSDT", 100.1, [wall], (), [wall],
        ))
        pending, = engine.trade_manager.get_pending_trades()
        run_immediate(engine.process_market_update(
            "BTCUSDT", 100.1, [wall], (), [wall],
        ))

        # A complete book update reports the wall gone before the ticker arrives.
        events = run_immediate(engine.process_market_update(
            "BTCUSDT", 100.1, [], (), [], price_updated=False,
        ))
        self.assertEqual(pending["status"], "cancelled")
        self.assertIn("ENTRY_CANCELLED", [event["event"] for event in events])
        self.record_market_health(engine, price_value=99.99)
        self.assertEqual(run_immediate(
            engine.process_confirmed_entry_tick("BTCUSDT", 99.99)
        ), [])
        self.assertEqual(pending["status"], "cancelled")

    def test_fresh_synchronized_tick_is_the_virtual_entry_price(self):
        engine = self.make_engine()
        self.record_market_health(engine, price_value=100.125)

        events = engine.on_market_update("BTCUSDT", 100.125, [], (), [])

        trade = next(event["trade"] for event in events if event["event"] == "TRADE_CREATED")
        self.assertEqual(trade["entry_price"], 100.125)
        self.assertEqual(engine.trade_manager.last_prices.get("BTCUSDT"), 100.125)

    def test_incomplete_scanner_snapshot_cannot_create_pending_with_live_metadata(self):
        engine = self.make_engine()
        self.record_market_health(engine, price_value=100.0)

        events = engine.on_market_update("BTCUSDT", 100.0, [], (), None)

        self.assertNotIn("TRADE_CREATED", [event["event"] for event in events])
        self.assertEqual(engine.trade_manager.get_pending_trades(), [])
        self.assertEqual(
            engine.wall_diagnostics["BTCUSDT"]["planner_reconciliation"]["status"],
            "skipped",
        )

    def test_new_pending_is_not_opened_by_entry_crossing_before_its_creation(self):
        engine = TradingEngine(
            {"BTCUSDT": PairSettings(
                "BTCUSDT", min_wall_age_seconds=0, max_wall_distance_bps=200,
            )},
            enabled_strategies={"BTCUSDT": {BOUNCE_STRATEGY_ID}},
        )
        engine.instrument_tick_sizes["BTCUSDT"] = 0.01
        engine.start()
        engine.set_feed_status("BTCUSDT", "connected")
        wall = {
            "side": "buy", "price": 100.0, "first_seen_at": "past-cross",
            "current_size": 10000.0, "initial_size": 10000.0,
            "wall_ratio": 900.0, "age": 45.0,
        }
        # The market crossed Entry before this Scanner snapshot admitted the
        # newly visible wall. BID remains below the current price, so the wall
        # is still a valid candidate even though Entry is now behind price.
        engine.trade_manager.last_prices["BTCUSDT"] = 100.03
        self.record_market_health(engine, price_value=100.01)
        self.assertEqual(
            run_immediate(engine.process_confirmed_entry_tick("BTCUSDT", 100.01)),
            [],
        )
        events = run_immediate(engine.process_market_update(
            "BTCUSDT", 100.01, [wall], (), [wall], entry_preprocessed=True,
        ))

        self.assertIn("TRADE_CREATED", [event["event"] for event in events])
        pending, = engine.trade_manager.get_pending_trades()
        self.assertEqual(pending["status"], "pending")
        self.assertFalse(pending["entry_armed"])

    def test_entry_crossing_rolls_back_if_postgres_does_not_save(self):
        repository = FakeRepository()
        settings = PairSettings(
            "BTCUSDT", min_wall_age_seconds=0, max_wall_distance_bps=200,
        )
        engine = TradingEngine(
            {"BTCUSDT": settings}, repository=repository,
            enabled_strategies={"BTCUSDT": {BOUNCE_STRATEGY_ID}},
        )
        engine.instrument_tick_sizes["BTCUSDT"] = 0.01
        engine.start()
        engine.set_feed_status("BTCUSDT", "connected")
        wall = {
            "side": "buy", "price": 100.0, "first_seen_at": "persist-cross",
            "current_size": 10000.0, "initial_size": 10000.0,
            "wall_ratio": 900.0, "age": 45.0,
        }
        self.record_market_health(engine, price_value=100.1)
        run_immediate(engine.process_market_update(
            "BTCUSDT", 100.1, [wall], (), [wall],
        ))
        run_immediate(engine.process_market_update(
            "BTCUSDT", 100.1, [wall], (), [wall],
        ))
        pending, = engine.trade_manager.get_pending_trades()
        balance_before = engine.trade_manager.balance_usdt

        repository.fail_trade_saves = True
        self.record_market_health(engine, price_value=99.99)
        self.assertEqual(run_immediate(
            engine.process_confirmed_entry_tick("BTCUSDT", 99.99)
        ), [])

        restored_pending, = engine.trade_manager.get_pending_trades()
        self.assertEqual(restored_pending["status"], "pending")
        self.assertEqual(engine.trade_manager.balance_usdt, balance_before)
        self.assertEqual(engine._entry_eligible_pending_ids.get("BTCUSDT"), None)
        self.assertEqual(
            engine.wall_diagnostics["BTCUSDT"]["planner_reconciliation"]["reason"],
            "PERSISTENCE_FAILED",
        )

    def test_public_trade_batch_uses_last_accepted_trade_and_records_sequence(self):
        cursor = TradeEventCursor()
        flow = TradeFlow(window_seconds=10)
        price = _update_flow(flow, {
            "ts": 1002,
            "data": [
                {"T": 1000, "seq": 7, "i": "t1", "p": "100.0", "v": "1", "S": "Buy"},
                {"T": 1001, "seq": 8, "i": "t2", "p": "100.1", "v": "2", "S": "Sell"},
            ],
        }, cursor)

        self.assertEqual(price, 100.1)
        self.assertEqual(cursor.latest.trade_id, "t2")
        self.assertEqual(cursor.latest.sequence, 8)
        self.assertEqual(flow.get_flow()["buy_volume_by_price_total"][100.0], 100.0)

    def test_ticker_snapshot_and_delta_keep_price_current_and_expose_feed_age(self):
        engine = self.make_engine()
        engine.mark_orderbook_unsynchronized("BTCUSDT", generation=1)
        cursor = TickerPriceCursor()
        ts = int(time() * 1000)
        first = _update_ticker_price(
            {"ts": ts, "cs": 11, "data": {"lastPrice": "100.25"}},
            cursor, engine, "BTCUSDT",
        )
        heartbeat = _update_ticker_price(
            {"ts": ts + 100, "cs": 12, "data": {"bid1Price": "100.2"}},
            cursor, engine, "BTCUSDT",
        )

        self.assertEqual(first, (100.25, True, True))
        self.assertEqual(heartbeat, (100.25, False, True))
        health = engine.get_market_data_health("BTCUSDT")
        self.assertTrue(health["price_fresh"])
        self.assertLess(health["price_age_seconds"], 0.2)

    def test_new_ticker_connection_does_not_inherit_old_price_without_snapshot(self):
        old_connection = TickerPriceCursor()
        old_connection.update({
            "ts": 1000, "cs": 10, "data": {"lastPrice": "100.0"}
        })

        new_connection = TickerPriceCursor()
        self.assertIsNone(new_connection.update({
            "ts": 1001, "cs": 11, "data": {"bid1Price": "99.9"}
        }))
        self.assertIsNone(new_connection.last_price)
        self.assertEqual(new_connection.update({
            "ts": 1002, "cs": 12, "data": {"lastPrice": "98.0"}
        })[0], 98.0)

    def test_open_position_continues_to_be_managed_while_entries_are_paused(self):
        engine = self.make_engine()
        self.record_market_health(engine)
        created = engine.on_market_update("BTCUSDT", 100.0, [], (), [])
        trade = next(event["trade"] for event in created if event["event"] == "TRADE_CREATED")
        engine.pause_new_entries()
        self.record_market_health(engine, price_value=98.0)

        events = engine.on_market_update("BTCUSDT", 98.0, [], (), [])

        self.assertEqual(trade["status"], "closed")
        self.assertEqual(trade["close_reason"], "STOP_LOSS")
        self.assertIn("TRADE_CLOSED", [event["event"] for event in events])

    def _open_position(self, engine):
        self.record_market_health(engine, price_value=100.0)
        events = engine.on_market_update("BTCUSDT", 100.0, [], (), [])
        return next(event["trade"] for event in events if event["event"] == "TRADE_CREATED")

    def test_open_trade_does_not_close_on_stale_price_past_stop(self):
        engine = self.make_engine()
        trade = self._open_position(engine)
        self.record_market_health(engine, received_age=2.0, price_value=98.0)

        engine.on_market_update("BTCUSDT", 98.0, [])

        self.assertEqual(trade["status"], "open")

    def test_open_trade_rejects_price_from_wrong_source(self):
        engine = self.make_engine()
        trade = self._open_position(engine)
        self.record_market_health(
            engine, price_value=98.0, source="Bybit mark price"
        )

        engine.on_market_update("BTCUSDT", 98.0, [])

        self.assertEqual(trade["status"], "open")

    def test_open_trade_rejects_mismatched_price_event(self):
        engine = self.make_engine()
        trade = self._open_position(engine)
        self.record_market_health(engine, price_value=100.0)

        engine.on_market_update("BTCUSDT", 98.0, [])

        self.assertEqual(trade["status"], "open")

    def test_open_trade_waits_for_current_generation_after_reconnect(self):
        engine = self.make_engine()
        trade = self._open_position(engine)
        engine.mark_orderbook_unsynchronized("BTCUSDT", generation=2)
        self.record_market_health(engine, price_value=98.0, generation=1)
        engine.mark_orderbook_unsynchronized("BTCUSDT", generation=2)

        engine.on_market_update("BTCUSDT", 98.0, [])
        self.assertEqual(trade["status"], "open")

        self.record_market_health(engine, price_value=98.0, generation=2)
        events = engine.on_market_update("BTCUSDT", 98.0, [])
        self.assertEqual(trade["status"], "closed")
        self.assertIn("TRADE_CLOSED", [event["event"] for event in events])

    def test_duplicate_price_event_does_not_close_trade_twice(self):
        engine = self.make_engine()
        trade = self._open_position(engine)
        target = trade["take_profit"]
        self.record_market_health(engine, price_value=target)
        first = engine.on_market_update("BTCUSDT", target, [])
        second = engine.on_market_update("BTCUSDT", target, [])

        self.assertEqual(trade["status"], "closed")
        self.assertEqual(sum(event["event"] == "TRADE_CLOSED" for event in first), 1)
        self.assertFalse(any(event["event"] == "TRADE_CLOSED" for event in second))

    def test_repeated_manager_event_and_closed_trade_are_idempotent(self):
        engine = self.make_engine()
        trade = self._open_position(engine)
        first = engine.trade_manager.on_price("BTCUSDT", trade["take_profit"])
        saved_exit_price = trade["exit_price"]
        second = engine.trade_manager.on_price("BTCUSDT", trade["take_profit"])

        self.assertEqual([event["event"] for event in first], ["TRADE_CLOSED"])
        self.assertEqual(second, [])
        self.assertEqual(trade["exit_price"], saved_exit_price)

    def test_other_symbol_tick_cannot_close_btc_position(self):
        engine = self.make_engine()
        trade = self._open_position(engine)

        engine.on_market_update("ETHUSDT", 98.0, [])

        self.assertEqual(trade["status"], "open")

    def test_admin_close_is_refused_without_fresh_price_metadata(self):
        engine = self.make_engine()
        created = engine.on_market_update("BTCUSDT", 100.0, [])
        trade = next(event["trade"] for event in created if event["event"] == "TRADE_CREATED")
        trade["status"] = "open"
        engine.trade_manager.last_prices["BTCUSDT"] = 100.0

        with self.assertRaisesRegex(RuntimeError, "stale market prices"):
            run_immediate(engine.close_open_trades())

        self.assertEqual(trade["status"], "open")

    def test_telegram_refresh_error_does_not_undo_persisted_close(self):
        class FakeState:
            async def clear(self):
                return None

        class FakeMessage:
            text = None
            reply_markup = None

            async def edit_text(self, text, reply_markup=None):
                raise RuntimeError("telegram edit failed")

        class FakeCallback:
            message = FakeMessage()

            async def answer(self, text, show_alert=False):
                self.answer_text = text

        engine = self.make_engine()
        self.record_market_health(engine)
        events = engine.on_market_update("BTCUSDT", 100.0, [], (), [])
        trade = next(event["trade"] for event in events if event["event"] == "TRADE_CREATED")
        callback = FakeCallback()

        run_immediate(close_open_trades_callback(
            callback, RuntimeController(engine), FakeState()
        ))

        self.assertEqual(trade["status"], "closed")
        self.assertIn("Закрыто открытых", callback.answer_text)

    def test_resume_ignores_pre_cutoff_trade_event(self):
        engine = self.make_engine()
        engine.pause_new_entries()
        engine.resume_new_entries()
        cutoff = engine.entry_event_cutoff_ms["BTCUSDT"]
        self.record_market_health(engine)
        engine._market_health("BTCUSDT")["price_event_time_ms"] = cutoff - 1

        events = engine.on_market_update("BTCUSDT", 100.0, [])

        self.assertNotIn("TRADE_CREATED", [event["event"] for event in events])

    def test_trade_cursor_deduplicates_and_rejects_out_of_order_events(self):
        cursor = TradeEventCursor()
        base = dict(
            price=100.0, message_time_ms=1000, sequence=10,
            received_monotonic=1.0,
            received_at=datetime.fromtimestamp(1, tz=timezone.utc),
            delivery_lag_ms=0.0,
        )
        first = PriceTick(event_time_ms=100, trade_id="first", **base)
        self.assertTrue(cursor.accept(first))
        self.assertFalse(cursor.accept(first))
        older = PriceTick(event_time_ms=99, trade_id="older", **base)
        self.assertFalse(cursor.accept(older))
        self.assertEqual(cursor.latest.trade_id, "first")

    def test_book_only_refresh_does_not_reuse_old_price_for_stop_management(self):
        engine = self.make_engine()
        self.record_market_health(engine)
        events = engine.on_market_update("BTCUSDT", 100.0, [], (), [])
        trade = next(event["trade"] for event in events if event["event"] == "TRADE_CREATED")
        trade["status"] = "open"
        engine._market_health("BTCUSDT")["price_received_monotonic"] = monotonic() - 2.0
        engine.record_orderbook_market_data(
            "BTCUSDT", message_time_ms=int(time() * 1000), cts_ms=int(time() * 1000),
            sequence=2, update_id=2, received_monotonic=monotonic(),
            received_at=datetime.now(timezone.utc), delivery_lag_ms=0,
            synchronized=True, generation=1,
        )

        events = engine.on_market_update("BTCUSDT", 90.0, [], price_updated=False)

        self.assertNotIn("TRADE_CLOSED", [event["event"] for event in events])
        self.assertEqual(trade["status"], "open")


class EngineControlContinuationTests(unittest.TestCase):
    def test_restart_preserves_disarmed_pending_plan_and_open_trade_and_balance(self):
        settings = PairSettings("BTCUSDT", max_open_trades=3)
        engine = TradingEngine({"BTCUSDT": settings})
        pending = engine.trade_manager.create_trade({
            "strategy_id": "stable_wall_v1", "symbol": "BTCUSDT",
            "wall_side": "buy", "wall_price": 100.0,
            "wall_id": "restart-pending", "wall_size": 1000.0,
            "wall_ratio": 900.0,
        }, settings)
        opened = engine.trade_manager.create_trade({
            "strategy_id": "stable_wall_v1", "symbol": "BTCUSDT",
            "wall_side": "sell", "wall_price": 101.0,
            "wall_id": "restart-open", "wall_size": 1000.0,
            "wall_ratio": 900.0,
        }, settings)
        engine.trade_manager.on_price("BTCUSDT", 101.0)
        balance = engine.trade_manager.balance_usdt
        engine.mode = EngineMode.ENTRIES_PAUSED
        runtime = RuntimeController(engine)

        def fake_create_task(coroutine, *, name):
            coroutine.close()
            return FakeTask()

        with patch("app.runtime.asyncio.create_task", side_effect=fake_create_task):
            run_immediate(runtime.start(preserve_mode=True))

        self.assertEqual(pending["status"], "pending")
        self.assertFalse(pending["entry_armed"])
        self.assertEqual(opened["status"], "open")
        self.assertEqual(engine.trade_manager.balance_usdt, balance)
        self.assertEqual(engine.mode, EngineMode.ENTRIES_PAUSED)

    def test_add_pair_rejects_invalid_and_duplicate_symbols(self):
        runtime = RuntimeController(TradingEngine({}))
        with self.assertRaises(ValueError):
            run_immediate(runtime.add_pair("BTC/USD"))

        run_immediate(runtime.add_pair("BTCUSDT"))
        with self.assertRaises(ValueError):
            run_immediate(runtime.add_pair("BTCUSDT"))

    def test_pause_keeps_open_trade_managed_and_full_stop_finishes(self):
        settings = TradingSettings(
            stake_usdt=10, initial_balance_usdt=100, max_open_trades=2
        )
        manager = TradeManager(settings)
        engine = TradingEngine(
            {"BTCUSDT": PairSettings("BTCUSDT", max_open_trades=2)},
            trading_settings=settings,
            trade_manager=manager,
        )
        engine.start()
        signal = {
            "strategy_id": "stable_wall_v1",
            "symbol": "BTCUSDT",
            "wall_side": "buy",
            "wall_price": 100.0,
            "wall_id": "wall-stop-test",
            "wall_size": 1000.0,
            "wall_ratio": 900.0,
        }
        trade = manager.create_trade(signal, engine.pair_settings["BTCUSDT"])
        manager.on_price("BTCUSDT", 100.1)
        manager.on_price("BTCUSDT", 100.0)
        MarketFreshnessTests.record_market_health(engine, price_value=100.0)
        engine.on_market_update("BTCUSDT", 100.0, [])
        pending_signal = dict(signal, wall_id="wall-pending-during-pause", wall_price=101.0)
        pending = manager.create_trade(
            pending_signal, engine.pair_settings["BTCUSDT"]
        )
        runtime = RuntimeController(engine)
        run_immediate(runtime.pause_new_entries())

        self.assertEqual(trade["status"], "open")
        self.assertEqual(pending["status"], "cancelled")
        self.assertEqual(engine.mode, EngineMode.ENTRIES_PAUSED)
        MarketFreshnessTests.record_market_health(engine, price_value=100.01)
        engine.on_market_update("BTCUSDT", 100.01, [])

        stop = engine.stop_all()
        with patch("script.engine.asyncio.current_task", return_value=None):
            with self.assertRaises(StopIteration) as completed:
                stop.send(None)

        self.assertEqual(completed.exception.value[0]["event"], "TRADE_CLOSED")
        self.assertEqual(engine.mode, EngineMode.STOPPED)
        self.assertEqual(trade["exit_price"], 100.01)
        self.assertEqual(trade["close_reason"], "ENGINE_STOPPED")

    def test_graceful_shutdown_preserves_open_virtual_trade_and_running_mode(self):
        settings = TradingSettings(
            stake_usdt=10, initial_balance_usdt=100, max_open_trades=2
        )
        manager = TradeManager(settings)
        engine = TradingEngine(
            {"BTCUSDT": PairSettings("BTCUSDT", max_open_trades=2)},
            trading_settings=settings,
            trade_manager=manager,
        )
        engine.start()
        signal = {
            "strategy_id": "stable_wall_v1",
            "symbol": "BTCUSDT",
            "wall_side": "buy",
            "wall_price": 100.0,
            "wall_id": "wall-shutdown-test",
            "wall_size": 1000.0,
            "wall_ratio": 900.0,
        }
        trade = manager.create_trade(signal, engine.pair_settings["BTCUSDT"])
        manager.on_price("BTCUSDT", 100.1)
        manager.on_price("BTCUSDT", 100.0)
        MarketFreshnessTests.record_market_health(engine, price_value=100.0)
        engine.on_market_update("BTCUSDT", 100.0, [])
        runtime = RuntimeController(engine)

        run_immediate(runtime.shutdown())

        restored_trade = engine.trade_manager.trades[0]
        self.assertEqual(restored_trade["status"], "open")
        self.assertIsNone(restored_trade["exit_price"])
        self.assertEqual(engine.mode, EngineMode.RUNNING)
        self.assertEqual(engine.feed_status["BTCUSDT"], "stopped")

    def test_graceful_shutdown_preserves_entries_paused_mode(self):
        engine = TradingEngine({"BTCUSDT": PairSettings("BTCUSDT")})
        engine.mode = EngineMode.ENTRIES_PAUSED
        engine.feed_status["BTCUSDT"] = "connected"
        runtime = RuntimeController(engine)

        run_immediate(runtime.shutdown())

        self.assertEqual(engine.mode, EngineMode.ENTRIES_PAUSED)
        self.assertEqual(engine.feed_status["BTCUSDT"], "stopped")

    def test_close_open_trades_keeps_engine_running_and_pending_entry(self):
        settings = TradingSettings(
            stake_usdt=10, initial_balance_usdt=100, max_open_trades=3
        )
        manager = TradeManager(settings)
        pair = PairSettings("BTCUSDT", max_open_trades=3)
        engine = TradingEngine(
            {"BTCUSDT": pair}, trading_settings=settings, trade_manager=manager
        )
        engine.start()
        open_trade = manager.create_trade(
            {
                "strategy_id": "stable_wall_v1",
                "symbol": "BTCUSDT",
                "wall_side": "buy",
                "wall_price": 100.0,
                "wall_id": "wall-admin-close-open",
                "wall_size": 1000.0,
                "wall_ratio": 900.0,
            },
            pair,
        )
        manager.on_price("BTCUSDT", 100.0)
        pending_trade = manager.create_trade(
            {
                "strategy_id": "stable_wall_v1",
                "symbol": "BTCUSDT",
                "wall_side": "buy",
                "wall_price": 99.0,
                "wall_id": "wall-admin-close-pending",
                "wall_size": 1000.0,
                "wall_ratio": 900.0,
            },
            pair,
        )
        runtime = RuntimeController(engine)

        MarketFreshnessTests.record_market_health(engine)

        events = run_immediate(runtime.close_open_trades())

        self.assertEqual([event["event"] for event in events], ["TRADE_CLOSED"])
        self.assertEqual(open_trade["status"], "closed")
        self.assertEqual(open_trade["close_reason"], "ADMIN_CLOSED")
        self.assertEqual(pending_trade["status"], "pending")
        self.assertEqual(engine.mode, EngineMode.RUNNING)

    def test_close_open_trade_database_failure_restores_position(self):
        settings = TradingSettings(
            stake_usdt=10, initial_balance_usdt=100, max_open_trades=2
        )
        repository = FakeRepository()
        pair = PairSettings("BTCUSDT", max_open_trades=2)
        engine = TradingEngine(
            {"BTCUSDT": pair}, trading_settings=settings, repository=repository
        )
        engine.start()
        trade = engine.trade_manager.create_trade(
            {
                "strategy_id": "stable_wall_v1",
                "symbol": "BTCUSDT",
                "wall_side": "buy",
                "wall_price": 100.0,
                "wall_id": "wall-admin-close-db-failure",
                "wall_size": 1000.0,
                "wall_ratio": 900.0,
            },
            pair,
        )
        engine.trade_manager.on_price("BTCUSDT", 100.1)
        engine.trade_manager.on_price("BTCUSDT", 100.0)
        MarketFreshnessTests.record_market_health(engine)
        run_immediate(engine.process_market_update("BTCUSDT", 100.0, []))
        repository.fail_trade_saves = True
        runtime = RuntimeController(engine)
        balance_before = engine.trade_manager.balance_usdt

        with self.assertRaisesRegex(RuntimeError, "persist closed"):
            run_immediate(runtime.close_open_trades())

        restored_trade = engine.trade_manager.trades[0]
        self.assertEqual(restored_trade["status"], "open")
        self.assertIsNone(restored_trade["exit_price"])
        self.assertEqual(engine.trade_manager.balance_usdt, balance_before)
        self.assertEqual(engine.get_status()["persistence_pending"], 0)
        self.assertEqual(repository.saved_trade_batches, [])

    def test_disabled_pair_cancels_pending_entry_before_price_activation(self):
        settings = PairSettings("BTCUSDT", enabled=False)
        engine = TradingEngine({"BTCUSDT": settings})
        engine.start()
        signal = {
            "strategy_id": "stable_wall_v1",
            "symbol": "BTCUSDT",
            "wall_side": "buy",
            "wall_price": 100.0,
            "wall_id": "wall-disabled-pair",
            "wall_size": 1000.0,
            "wall_ratio": 900.0,
        }
        trade = engine.trade_manager.create_trade(signal, settings)

        events = engine.on_market_update("BTCUSDT", 99.0, [])

        self.assertEqual(trade["status"], "cancelled")
        self.assertEqual(trade["close_reason"], "PAIR_DISABLED")
        self.assertEqual([event["event"] for event in events], ["ENTRY_CANCELLED"])

    def test_engine_pause_guard_cancels_any_pending_before_activation(self):
        settings = PairSettings("BTCUSDT")
        engine = TradingEngine({"BTCUSDT": settings})
        engine.start()
        signal = {
            "strategy_id": "stable_wall_v1",
            "symbol": "BTCUSDT",
            "wall_side": "buy",
            "wall_price": 100.0,
            "wall_id": "wall-paused-engine-guard",
            "wall_size": 1000.0,
            "wall_ratio": 900.0,
        }
        trade = engine.trade_manager.create_trade(signal, settings)
        engine.pause_new_entries()

        events = engine.on_market_update("BTCUSDT", 99.0, [])

        self.assertEqual(trade["status"], "cancelled")
        self.assertEqual(trade["close_reason"], "ENTRIES_PAUSED")
        self.assertEqual([event["event"] for event in events], ["ENTRY_CANCELLED"])

    def test_failed_pause_persistence_restores_running_state_and_pending_entry(self):
        settings = PairSettings("BTCUSDT")
        repository = FakeRepository()
        repository.fail_trade_saves = True
        engine = TradingEngine({"BTCUSDT": settings}, repository=repository)
        engine.start()
        signal = {
            "strategy_id": "stable_wall_v1",
            "symbol": "BTCUSDT",
            "wall_side": "buy",
            "wall_price": 100.0,
            "wall_id": "wall-pause-persist-fail",
            "wall_size": 1000.0,
            "wall_ratio": 900.0,
        }
        trade = engine.trade_manager.create_trade(signal, settings)
        engine._pending_persistence[trade["signal_id"]] = trade
        runtime = RuntimeController(engine)

        with self.assertRaises(RuntimeError):
            run_immediate(runtime.pause_new_entries())

        self.assertEqual(engine.mode, EngineMode.RUNNING)
        self.assertEqual(engine.trade_manager.trades[0]["status"], "pending")
        self.assertEqual(engine.get_status()["persistence_pending"], 1)
        self.assertIs(
            engine._pending_persistence[trade["signal_id"]],
            engine.trade_manager.trades[0],
        )

    def test_runtime_start_cancels_restored_entries_for_disabled_pairs(self):
        settings = PairSettings("BTCUSDT", enabled=False)
        engine = TradingEngine({"BTCUSDT": settings})
        signal = {
            "strategy_id": "stable_wall_v1",
            "symbol": "BTCUSDT",
            "wall_side": "buy",
            "wall_price": 100.0,
            "wall_id": "wall-disabled-restart",
            "wall_size": 1000.0,
            "wall_ratio": 900.0,
        }
        trade = engine.trade_manager.create_trade(signal, settings)
        runtime = RuntimeController(engine)

        run_immediate(runtime.start())

        self.assertEqual(engine.mode, EngineMode.RUNNING)
        self.assertEqual(trade["status"], "cancelled")
        self.assertEqual(trade["close_reason"], "PAIR_DISABLED")
        self.assertEqual(runtime._workers_by_symbol, {})

    def test_failed_persistence_does_not_apply_disabled_pair_cancellation(self):
        settings = PairSettings("BTCUSDT", enabled=False)
        repository = FakeRepository()
        repository.fail_trade_saves = True
        engine = TradingEngine({"BTCUSDT": settings}, repository=repository)
        signal = {
            "strategy_id": "stable_wall_v1",
            "symbol": "BTCUSDT",
            "wall_side": "buy",
            "wall_price": 100.0,
            "wall_id": "wall-disabled-persist-fail",
            "wall_size": 1000.0,
            "wall_ratio": 900.0,
        }
        trade = engine.trade_manager.create_trade(signal, settings)
        runtime = RuntimeController(engine)

        with self.assertRaises(RuntimeError):
            run_immediate(runtime.start())

        self.assertEqual(engine.mode, EngineMode.STOPPED)
        self.assertEqual(engine.trade_manager.trades[0]["status"], "pending")
        self.assertEqual(engine.get_status()["persistence_pending"], 0)

    def test_failed_trade_save_keeps_pair_enabled_and_pending_trade(self):
        settings = PairSettings("BTCUSDT")
        repository = FakeRepository()
        repository.fail_trade_saves = True
        engine = TradingEngine({"BTCUSDT": settings}, repository=repository)
        signal = {
            "strategy_id": "stable_wall_v1",
            "symbol": "BTCUSDT",
            "wall_side": "buy",
            "wall_price": 100.0,
            "wall_id": "wall-disable-write-fail",
            "wall_size": 1000.0,
            "wall_ratio": 900.0,
        }
        engine.trade_manager.create_trade(signal, settings)
        runtime = RuntimeController(engine)

        with self.assertRaises(RuntimeError):
            run_immediate(
                runtime.update_pair_settings(replace(settings, enabled=False))
            )

        self.assertTrue(engine.pair_settings["BTCUSDT"].enabled)
        self.assertEqual(engine.trade_manager.trades[0]["status"], "pending")

    def test_failed_settings_save_keeps_runtime_setting_unchanged(self):
        settings = PairSettings("BTCUSDT")
        repository = FakeRepository()
        repository.fail_pair_saves = True
        engine = TradingEngine({"BTCUSDT": settings}, repository=repository)
        signal = {
            "strategy_id": "stable_wall_v1",
            "symbol": "BTCUSDT",
            "wall_side": "buy",
            "wall_price": 100.0,
            "wall_id": "wall-settings-write-fail",
            "wall_size": 1000.0,
            "wall_ratio": 900.0,
        }
        engine.trade_manager.create_trade(signal, settings)
        runtime = RuntimeController(engine)

        with self.assertRaises(RuntimeError):
            run_immediate(
                runtime.update_pair_settings(replace(settings, enabled=False))
            )

        self.assertTrue(engine.pair_settings["BTCUSDT"].enabled)
        self.assertEqual(engine.trade_manager.trades[0]["status"], "pending")
        self.assertEqual(len(repository.saved_trade_batches), 0)
        self.assertEqual(repository.saved_pair_settings, [])

    def test_trade_state_retries_after_repository_failure(self):
        repository = FakeRepository()
        repository.fail_trade_saves = True
        engine = TradingEngine(
            {"BTCUSDT": PairSettings("BTCUSDT")}, repository=repository
        )
        signal = {
            "strategy_id": "stable_wall_v1",
            "symbol": "BTCUSDT",
            "wall_side": "buy",
            "wall_price": 100.0,
            "wall_id": "wall-persist-test",
            "wall_size": 1000.0,
            "wall_ratio": 900.0,
        }
        trade = engine.trade_manager.create_trade(
            signal, engine.pair_settings["BTCUSDT"]
        )
        event = {"event": "TRADE_CREATED", "trade": trade}

        self.assertFalse(run_immediate(engine.persist_events([event])))
        self.assertEqual(engine.get_status()["persistence_pending"], 1)

        repository.fail_trade_saves = False
        self.assertTrue(run_immediate(engine.persist_events([])))
        self.assertEqual(engine.get_status()["persistence_pending"], 0)
        self.assertEqual(len(repository.saved_trade_batches), 1)

    def test_process_market_update_serializes_market_and_price_persistence(self):
        repository = FakeRepository()
        engine = TradingEngine(
            {"BTCUSDT": PairSettings("BTCUSDT")}, repository=repository
        )
        engine.start()
        wall = {
            "side": "buy",
            "price": 100.0,
            "current_size": 1000.0,
            "wall_ratio": 900.0,
            "first_seen_at": "test-wall-instance",
        }

        events = run_immediate(
            engine.process_market_update("BTCUSDT", 100.0, [wall], (), [wall])
        )

        self.assertEqual(events, [])
        self.assertEqual(repository.saved_trade_batches, [])
        self.assertEqual(repository.saved_prices, [("BTCUSDT", 100.0)])

    def test_market_price_persistence_is_rate_limited_per_symbol(self):
        repository = FakeRepository()
        engine = TradingEngine(
            {"BTCUSDT": PairSettings("BTCUSDT")}, repository=repository
        )
        with patch("script.engine.monotonic", side_effect=[100, 104, 105]):
            run_immediate(engine.persist_market_price("BTCUSDT", 100.0))
            run_immediate(engine.persist_market_price("BTCUSDT", 101.0))
            run_immediate(engine.persist_market_price("BTCUSDT", 102.0))

        self.assertEqual(
            repository.saved_prices, [("BTCUSDT", 100.0), ("BTCUSDT", 102.0)]
        )

    def test_cancelled_cleanup_prunes_only_repository_confirmed_cancelled_rows(self):
        manager = CoreTradeManager(TradingSettings(initial_balance_usdt=100))
        manager.trades = [
            {"signal_id": "old-cancelled", "status": "cancelled", "stake_usdt": 10},
            {"signal_id": "active-pending", "status": "pending", "stake_usdt": 10},
            {"signal_id": "active-open", "status": "open", "stake_usdt": 10},
            {"signal_id": "closed", "status": "closed", "stake_usdt": 10, "profit_usdt": 0},
        ]
        manager._used_signal_ids = {trade["signal_id"] for trade in manager.trades}

        class CleanupRepository:
            async def cleanup_cancelled_trades(self, *, exclude_signal_ids):
                self.excluded = set(exclude_signal_ids)
                now = datetime.now(timezone.utc)
                return {
                    "deleted_signal_ids": ["old-cancelled"],
                    "cancelled_trade_count": 7,
                    "last_cleanup_at": now,
                    "deleted_count": 1,
                }

        repository = CleanupRepository()
        engine = TradingEngine({}, trade_manager=manager, repository=repository)
        removed = run_immediate(engine.cleanup_cancelled_trades())

        self.assertEqual(removed, 1)
        self.assertEqual(repository.excluded, set())
        self.assertEqual(engine.cancelled_trade_count, 7)
        self.assertEqual(engine.last_cancelled_cleanup_deleted, 1)
        self.assertEqual(
            {trade["signal_id"] for trade in manager.trades},
            {"active-pending", "active-open", "closed"},
        )
        self.assertEqual(
            sum(
                trade["stake_usdt"] for trade in manager.trades
                if trade["status"] in {"pending", "open"}
            ),
            20,
        )
        self.assertNotIn("old-cancelled", manager._used_signal_ids)

    def test_cancelled_cleanup_database_failure_keeps_memory_unchanged(self):
        manager = CoreTradeManager(TradingSettings(initial_balance_usdt=100))
        manager.trades = [{
            "signal_id": "old-cancelled", "status": "cancelled", "stake_usdt": 10,
        }]

        class FailingRepository:
            async def cleanup_cancelled_trades(self, *, exclude_signal_ids):
                raise OSError("database unavailable")

        engine = TradingEngine({}, trade_manager=manager, repository=FailingRepository())
        with self.assertRaisesRegex(OSError, "database unavailable"):
            run_immediate(engine.cleanup_cancelled_trades())
        self.assertEqual(len(manager.trades), 1)

    def test_cleanup_service_continues_to_a_later_cycle_after_database_failure(self):
        class Repository:
            pass

        engine = TradingEngine({}, repository=Repository())
        engine.cleanup_cancelled_trades = AsyncMock(
            side_effect=[OSError("database unavailable"), 0]
        )
        runtime = RuntimeController(engine)
        with self.assertLogs("app.runtime", level="ERROR"):
            first_result = run_immediate(runtime._cleanup_cancelled_once())
        second_result = run_immediate(runtime._cleanup_cancelled_once())

        self.assertIsNone(first_result)
        self.assertEqual(second_result, 0)
        self.assertEqual(engine.cleanup_cancelled_trades.await_count, 2)


if __name__ == "__main__":
    unittest.main()
