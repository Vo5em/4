import os
import json
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from datetime import timedelta
from math import nan
from unittest.mock import AsyncMock, patch
from aiogram.types import Chat, Message, User

os.environ.setdefault("ADMIN_USER_ID", "4242")

from script.engine import EngineMode, TradingEngine
from script.formulas import (
    calculate_entry_price,
    calculate_stop_loss_price,
    calculate_x2_price,
)
from script.market_data import (
    BYBIT_REST_URL,
    OrderbookDeltaBuffer,
    SnapshotRetryRequired,
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
from script.settings import PairSettings, TradingSettings
from script.strategies import (
    BOUNCE_STRATEGY_ID,
    BREAKOUT_STRATEGY_ID,
    BounceStrategy,
    BreakoutStrategy,
    StrategyContext,
    StrategyRegistry,
    TradeSignal,
)
from script.trade_manager import TradeManager
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
    edit_message_if_changed,
    set_pair_strategy,
)
from run import _symbols_from_environment, build_runtime


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
        self.fail_strategy_saves = False

    async def save_trades(self, trades, balance_usdt, engine_mode=None):
        if self.fail_trade_saves:
            raise OSError("temporary database failure")
        self.saved_trade_batches.append((trades, balance_usdt, engine_mode))

    async def save_market_price(self, symbol, price):
        self.saved_prices.append((symbol, price))

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

    def test_bounce_support_requires_approach_then_reversal_and_has_1_5r(self):
        strategy = BounceStrategy()
        wall = self.wall()
        self.assertEqual(strategy.evaluate(self.context(strategy, 100.01, 1, [wall])), [])
        signal, = strategy.evaluate(self.context(strategy, 100.02, 2, [wall]))
        self.assertEqual(signal.side, "long")
        self.assertAlmostEqual(signal.stop_loss, 99.98)
        self.assertAlmostEqual(signal.take_profit - signal.entry_price,
                               1.5 * (signal.entry_price - signal.stop_loss))
        repeated, = strategy.evaluate(self.context(strategy, 100.03, 3, [wall]))
        self.assertEqual(repeated.wall_id, signal.wall_id)

    def test_bounce_resistance_is_short_and_wall_loss_clears_state(self):
        strategy = BounceStrategy()
        wall = self.wall("sell", identity="old")
        self.assertEqual(strategy.evaluate(self.context(strategy, 99.99, 1, [wall])), [])
        new_wall = self.wall("sell", identity="new")
        self.assertEqual(strategy.evaluate(self.context(strategy, 99.99, 2, [new_wall])), [])
        signal, = strategy.evaluate(self.context(strategy, 99.97, 3, [new_wall]))
        self.assertEqual(signal.side, "short")
        self.assertGreater(signal.stop_loss, signal.entry_price)
        self.assertLess(signal.take_profit, signal.entry_price)

    def test_bounce_requires_full_stability_and_skips_bad_reward_obstacle(self):
        strategy = BounceStrategy()
        unstable = self.wall(age=39)
        self.assertEqual(strategy.evaluate(self.context(strategy, 100.01, 1, [unstable])), [])
        wall = self.wall()
        strategy.evaluate(self.context(strategy, 100.01, 2, [wall]))
        obstacle = self.wall("sell", 100.3, "obstacle")
        self.assertEqual(
            strategy.evaluate(self.context(strategy, 100.2, 3, [wall, obstacle])), []
        )

    def test_nearest_stable_support_is_the_only_bounce_signal_candidate(self):
        strategy = BounceStrategy()
        nearest = self.wall("buy", 100.0, "nearest")
        farther = self.wall("buy", 99.98, "farther")
        strategy.evaluate(self.context(strategy, 100.01, 1, [nearest, farther]))
        signals = strategy.evaluate(self.context(strategy, 100.02, 2, [nearest, farther]))
        self.assertEqual([signal.wall_id for signal in signals], ["BTCUSDT:buy:100:nearest"])
        self.assertEqual(signals[0].side, "long")

    def test_nearest_stable_resistance_is_the_only_bounce_signal_candidate(self):
        strategy = BounceStrategy()
        nearest = self.wall("sell", 100.0, "nearest")
        farther = self.wall("sell", 100.02, "farther")
        strategy.evaluate(self.context(strategy, 99.99, 1, [nearest, farther]))
        signals = strategy.evaluate(self.context(strategy, 99.98, 2, [nearest, farther]))
        self.assertEqual([signal.wall_id for signal in signals], ["BTCUSDT:sell:100:nearest"])
        self.assertEqual(signals[0].side, "short")

    def test_nearest_wall_with_invalid_rr_does_not_fall_through_to_farther_wall(self):
        strategy = BounceStrategy()
        nearest = self.wall("buy", 100.0, "nearest")
        farther = self.wall("buy", 99.98, "farther")
        # The closer opposite wall removes the required 1.5R upside.
        obstacle = self.wall("sell", 100.04, "obstacle")
        strategy.evaluate(self.context(strategy, 100.01, 1, [nearest, farther, obstacle]))
        signals = strategy.evaluate(self.context(strategy, 100.02, 2, [nearest, farther, obstacle]))
        self.assertEqual(signals, [])

    def test_new_nearer_wall_changes_priority_without_recreating_old_signal(self):
        strategy = BounceStrategy()
        old = self.wall("buy", 100.0, "old")
        strategy.evaluate(self.context(strategy, 100.01, 1, [old]))
        old_signal, = strategy.evaluate(self.context(strategy, 100.02, 2, [old]))
        nearer = self.wall("buy", 100.015, "new")
        # New support becomes the selected nearest wall. It gets its own
        # approach and confirmation; the old signal is not emitted again.
        self.assertEqual(strategy.evaluate(self.context(strategy, 100.02, 3, [old, nearer])), [])
        new_signal, = strategy.evaluate(self.context(strategy, 100.03, 4, [old, nearer]))
        self.assertNotEqual(old_signal.wall_id, new_signal.wall_id)
        self.assertEqual(new_signal.wall_id, "BTCUSDT:buy:100.015:new")

    def test_breakout_requires_hold_above_sell_wall_and_levels_are_directional(self):
        strategy = BreakoutStrategy()
        wall = self.wall("sell")
        self.assertEqual(strategy.evaluate(self.context(strategy, 100.01, 10, [wall])), [])
        self.assertEqual(strategy.evaluate(self.context(strategy, 100.02, 12, [wall])), [])
        self.assertEqual(strategy.evaluate(self.context(strategy, 100.02, 14, [wall])), [])
        signal, = strategy.evaluate(self.context(strategy, 100.02, 15.1, [wall]))
        self.assertEqual(signal.side, "long")
        self.assertAlmostEqual(signal.stop_loss, signal.wall_price)
        self.assertAlmostEqual(signal.take_profit - signal.entry_price,
                               1.5 * (signal.entry_price - signal.wall_price))

    def test_breakout_below_buy_wall_is_short_and_single_touch_is_not_enough(self):
        strategy = BreakoutStrategy()
        wall = self.wall("buy")
        self.assertEqual(strategy.evaluate(self.context(strategy, 99.99, 1, [wall])), [])
        self.assertEqual(strategy.evaluate(self.context(strategy, 99.99, 3, [wall])), [])
        self.assertEqual(strategy.evaluate(self.context(strategy, 100.01, 4, [wall])), [])
        self.assertEqual(strategy.evaluate(self.context(strategy, 99.99, 5, [wall])), [])
        self.assertEqual(strategy.evaluate(self.context(strategy, 99.99, 7, [wall])), [])
        self.assertEqual(strategy.evaluate(self.context(strategy, 99.99, 9, [wall])), [])
        signal, = strategy.evaluate(self.context(strategy, 99.98, 10.1, [wall]))
        self.assertEqual(signal.side, "short")
        self.assertLess(signal.take_profit, signal.entry_price)
        self.assertGreater(signal.stop_loss, signal.entry_price)

    def test_breakout_selects_nearest_confirmed_wall_for_each_book_side(self):
        strategy = BreakoutStrategy()
        farther = self.wall("sell", 100.0, "farther")
        nearer = self.wall("sell", 100.02, "nearer")
        events = [{**farther, "event": "CONSUMED"}, {**nearer, "event": "CONSUMED"}]
        strategy.evaluate(self.context(strategy, 100.04, 1, [], events))
        strategy.evaluate(self.context(strategy, 100.04, 3, [], events))
        strategy.evaluate(self.context(strategy, 100.04, 5, [], events))
        signals = strategy.evaluate(self.context(strategy, 100.04, 6.1, [], events))
        self.assertEqual([signal.wall_id for signal in signals], ["BTCUSDT:sell:100.02:nearer"])
        self.assertEqual(signals[0].side, "long")

    def test_breakout_cancelled_when_price_falls_back_or_wall_is_removed(self):
        strategy = BreakoutStrategy()
        wall = self.wall("sell")
        strategy.evaluate(self.context(strategy, 100.01, 0, [wall]))
        strategy.evaluate(self.context(strategy, 100.0, 2, [wall]))
        self.assertEqual(strategy.evaluate(self.context(strategy, 100.02, 3, [wall])), [])
        self.assertEqual(
            strategy.evaluate(self.context(strategy, 100.02, 9, [], [{**wall, "event": "REMOVED"}])),
            [],
        )

    def test_confirmation_ignores_book_only_refresh_and_resets_after_price_gap(self):
        strategy = BreakoutStrategy()
        wall = self.wall("sell")
        no_price_context = StrategyContext(
            "BTCUSDT", 100.02, (wall,), (), self.settings, 1, False
        )
        self.assertEqual(strategy.evaluate(no_price_context), [])
        self.assertEqual(strategy.evaluate(self.context(strategy, 100.02, 1, [wall])), [])
        self.assertEqual(strategy.evaluate(self.context(strategy, 100.02, 10, [wall])), [])
        self.assertEqual(strategy.evaluate(self.context(strategy, 100.02, 15, [wall])), [])

    def test_confirmed_signal_creates_virtual_trade_with_strategy_levels(self):
        strategy = BounceStrategy()
        wall = self.wall()
        strategy.evaluate(self.context(strategy, 100.01, 1, [wall]))
        signal, = strategy.evaluate(self.context(strategy, 100.02, 2, [wall]))
        manager = TradeManager(TradingSettings(stake_usdt=10, initial_balance_usdt=100))
        trade = manager.create_trade(signal, self.settings)
        self.assertEqual(trade["side"], "long")
        self.assertEqual(trade["strategy_id"], BOUNCE_STRATEGY_ID)
        self.assertEqual(trade["status"], "open")
        self.assertIsNotNone(trade["opened_at"])
        self.assertIsNone(manager.create_trade(signal, self.settings))

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
            stake_usdt=10, initial_balance_usdt=100, max_open_trades=1
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


class StrategyPipelineTests(unittest.TestCase):
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
                signal = {
                    "strategy_id": "stable_wall_v1",
                    "symbol": "BTCUSDT",
                    "wall_side": "buy",
                    "wall_price": 100.0,
                    "wall_id": f"wall-event-{event_type}",
                    "wall_size": 1000.0,
                    "wall_ratio": 900.0,
                }
                trade = engine.trade_manager.create_trade(signal, settings)

                events = engine.on_market_update(
                    "BTCUSDT",
                    100.0,
                    [],
                    [{"event": event_type, "side": "buy", "price": 100.0}],
                )

                self.assertEqual(trade["status"], "cancelled")
                self.assertEqual(trade["close_reason"], f"WALL_{event_type}")
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
        runtime = RuntimeController(
            TradingEngine({"BTCUSDT": PairSettings("BTCUSDT")})
        )

        text = dashboard_text(runtime)

        self.assertIn("Плавающий P/L: +0.00 USDT", text)
        self.assertIn("оценка баланса: 1000.00 USDT", text)
        self.assertIn("Доходность: +0.00%", text)
        self.assertIn("прибыльных сделок: 0.0%", text)
        self.assertIn("Доступно для новых сделок: 1000.00 USDT", text)
        self.assertIn("Общий лимит сделок не задан", text)

    def test_dashboard_shows_algorithm_feed_analysis_and_database_status(self):
        engine = TradingEngine({"BTCUSDT": PairSettings("BTCUSDT")})
        runtime = RuntimeController(engine)
        text = dashboard_text(runtime)
        self.assertIn("Алгоритм: остановлен", text)
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
        def __init__(self, data, edit_error=None):
            self.data = data
            self.from_user = User(id=7319, is_bot=False, first_name="Admin")
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


class PairSymbolValidationTests(unittest.TestCase):
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
        self.assertAlmostEqual(trade["stop_loss"], 100.02)
        self.assertAlmostEqual(
            trade["entry_price"] - trade["take_profit"],
            trade["stop_loss"] - trade["entry_price"],
        )
        self.manager.on_price("BTCUSDT", 100.0)
        events = self.manager.on_price("BTCUSDT", 100.03)
        self.assertEqual(events[0]["event"], "TRADE_CLOSED")
        self.assertEqual(trade["close_reason"], "STOP_LOSS")
        self.assertLess(trade["profit_usdt"], 0)

    def test_activation_take_profit_and_deduplication(self):
        trade = self.manager.create_trade(self.signal, self.settings)
        self.assertIsNotNone(trade)
        self.assertIsNone(self.manager.create_trade(self.signal, self.settings))
        events = self.manager.on_price("btcusdt", 100.0)
        self.assertEqual(events[0]["event"], "ENTRY_ACTIVATED")
        self.assertIn("BTCUSDT", self.manager.last_prices)

        self.manager.on_price("BTCUSDT", trade["take_profit"])
        self.assertEqual(trade["close_reason"], "TAKE_PROFIT")
        self.assertGreater(trade["profit_usdt"], 0)

    def test_statistics_include_unrealized_and_available_balance(self):
        self.manager.create_trade(self.signal, self.settings)
        self.manager.on_price("BTCUSDT", 100.0)

        stats = self.manager.get_statistics()

        expected_unrealized = (100.0 - 100.015) / 100.015 * 10
        self.assertAlmostEqual(stats["unrealized_profit_usdt"], expected_unrealized)
        self.assertAlmostEqual(stats["equity_usdt"], 100 + expected_unrealized)
        self.assertEqual(stats["available_balance_usdt"], 90)

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
        self.assertEqual(repository.saved_trade_batches[-1][0][0]["status"], "open")

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


if __name__ == "__main__":
    unittest.main()
