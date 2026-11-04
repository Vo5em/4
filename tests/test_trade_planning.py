import unittest
from dataclasses import replace
from time import monotonic
from unittest.mock import patch

from database.models import VirtualTrade
from script.engine import EngineMode, TradingEngine
from script.settings import PairSettings, TradingSettings
from app.runtime import RuntimeController
from script.strategies import BOUNCE_STRATEGY_ID, BREAKOUT_STRATEGY_ID
from script.trade_levels import (
    calculate_trade_levels,
    effective_trade_level_bps,
    estimate_trade_outcomes,
)
from script.trade_manager import TradeManager
from script.wall_tracker import WallTracker
from script.orderbook import LocalOrderBook
from script.market_data import analyze_synced_orderbook


def run_immediate(coroutine):
    try:
        coroutine.send(None)
    except StopIteration as completed:
        return completed.value
    finally:
        coroutine.close()
    raise AssertionError("Coroutine unexpectedly suspended in synchronous test")


class TradeLevelPolicyTests(unittest.TestCase):
    def test_four_approved_scenarios_are_tick_aligned_and_directional(self):
        cases = (
            (BOUNCE_STRATEGY_ID, "buy", 2284.80, "long", "cross_down"),
            (BOUNCE_STRATEGY_ID, "sell", 2300.67, "short", "cross_up"),
            (BREAKOUT_STRATEGY_ID, "buy", 2284.80, "short", "cross_down"),
            (BREAKOUT_STRATEGY_ID, "sell", 2300.67, "long", "cross_up"),
        )
        for strategy, wall_side, wall, expected_side, trigger in cases:
            with self.subTest(strategy=strategy, wall_side=wall_side):
                levels = calculate_trade_levels(
                    strategy_id=strategy,
                    wall_side=wall_side,
                    wall_price=wall,
                    tick_size="0.01",
                    take_profit_bps=5,
                    stop_loss_bps=5,
                )
                self.assertEqual(levels["side"], expected_side)
                self.assertEqual(levels["entry_trigger_direction"], trigger)
                for key in ("entry_price", "stop_loss", "take_profit"):
                    self.assertEqual(levels[key] % levels["entry_price"].__class__("0.01"), 0)
                if expected_side == "long":
                    self.assertLess(levels["stop_loss"], levels["entry_price"])
                    self.assertLess(levels["entry_price"], levels["take_profit"])
                else:
                    self.assertLess(levels["take_profit"], levels["entry_price"])
                    self.assertLess(levels["entry_price"], levels["stop_loss"])
                self.assertGreater(levels["take_profit"], 0)
                self.assertGreater(levels["stop_loss"], 0)

    def test_bounce_bid_matches_approved_unrounded_formula_before_tick_rounding(self):
        levels = calculate_trade_levels(
            strategy_id=BOUNCE_STRATEGY_ID,
            wall_side="buy",
            wall_price="2284.80",
            tick_size="0.00001",
            take_profit_bps=5,
            stop_loss_bps=5,
        )
        self.assertAlmostEqual(float(levels["entry_price"]), 2285.14272, places=5)
        self.assertAlmostEqual(float(levels["stop_loss"]), 2284.00014, places=5)
        self.assertAlmostEqual(float(levels["take_profit"]), 2286.28530, places=5)

    def test_tp_and_sl_bps_are_from_entry_and_round_outward_to_tick(self):
        levels = calculate_trade_levels(
            strategy_id=BOUNCE_STRATEGY_ID,
            wall_side="buy", wall_price="100", tick_size="0.01",
            take_profit_bps=12, stop_loss_bps=8,
        )
        self.assertEqual(float(levels["entry_price"]), 100.02)
        self.assertEqual(float(levels["take_profit"]), 100.15)
        self.assertEqual(float(levels["stop_loss"]), 99.93)

    def test_breakout_tp_before_wall_and_bounce_sl_inside_wall_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "does not reach"):
            calculate_trade_levels(
                strategy_id=BREAKOUT_STRATEGY_ID, wall_side="sell",
                wall_price=100, tick_size="0.01",
                take_profit_bps=1, stop_loss_bps=5,
            )
        with self.assertRaisesRegex(ValueError, "does not extend beyond"):
            calculate_trade_levels(
                strategy_id=BOUNCE_STRATEGY_ID, wall_side="buy",
                wall_price=100, tick_size="0.01",
                take_profit_bps=5, stop_loss_bps=1.5,
            )

    def test_invalid_bps_values_are_rejected(self):
        for invalid in (0, -1, float("nan"), float("inf")):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                calculate_trade_levels(
                    strategy_id=BOUNCE_STRATEGY_ID, wall_side="buy",
                    wall_price=100, tick_size="0.01",
                    take_profit_bps=invalid, stop_loss_bps=5,
                )
        for factory in (
            lambda: TradingSettings(default_take_profit_bps="bad"),
            lambda: TradingSettings(default_stop_loss_bps=float("nan")),
            lambda: PairSettings("BTCUSDT", take_profit_bps=-1),
            lambda: PairSettings("ETHUSDT", stop_loss_bps="bad"),
        ):
            with self.subTest(factory=factory), self.assertRaises(ValueError):
                factory()

    def test_pair_overrides_are_independent_and_fall_back_to_global(self):
        global_settings = TradingSettings(
            default_take_profit_bps=40, default_stop_loss_bps=40,
        )
        btc = PairSettings("BTCUSDT", take_profit_bps=50)
        eth = PairSettings("ETHUSDT", stop_loss_bps=25)
        self.assertEqual(effective_trade_level_bps(btc, global_settings), (50, 40))
        self.assertEqual(effective_trade_level_bps(eth, global_settings), (40, 25))
        self.assertEqual(
            effective_trade_level_bps(replace(btc, take_profit_bps=None), global_settings),
            (40, 40),
        )

    def test_pnl_projection_uses_notional_and_supplied_fee_rate_both_sides(self):
        outcome = estimate_trade_outcomes(
            side="long", entry_price=100, take_profit_price=100.05,
            stop_loss_price=99.95, position_notional_usdt=750,
            taker_fee_rate=0.00055,
        )
        self.assertAlmostEqual(float(outcome["tp_gross_pnl_usdt"]), 0.375)
        self.assertAlmostEqual(float(outcome["entry_fee_usdt"]), 0.4125)
        self.assertLess(outcome["tp_net_pnl_usdt"], 0)
        self.assertLess(outcome["sl_net_pnl_usdt"], -0.375)


class TradingPlanLifecycleTests(unittest.TestCase):
    def make_engine(self, strategy=BOUNCE_STRATEGY_ID, *, max_open=3, balance=1000):
        settings = PairSettings(
            "ETHUSDT", max_wall_distance_bps=1000, max_open_trades=max_open,
            min_wall_age_seconds=0,
        )
        engine = TradingEngine(
            {"ETHUSDT": settings},
            TradingSettings(
                stake_usdt=30, initial_balance_usdt=balance,
                max_open_trades=max_open, leverage=25,
            ),
            enabled_strategies={"ETHUSDT": {strategy}},
        )
        engine.instrument_tick_sizes["ETHUSDT"] = 0.01
        engine.start()
        return engine, settings

    @staticmethod
    def wall(side, price, identity, *, size=1000, age=45):
        return {
            "side": side, "price": price, "first_seen_at": identity,
            "current_size": size, "initial_size": size, "wall_ratio": 900,
            "age": age,
        }

    def update(self, engine, price, walls, *, price_updated=True):
        return engine.on_market_update(
            "ETHUSDT", price, walls, (), walls, price_updated=price_updated
        )

    class Repository:
        def __init__(self):
            self.fail = False
            self.trade_batches = []
            self.pair_batches = []
            self.global_batches = []

        async def save_trades(self, trades, balance_usdt, engine_mode=None):
            if self.fail:
                raise OSError("simulated PostgreSQL write failure")
            self.trade_batches.append((trades, balance_usdt, engine_mode))

        async def save_market_price(self, symbol, price):
            return None

        async def save_pair_settings_with_trades(
            self, settings, trades, balance_usdt, engine_mode,
        ):
            if self.fail:
                raise OSError("simulated pair settings write failure")
            self.pair_batches.append((settings, trades, balance_usdt, engine_mode))

        async def save_trading_settings_with_trades(
            self, settings, trades, balance_usdt, engine_mode,
        ):
            if self.fail:
                raise OSError("simulated global settings write failure")
            self.global_batches.append((settings, trades, balance_usdt, engine_mode))

    @staticmethod
    async def process_update(engine, price, walls):
        return await engine.process_market_update(
            "ETHUSDT", price, walls, (), walls,
        )

    def test_stable_bid_and_ask_create_two_pending_plans_without_confirmation(self):
        engine, _ = self.make_engine()
        walls = [
            self.wall("buy", 100.0, "bid"),
            self.wall("sell", 101.0, "ask"),
        ]
        events = self.update(engine, 100.5, walls)
        pending = engine.trade_manager.get_pending_trades()
        self.assertEqual(len(pending), 2)
        self.assertEqual(sum(event["event"] == "TRADE_CREATED" for event in events), 2)
        plans = {trade["wall_side"]: trade for trade in pending}
        self.assertEqual(plans["buy"]["side"], "long")
        self.assertEqual(plans["sell"]["side"], "short")
        self.assertEqual(plans["buy"]["accounting_version"], 1)
        self.assertEqual(plans["sell"]["accounting_version"], 1)
        self.assertEqual(engine.trade_manager.get_statistics()["reserved_margin_usdt"], 0)
        self.assertEqual(engine.trade_manager.get_statistics()["available_balance_usdt"], 1000)

    def test_candidate_gets_pending_only_after_40_continuous_scanner_seconds(self):
        engine, settings = self.make_engine()
        book = LocalOrderBook()
        book.bids[100.0] = 10.0
        tracker = WallTracker(min_age_seconds=40)
        candidate = {"side": "buy", "price": 100.0, "notional": 1000.0,
                     "wall_ratio": 900.0}
        clock = [100.0]

        with patch("script.wall_tracker.time.monotonic", side_effect=lambda: clock[0]):
            tracker.update([candidate], book, {})
            clock[0] = 139.0
            tracker.update([candidate], book, {})
            self.update(engine, 100.5, tracker.get_stable_walls(), price_updated=False)
            self.assertEqual(engine.trade_manager.get_pending_trades(), [])

            clock[0] = 140.0
            tracker.update([candidate], book, {})
            stable = tracker.get_stable_walls()
            self.assertEqual(len(stable), 1)
            created = self.update(engine, 100.5, stable, price_updated=False)

        self.assertEqual(len(engine.trade_manager.get_pending_trades()), 1)
        self.assertEqual([event["event"] for event in created], ["TRADE_CREATED"])

    def test_pending_plans_have_stable_identity_and_restore_without_duplicates(self):
        engine, settings = self.make_engine()
        wall = self.wall("buy", 100, "restart-wall")
        self.update(engine, 100.5, [wall])
        original, = engine.trade_manager.get_pending_trades()
        restored = TradeManager(engine.trading_settings)
        restored.restore_state(engine.trade_manager.trades, engine.trade_manager.balance_usdt)
        restarted_engine = TradingEngine(
            {"ETHUSDT": settings}, engine.trading_settings,
            trade_manager=restored,
            enabled_strategies={"ETHUSDT": {BOUNCE_STRATEGY_ID}},
        )
        restarted_engine.instrument_tick_sizes["ETHUSDT"] = 0.01
        restarted_engine.start()
        restarted_engine.on_market_update("ETHUSDT", 100.5, [wall], (), [wall])
        self.assertEqual(len(restored.get_pending_trades()), 1)
        self.assertEqual(restored.get_pending_trades()[0]["signal_id"], original["signal_id"])

    def test_open_limit_retains_untriggered_pending_but_requires_fresh_cross(self):
        manager = TradeManager(TradingSettings(
            stake_usdt=10, initial_balance_usdt=100, max_open_trades=1,
        ))
        settings = PairSettings("BTCUSDT")
        manager.set_pair_settings_for_price_check(settings)
        manager.on_price("BTCUSDT", 100.2)
        first = manager.create_trade({
            "signal_id": "global-a", "strategy_id": "bounce", "symbol": "BTCUSDT",
            "wall_side": "buy", "wall_price": 100, "wall_id": "global-wall-a",
            "wall_size": 1000, "wall_ratio": 900, "side": "long",
            "entry_price": 100.1, "stop_loss": 99.9, "take_profit": 100.4,
            "entry_trigger_direction": "cross_down", "reference_price": 100.2,
        }, settings)
        second = manager.create_trade({
            "signal_id": "global-b", "strategy_id": "bounce", "symbol": "ETHUSDT",
            "wall_side": "buy", "wall_price": 100, "wall_id": "global-wall-b",
            "wall_size": 1000, "wall_ratio": 900, "side": "long",
            "entry_price": 100.1, "stop_loss": 99.9, "take_profit": 100.4,
            "entry_trigger_direction": "cross_down", "reference_price": 100.2,
        }, PairSettings("ETHUSDT"))
        self.assertEqual(manager.on_price("BTCUSDT", 100.0)[0]["event"], "ENTRY_ACTIVATED")
        manager.on_price("ETHUSDT", 100.0)
        self.assertEqual(first["status"], "open")
        self.assertEqual(second["status"], "pending")
        self.assertFalse(second["entry_armed"])
        manager._close_trade(first, 100.0, "test")
        manager.on_price("ETHUSDT", 100.2)  # safe side arms after the slot is free
        manager.on_price("ETHUSDT", 100.0)  # a new cross, never a backdated open
        self.assertEqual(second["status"], "open")

    def test_pending_without_funds_is_rejected_at_entry_without_negative_available_balance(self):
        manager = TradeManager(TradingSettings(
            stake_usdt=10, initial_balance_usdt=10, max_open_trades=1,
        ))
        settings = PairSettings("BTCUSDT", leverage=25)
        manager.set_pair_settings_for_price_check(settings)
        manager.on_price("BTCUSDT", 100.2)
        pending = manager.create_trade({
            "signal_id": "funds-late", "strategy_id": "bounce", "symbol": "BTCUSDT",
            "wall_side": "buy", "wall_price": 100, "wall_id": "funds-wall",
            "wall_size": 1000, "wall_ratio": 900, "side": "long",
            "entry_price": 100.1, "stop_loss": 99.9, "take_profit": 100.4,
            "entry_trigger_direction": "cross_down", "reference_price": 100.2,
        }, settings)
        self.assertEqual(manager.get_statistics()["available_balance_usdt"], 10)
        events = manager.on_price("BTCUSDT", 100.0)
        self.assertTrue(any(event.get("reason") == "INSUFFICIENT_AVAILABLE_BALANCE" for event in events))
        self.assertEqual(pending["status"], "pending")
        self.assertGreaterEqual(manager.get_statistics()["available_balance_usdt"], 0)

    def test_complete_stable_snapshots_keep_breakout_pending_for_four_symbols(self):
        for symbol in ("BTCUSDT", "ETHUSDT", "DOGEUSDT", "1000PEPEUSDT"):
            with self.subTest(symbol=symbol):
                settings = PairSettings(
                    symbol, min_wall_age_seconds=0, max_wall_distance_bps=1000,
                )
                engine = TradingEngine(
                    {symbol: settings},
                    TradingSettings(stake_usdt=10, initial_balance_usdt=500),
                    enabled_strategies={symbol: {BREAKOUT_STRATEGY_ID}},
                )
                engine.instrument_tick_sizes[symbol] = 0.0001
                engine.start()
                wall = {
                    "side": "buy", "price": 100.0, "first_seen_at": f"life-{symbol}",
                    "current_size": 10000, "initial_size": 10000,
                    "wall_ratio": 900, "age": 50,
                }
                candidates = [{
                    "side": "buy", "price": 100.0, "notional": 10000,
                    "wall_ratio": 900,
                }]
                engine.on_market_update(symbol, 100.5, [wall], (), candidates)
                plan, = engine.trade_manager.get_pending_trades()
                engine.on_market_update(symbol, 100.5, [dict(wall, age=50.2)], (), candidates)
                self.assertEqual(plan["status"], "pending")
                self.assertEqual(len(engine.trade_manager.get_pending_trades()), 1)

    def test_other_stable_walls_do_not_cancel_existing_wall_plan(self):
        engine, _ = self.make_engine(BREAKOUT_STRATEGY_ID)
        original = self.wall("buy", 100, "keep-this-plan")
        other = self.wall("sell", 101, "another-wall")
        self.update(engine, 100.5, [original])
        plan, = engine.trade_manager.get_pending_trades()
        events = self.update(engine, 100.5, [original, other])
        self.assertEqual(plan["status"], "pending")
        self.assertEqual(len(engine.trade_manager.get_pending_trades()), 2)
        self.assertFalse(any(
            event.get("trade", {}).get("signal_id") == plan["signal_id"]
            and event.get("event") == "ENTRY_CANCELLED"
            for event in events
        ))

    def test_incomplete_wall_snapshot_neither_cancels_nor_activates_pending(self):
        engine, _ = self.make_engine(BREAKOUT_STRATEGY_ID)
        wall = self.wall("buy", 100, "temporary-data-gap")
        self.update(engine, 100.5, [wall])
        plan, = engine.trade_manager.get_pending_trades()
        events = engine.on_market_update("ETHUSDT", 100.0, [], (), None)
        self.assertEqual(plan["status"], "pending")
        self.assertFalse(any(event["event"] == "ENTRY_ACTIVATED" for event in events))

    def test_reconnecting_candidate_cancels_old_pending_until_full_requalification(self):
        engine, _ = self.make_engine(BREAKOUT_STRATEGY_ID)
        wall = self.wall("buy", 100, "same-lifecycle")
        engine.instrument_tick_sizes["ETHUSDT"] = 0.01
        self.update(engine, 100.5, [wall])
        plan, = engine.trade_manager.get_pending_trades()
        candidate = {"side": "buy", "price": 100.0, "notional": 1000, "wall_ratio": 900}
        actual_wall_id = plan["wall_id"]
        engine.wall_diagnostics["ETHUSDT"]["tracked_walls_by_id"] = {
            actual_wall_id: {"wall_id": actual_wall_id, "status": "CANDIDATE", "qualifies": True,
                             "age_seconds": 2, "wall_ratio": 900, "notional": 1000}
        }
        for _ in range(3):
            engine.on_market_update("ETHUSDT", 100.5, [], (), [candidate])
            self.assertEqual(plan["status"], "cancelled")
            self.assertEqual(engine.trade_manager.get_pending_trades(), [])
        self.assertEqual(plan["close_reason"], "NO_STABLE_WALL")
        engine.on_market_update("ETHUSDT", 100.5, [dict(wall, age=45)], (), [candidate])
        replacement, = engine.trade_manager.get_pending_trades()
        self.assertEqual(replacement["wall_id"], actual_wall_id)
        self.assertNotEqual(replacement["signal_id"], plan["signal_id"])

    def test_real_wall_tracker_restart_revalidates_same_pending_identity(self):
        symbol = "ETHUSDT"
        settings = PairSettings(
            symbol, min_wall_ratio=50, max_wall_distance_bps=200,
            min_wall_age_seconds=0,
        )
        engine = TradingEngine(
            {symbol: settings}, TradingSettings(initial_balance_usdt=500),
            enabled_strategies={symbol: {BREAKOUT_STRATEGY_ID}},
        )
        engine.instrument_tick_sizes[symbol] = 0.01
        engine.start()
        book = LocalOrderBook()
        book.bids.update({100.0: 1.0, 99.9: 1.0, 99.8: 1.0, 99.7: 1.0, 99.6: 100.0})
        book.synchronized = True
        flow = {
            "buy_volume_per_second": 0.0, "sell_volume_per_second": 0.0,
            "buy_volume_by_price_total": {}, "sell_volume_by_price_total": {},
        }
        tracker = WallTracker(min_age_seconds=40, consumed_ratio=settings.consumed_ratio)
        result = analyze_synced_orderbook(book, 100.5, flow, settings, tracker)
        self.assertEqual(len(result["candidates"]), 1)
        tracker.walls[("buy", 99.6)]["first_seen"] = monotonic() - 41
        result = analyze_synced_orderbook(book, 100.5, flow, settings, tracker)
        self.assertEqual(len(result["stable_walls"]), 1)
        engine.record_wall_analysis(symbol, 100.5, result)
        engine.on_market_update(
            symbol, 100.5, result["stable_walls"], result["wall_events"], result["candidates"]
        )
        plan, = engine.trade_manager.get_pending_trades()
        identity_before = plan["wall_id"]

        restarted_tracker = WallTracker(min_age_seconds=40, consumed_ratio=settings.consumed_ratio)
        restarted_tracker.seed_lifecycle_identities(
            engine.trade_manager.get_wall_lifecycle_identities()
        )
        reconnect_result = analyze_synced_orderbook(
            book, 100.5, flow, settings, restarted_tracker
        )
        self.assertEqual(reconnect_result["stable_walls"], [])
        self.assertEqual(len(reconnect_result["candidates"]), 1)
        engine.record_wall_analysis(symbol, 100.5, reconnect_result)
        engine.on_market_update(
            symbol, 100.5, reconnect_result["stable_walls"],
            reconnect_result["wall_events"], reconnect_result["candidates"],
        )
        self.assertEqual(plan["status"], "cancelled")
        self.assertEqual(plan["wall_id"], identity_before)

        restarted_tracker.walls[("buy", 99.6)]["first_seen"] = monotonic() - 41
        stable_again = analyze_synced_orderbook(
            book, 100.5, flow, settings, restarted_tracker
        )
        engine.record_wall_analysis(symbol, 100.5, stable_again)
        engine.on_market_update(
            symbol, 100.5, stable_again["stable_walls"],
            stable_again["wall_events"], stable_again["candidates"],
        )
        self.assertEqual(plan["status"], "cancelled")
        self.assertEqual(plan["wall_id"], identity_before)
        replacement, = engine.trade_manager.get_pending_trades()
        self.assertEqual(replacement["wall_id"], identity_before)
        self.assertNotEqual(replacement["signal_id"], plan["signal_id"])

    def test_real_wall_filter_loss_cancels_with_specific_internal_reason(self):
        engine, _ = self.make_engine(BREAKOUT_STRATEGY_ID)
        wall = self.wall("buy", 100, "ratio-loss")
        self.update(engine, 100.5, [wall])
        plan, = engine.trade_manager.get_pending_trades()
        engine.wall_diagnostics["ETHUSDT"].update({
            "tracked_walls_by_id": {
                plan["wall_id"]: {
                    "wall_id": plan["wall_id"], "status": "BELOW_FILTER",
                    "qualifies": False, "age_seconds": 52, "wall_ratio": 3.0,
                    "notional": 1000,
                }
            },
            "wall_level_diagnostics": {
                "buy:100": {"wall_ratio": 3.0, "rejection": "WALL_BELOW_RATIO"}
            },
        })
        events = engine.on_market_update("ETHUSDT", 100.5, [], (), [])
        cancelled, = [event for event in events if event["event"] == "ENTRY_CANCELLED"]
        self.assertEqual(plan["status"], "cancelled")
        self.assertEqual(plan["close_reason"], "NO_STABLE_WALL")
        self.assertEqual(cancelled["diagnostic_reason"], "WALL_RATIO_FAILED")
        self.assertEqual(
            engine.wall_diagnostics["ETHUSDT"]["planner_reconciliation"]["cancel_reasons"],
            {"WALL_RATIO_FAILED": 1},
        )
        # Once the same lifecycle qualifies again it gets a new durable plan
        # generation; the canceled database row remains historical.
        engine.on_market_update(
            "ETHUSDT", 100.5, [dict(wall, age=55)], (),
            [{"side": "buy", "price": 100.0, "notional": 1000, "wall_ratio": 900}],
        )
        replacement, = engine.trade_manager.get_pending_trades()
        self.assertNotEqual(replacement["signal_id"], plan["signal_id"])
        self.assertEqual(plan["status"], "cancelled")
        engine.on_market_update(
            "ETHUSDT", 100.5, [dict(wall, age=55.2)], (),
            [{"side": "buy", "price": 100.0, "notional": 1000, "wall_ratio": 900}],
        )
        self.assertEqual(len(engine.trade_manager.get_pending_trades()), 1)

    def test_each_qualified_wall_keeps_its_plan_and_unchanged_plans_are_not_recreated(self):
        engine, _ = self.make_engine()
        far = self.wall("buy", 99.0, "far", size=1200)
        near = self.wall("buy", 100.0, "near", size=1300)
        self.update(engine, 100.5, [far])
        first = engine.trade_manager.get_pending_trades()[0]
        count = len(engine.trade_manager.trades)
        self.update(engine, 100.5, [far])
        self.assertEqual(len(engine.trade_manager.trades), count)
        self.update(engine, 100.5, [far, near])
        current = engine.trade_manager.get_pending_trades()
        self.assertEqual({trade["wall_id"] for trade in current}, {
            "ETHUSDT:buy:99:far", "ETHUSDT:buy:100:near",
        })
        self.assertEqual(first["status"], "pending")

    def test_ten_eligible_stable_walls_create_exactly_ten_pending_plans(self):
        engine, _ = self.make_engine(max_open=1)
        walls = [
            self.wall("buy", 100.0 - index * 0.05, f"ten-wall-{index}")
            for index in range(10)
        ]

        created = self.update(engine, 100.5, walls)
        pending = engine.trade_manager.get_pending_trades()
        self.assertEqual(len([event for event in created if event["event"] == "TRADE_CREATED"]), 10)
        self.assertEqual(len(pending), 10)
        self.assertEqual(len({trade["wall_id"] for trade in pending}), 10)
        reconciliation = engine.wall_diagnostics["ETHUSDT"]["planner_reconciliation"]
        self.assertEqual(reconciliation["status"], "complete")
        self.assertEqual(reconciliation["available_stable_count"], 10)
        self.assertEqual(reconciliation["pending_after"], 10)
        self.assertEqual(reconciliation["stable_without_pending"], 0)
        self.assertEqual(reconciliation["pending_without_stable"], 0)

        repeated = self.update(engine, 100.5, walls)
        self.assertEqual(repeated, [])
        self.assertEqual(len(engine.trade_manager.get_pending_trades()), 10)

    def test_reconciliation_repairs_duplicate_pending_for_one_physical_wall(self):
        engine, _ = self.make_engine()
        wall = self.wall("buy", 100.0, "dedupe-wall")
        self.update(engine, 100.5, [wall])
        original, = engine.trade_manager.get_pending_trades()
        duplicate = dict(original)
        duplicate["signal_id"] = original["signal_id"] + ":restored-duplicate"
        engine.trade_manager.trades.append(duplicate)

        events = self.update(engine, 100.5, [wall])

        active = engine.trade_manager.get_pending_trades()
        self.assertEqual(len(active), 1)
        self.assertIs(active[0], original)
        self.assertEqual(duplicate["status"], "cancelled")
        self.assertEqual(duplicate["close_reason"], "DUPLICATE_PENDING")
        self.assertIn("ENTRY_CANCELLED", [event["event"] for event in events])

    def test_adding_wall_persists_only_new_plan_without_changing_old_pending(self):
        engine, _ = self.make_engine()
        repository = self.Repository()
        engine.repository = repository
        engine.start()
        far = self.wall("buy", 99.0, "persisted-far")
        near = self.wall("buy", 100.0, "persisted-near")

        async def updates():
            await self.process_update(engine, 100.5, [far])
            original_balance = engine.trade_manager.balance_usdt
            original_pending, = engine.trade_manager.get_pending_trades()
            events = await self.process_update(engine, 100.5, [far, near])
            return original_balance, original_pending, events

        # Drive both updates in the same coroutine so the engine's asyncio
        # locks remain bound to one event loop on Windows.
        original_balance, original_pending, events = run_immediate(updates())

        self.assertEqual(len(repository.trade_batches), 2)
        replacement_batch = repository.trade_batches[-1][0]
        self.assertEqual(
            [(row["signal_id"], row["status"], row.get("close_reason"))
             for row in replacement_batch],
            [(next(row["trade"]["signal_id"] for row in events
                   if row["event"] == "TRADE_CREATED"), "pending", None)],
        )
        self.assertEqual(repository.trade_batches[-1][1], original_balance)
        self.assertEqual(original_pending["status"], "pending")
        self.assertEqual(engine.trade_manager.get_statistics()["reserved_margin_usdt"], 0)

    def test_failed_postgres_new_plan_rolls_back_without_changing_existing_pending(self):
        engine, _ = self.make_engine()
        repository = self.Repository()
        engine.repository = repository
        engine.start()
        far = self.wall("buy", 99.0, "rollback-far")
        near = self.wall("buy", 100.0, "rollback-near")
        async def updates():
            await self.process_update(engine, 100.5, [far])
            initial_balance = engine.trade_manager.balance_usdt
            original_pending, = engine.trade_manager.get_pending_trades()
            repository.fail = True
            events = await self.process_update(engine, 100.5, [far, near])
            return initial_balance, original_pending, events

        initial_balance, original_pending, events = run_immediate(updates())

        restored_pending, = engine.trade_manager.get_pending_trades()
        self.assertEqual(events, [])
        self.assertEqual(restored_pending["signal_id"], original_pending["signal_id"])
        self.assertEqual(restored_pending["wall_id"], original_pending["wall_id"])
        self.assertEqual(engine.trade_manager.balance_usdt, initial_balance)
        self.assertEqual(engine.trade_manager.get_statistics()["reserved_margin_usdt"], 0)
        self.assertEqual(engine.get_status()["persistence_pending"], 0)
        self.assertEqual(len(repository.trade_batches), 1)

    def test_pair_level_update_replaces_pending_but_preserves_open_levels(self):
        engine, settings = self.make_engine()
        engine.start()
        bid = self.wall("buy", 100, "pair-level-bid")
        ask = self.wall("sell", 101, "pair-level-ask")
        self.update(engine, 100.5, [bid, ask])
        bid_pending = engine.trade_manager.get_pending_for_slot(
            "ETHUSDT", BOUNCE_STRATEGY_ID, "buy"
        )
        ask_old = dict(engine.trade_manager.get_pending_for_slot(
            "ETHUSDT", BOUNCE_STRATEGY_ID, "sell"
        ))
        engine.trade_manager.on_price("ETHUSDT", 100.5)
        engine.trade_manager.on_price("ETHUSDT", 100.01)
        self.assertEqual(bid_pending["status"], "open")
        frozen_open_levels = tuple(
            bid_pending[key] for key in ("entry_price", "stop_loss", "take_profit")
        )
        original_balance = engine.trade_manager.balance_usdt

        runtime = RuntimeController(engine)

        async def no_worker_changes():
            return None

        runtime._sync_workers = no_worker_changes
        updated = replace(settings, take_profit_bps=8)
        run_immediate(runtime.update_pair_settings(updated))

        ask_new = engine.trade_manager.get_pending_for_slot(
            "ETHUSDT", BOUNCE_STRATEGY_ID, "sell"
        )
        self.assertEqual(ask_old["status"], "pending")
        self.assertEqual(ask_new["signal_id"], ask_old["signal_id"])
        self.assertNotEqual(ask_new["take_profit"], ask_old["take_profit"])
        self.assertAlmostEqual(
            ask_new["take_profit"],
            ask_new["entry_price"] * (1 - 8 / 10000),
            delta=0.01,
        )
        self.assertEqual(tuple(
            bid_pending[key] for key in ("entry_price", "stop_loss", "take_profit")
        ), frozen_open_levels)
        self.assertEqual(engine.trade_manager.balance_usdt, original_balance)
        self.assertEqual(engine.trade_manager.get_statistics()["reserved_margin_usdt"], 30)

        run_immediate(runtime.update_pair_settings(
            replace(updated, take_profit_bps=None)
        ))
        ask_reset = engine.trade_manager.get_pending_for_slot(
            "ETHUSDT", BOUNCE_STRATEGY_ID, "sell"
        )
        self.assertEqual(ask_reset["signal_id"], ask_new["signal_id"])
        self.assertAlmostEqual(
            ask_reset["take_profit"],
            float(ask_reset["entry_price"]) * (1 - 5 / 10000),
            delta=0.01,
        )

    def test_global_level_update_replans_pending_atomically_and_rolls_back_on_error(self):
        engine, _ = self.make_engine()
        repository = self.Repository()
        engine.repository = repository
        engine.start()
        wall = self.wall("buy", 100, "global-level-bid")
        self.update(engine, 100.5, [wall])
        old_pending, = engine.trade_manager.get_pending_trades()
        old_take_profit = old_pending["take_profit"]
        runtime = RuntimeController(engine)

        new_settings = run_immediate(runtime.update_trade_level_defaults(
            take_profit_bps=12
        ))
        new_pending, = engine.trade_manager.get_pending_trades()
        self.assertEqual(new_settings.default_take_profit_bps, 12)
        self.assertNotEqual(new_pending["take_profit"], old_take_profit)
        self.assertEqual(len(repository.global_batches), 1)
        self.assertEqual(
            [trade["status"] for trade in repository.global_batches[0][1]],
            ["pending"],
        )

        previous_settings = engine.trading_settings
        previous_pending_id = new_pending["signal_id"]
        balance = engine.trade_manager.balance_usdt
        repository.fail = True
        with self.assertRaises(RuntimeError):
            run_immediate(runtime.update_trade_level_defaults(stop_loss_bps=10))
        restored_pending, = engine.trade_manager.get_pending_trades()
        self.assertEqual(engine.trading_settings, previous_settings)
        self.assertEqual(restored_pending["signal_id"], previous_pending_id)
        self.assertEqual(engine.trade_manager.balance_usdt, balance)

    def test_missing_current_bybit_tick_size_cancels_pending_and_blocks_new_plan(self):
        engine, _ = self.make_engine()
        wall = self.wall("buy", 100.0, "tick-required")
        self.update(engine, 100.5, [wall])
        pending, = engine.trade_manager.get_pending_trades()
        engine.instrument_tick_sizes.pop("ETHUSDT")

        events = self.update(engine, 100.5, [wall])

        self.assertEqual(pending["status"], "cancelled")
        self.assertEqual(pending["close_reason"], "TICK_SIZE_UNAVAILABLE")
        self.assertEqual(engine.trade_manager.get_pending_trades(), [])
        self.assertFalse(any(event["event"] == "TRADE_CREATED" for event in events))

    def test_wall_disappearance_cancels_pending_but_does_not_touch_open(self):
        engine, _ = self.make_engine()
        wall = self.wall("buy", 100, "support")
        self.update(engine, 100.5, [wall])
        pending = engine.trade_manager.get_pending_trades()[0]
        open_trade = dict(pending)
        open_trade.update(signal_id="frozen-open", status="open", opened_at=pending["created_at"])
        engine.trade_manager.trades.append(open_trade)
        self.update(engine, 100.5, [])
        self.assertEqual(pending["status"], "cancelled")
        self.assertEqual(open_trade["status"], "open")

    def test_pair_open_blocks_and_cancels_crossed_pending_without_backdated_open(self):
        settings = PairSettings("BTCUSDT", max_open_trades=1)
        manager = TradeManager(TradingSettings(
            stake_usdt=10, initial_balance_usdt=100, max_open_trades=1
        ))
        manager.set_pair_settings_for_price_check(settings)
        manager.on_price("BTCUSDT", 100.015)
        plan = {
            "strategy_id": BOUNCE_STRATEGY_ID, "symbol": "BTCUSDT",
            "wall_side": "buy", "wall_price": 100, "wall_id": "wall-a",
            "wall_size": 1000, "wall_ratio": 900, "side": "long",
            "entry_price": 100.01, "stop_loss": 99.5, "take_profit": 101.0,
            "entry_trigger_direction": "cross_down", "reference_price": 100.1,
            "signal_id": "a",
        }
        first = manager.create_trade(plan, settings)
        second = manager.create_trade(dict(plan, wall_side="sell", wall_id="wall-b",
                                           side="short", entry_price=100.02,
                                           stop_loss=100.05, take_profit=99.99,
                                           entry_trigger_direction="cross_up",
                                           reference_price=100.0, signal_id="b"), settings)
        # PENDINGs neither reserve margin nor consume OPEN slots.
        self.assertEqual(len(manager.get_pending_trades()), 2)
        self.assertEqual(manager.get_statistics()["reserved_margin_usdt"], 0)
        self.assertEqual(manager.on_price("BTCUSDT", 100.0)[0]["event"], "ENTRY_ACTIVATED")
        # Since a pair already has OPEN, a crossed PENDING is cancelled.
        blocked = manager.on_price("BTCUSDT", 100.03)
        self.assertTrue(any(event.get("reason") == "ENTRY_BLOCKED_BY_OPEN_TRADE" for event in blocked))
        self.assertEqual(second["status"], "cancelled")
        self.assertEqual(second["close_reason"], "ENTRY_BLOCKED_BY_OPEN_TRADE")
        manager._close_trade(first, 100.0, "test")
        manager.on_price("BTCUSDT", 100.0)
        self.assertEqual(second["status"], "cancelled")

    def test_restore_disarms_pending_until_a_new_safe_side_and_cross(self):
        pair = PairSettings("BTCUSDT")
        manager = TradeManager(TradingSettings(initial_balance_usdt=100))
        trade = manager.create_trade({
            "strategy_id": BOUNCE_STRATEGY_ID, "symbol": "BTCUSDT",
            "wall_side": "buy", "wall_price": 100, "wall_id": "life",
            "wall_size": 1, "wall_ratio": 1, "side": "long",
            "entry_price": 100.01, "stop_loss": 99.98, "take_profit": 100.04,
            "entry_trigger_direction": "cross_down", "reference_price": 100.1,
            "signal_id": "restore-plan",
        }, pair)
        restored = TradeManager(manager.trading_settings)
        restored.restore_state([dict(trade)], manager.balance_usdt)
        self.assertFalse(restored.trades[0]["entry_armed"])
        restored.on_price("BTCUSDT", 100.0)
        self.assertEqual(restored.trades[0]["status"], "pending")
        restored.on_price("BTCUSDT", 100.1)
        restored.on_price("BTCUSDT", 100.0)
        self.assertEqual(restored.trades[0]["status"], "open")

    def test_postgres_model_has_additive_lifecycle_columns(self):
        columns = VirtualTrade.__table__.columns
        for name in (
            "wall_id", "entry_trigger_direction", "entry_armed",
            "entry_fill_price", "accounting_version",
        ):
            self.assertIn(name, columns)
        self.assertEqual(columns["accounting_version"].server_default.arg.text, "1")


if __name__ == "__main__":
    unittest.main()
