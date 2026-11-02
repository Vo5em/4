import unittest

from database.models import VirtualTrade
from script.engine import EngineMode, TradingEngine
from script.settings import PairSettings, TradingSettings
from script.strategies import BOUNCE_STRATEGY_ID, BREAKOUT_STRATEGY_ID
from script.trade_levels import calculate_trade_levels
from script.trade_manager import TradeManager


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
                self.assertEqual(
                    abs(levels["take_profit"] - levels["entry_price"]),
                    abs(levels["entry_price"] - levels["stop_loss"]),
                )

    def test_bounce_bid_matches_approved_unrounded_formula_before_tick_rounding(self):
        levels = calculate_trade_levels(
            strategy_id=BOUNCE_STRATEGY_ID,
            wall_side="buy",
            wall_price="2284.80",
            tick_size="0.00001",
        )
        self.assertAlmostEqual(float(levels["entry_price"]), 2285.14272, places=5)
        self.assertAlmostEqual(float(levels["stop_loss"]), 2284.34304, places=5)
        self.assertAlmostEqual(float(levels["take_profit"]), 2285.94240, places=5)


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
        self.assertEqual(engine.trade_manager.get_statistics()["reserved_margin_usdt"], 60)

    def test_nearest_wall_replaces_plan_and_unchanged_plan_is_not_recreated(self):
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
        self.assertEqual(len(current), 1)
        self.assertEqual(current[0]["wall_id"], "ETHUSDT:buy:100:near")
        self.assertEqual(first["status"], "cancelled")
        self.assertEqual(first["close_reason"], "PLAN_REPLACED")

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

    def test_open_only_limits_block_entry_and_require_safe_return_and_recross(self):
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
        # PENDINGs reserve margin, but do not consume OPEN slots.
        self.assertEqual(len(manager.get_pending_trades()), 2)
        self.assertEqual(manager.on_price("BTCUSDT", 100.0)[0]["event"], "ENTRY_ACTIVATED")
        # The second plan cannot open while the only slot is occupied.
        blocked = manager.on_price("BTCUSDT", 100.03)
        self.assertTrue(any(event["event"] == "ENTRY_BLOCKED" for event in blocked))
        self.assertEqual(second["status"], "pending")
        self.assertFalse(second["entry_armed"])
        manager._close_trade(first, 100.0, "test")
        manager.on_price("BTCUSDT", 100.0)  # safe side rearms it
        self.assertEqual(second["status"], "pending")
        manager.on_price("BTCUSDT", 100.03)  # fresh ASK cross upward
        self.assertEqual(second["status"], "open")

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
        for name in ("wall_id", "entry_trigger_direction", "entry_armed", "entry_fill_price"):
            self.assertIn(name, columns)


if __name__ == "__main__":
    unittest.main()
