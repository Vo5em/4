"""Deterministic interaction checks using the production strategy/manager code."""

import unittest

from script.settings import PairSettings, TradingSettings
from script.engine import EngineMode, TradingEngine
from script.strategies import CombinedStrategy, FalseBreakoutStrategy, StrategyContext
from script.trade_levels import calculate_trade_levels
from script.trade_manager import TradeManager


SYMBOL = "ETHUSDT"
WALL = {"side": "sell", "price": 1000.0, "first_seen_at": "fixed-wall",
        "current_size": 100_000.0, "wall_ratio": 900.0, "age": 60.0}
LEVEL = {"price": 1000.0, "type": "RESISTANCE", "timeframe": "5m", "touch_count": 3}


def settings(**overrides):
    values = dict(max_wall_distance_bps=100, max_wall_level_distance_bps=13,
                  false_breakout_point_1_offset_bps=3,
                  false_breakout_point_2_distance_bps=3,
                  false_breakout_observation_timeout_seconds=600)
    values.update(overrides)
    return PairSettings(SYMBOL, **values)


def context(price, pair_settings=None, *, wall=WALL, touch_count=3, price_updated=True):
    level = {**LEVEL, "touch_count": touch_count}
    return StrategyContext(
        symbol=SYMBOL, last_price=price,
        stable_walls=(wall,) if wall is not None else (), wall_events=(),
        settings=pair_settings or settings(), price_updated=price_updated,
        historical_levels=(level,), tick_size=0.01,
    )


def create_wall_pending(manager, signal, pair_settings, reference_price):
    levels = calculate_trade_levels(
        strategy_id=signal.level_policy_strategy_id or signal.strategy_id,
        wall_side=signal.wall_side, wall_price=signal.wall_price, tick_size=0.01,
        take_profit_bps=5, stop_loss_bps=5,
    )
    payload = {
        **signal.__dict__, **levels, "reference_price": reference_price,
        "signal_id": f"test:{signal.strategy_id}:{signal.wall_id}",
    }
    return manager.create_trade(payload, pair_settings, stake_usdt=10)


class CombinedFalseBreakoutInteractionTests(unittest.TestCase):
    def test_ask_combined_breakout_pending_opens_at_pre_wall_entry_before_point_1(self):
        pair = settings()
        strategy = CombinedStrategy()
        signals = strategy.evaluate(context(999.80, pair))
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].level_policy_strategy_id, "breakout")
        manager = TradeManager(TradingSettings())
        manager.last_prices[SYMBOL] = 999.80
        pending = create_wall_pending(manager, signals[0], pair, 999.80)
        self.assertIsNotNone(pending)
        entry = pending["entry_price"]
        self.assertLess(entry, WALL["price"])
        self.assertGreater(1000.30, WALL["price"] * 1.00015)  # False P1 is beyond the wall.
        events = manager.on_price(SYMBOL, entry)
        self.assertIn("ENTRY_ACTIVATED", [event["event"] for event in events])

    def test_false_breakout_pending_is_emitted_at_point_2_and_opens_on_return_to_point_1(self):
        pair = settings()
        strategy = FalseBreakoutStrategy()
        strategy.evaluate(context(999.80, pair))  # Creates OBSERVING only.
        p1 = 1000.30
        p2 = 1000.61
        strategy.evaluate(context(p1, pair))
        signals = strategy.evaluate(context(p2, pair))
        self.assertEqual(len(signals), 1)
        signal = signals[0]
        self.assertEqual(signal.false_breakout_phase, "PENDING")
        self.assertEqual(signal.entry_price, p1)
        self.assertEqual(signal.entry_trigger_direction, "cross_down")
        manager = TradeManager(TradingSettings())
        manager.last_prices[SYMBOL] = p2
        pending = manager.create_trade(signal, pair, stake_usdt=10)
        self.assertIsNotNone(pending)
        self.assertTrue(pending["entry_armed"])
        events = manager.on_price(SYMBOL, p1)
        self.assertIn("ENTRY_ACTIVATED", [event["event"] for event in events])

    def test_false_breakout_scenario_continues_after_wall_disappears(self):
        pair = settings()
        strategy = FalseBreakoutStrategy()
        strategy.evaluate(context(999.80, pair))
        strategy.evaluate(context(1000.30, pair, wall=None))
        signals = strategy.evaluate(context(1000.61, pair, wall=None))
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].false_breakout_phase, "PENDING")

    def test_combined_and_false_breakout_have_strategy_scoped_plans_and_open_limit_blocks_second(self):
        pair = settings()
        combined_signal = CombinedStrategy().evaluate(context(999.80, pair))[0]
        manager = TradeManager(TradingSettings())
        manager.last_prices[SYMBOL] = 999.80
        combined_pending = create_wall_pending(manager, combined_signal, pair, 999.80)
        self.assertEqual(combined_pending["status"], "pending")
        ordinary_entry = combined_pending["entry_price"]
        ordinary_events = manager.on_price(SYMBOL, ordinary_entry)
        self.assertIn("ENTRY_ACTIVATED", [event["event"] for event in ordinary_events])

        false_strategy = FalseBreakoutStrategy()
        false_strategy.evaluate(context(999.80, pair))
        false_strategy.evaluate(context(1000.30, pair))
        false_signal = false_strategy.evaluate(context(1000.61, pair))[0]
        # The market has since reached Point 2; no intermediate open-price
        # ticks are simulated here because they would also exercise TP.
        manager.last_prices[SYMBOL] = 1000.61
        false_pending = manager.create_trade(false_signal, pair, stake_usdt=10)
        self.assertEqual(false_pending["status"], "pending")
        self.assertNotEqual(combined_pending["strategy_id"], false_pending["strategy_id"])

        # False Breakout returns down across Point 1, but the pair already has
        # its one permitted OPEN position from Combined.
        events = manager.on_price(SYMBOL, 1000.30)
        self.assertIn("ENTRY_CANCELLED", [event["event"] for event in events])
        cancelled = next(event["trade"] for event in events if event["event"] == "ENTRY_CANCELLED")
        self.assertEqual(cancelled["close_reason"], "ENTRY_BLOCKED_BY_OPEN_TRADE")
        self.assertEqual(sum(t["status"] == "open" for t in manager.trades), 1)

    def test_combined_selects_bounce_for_one_trendln_touch(self):
        pair = settings()
        signals, traces = CombinedStrategy().evaluate_with_diagnostics(
            context(999.80, pair, touch_count=1)
        )
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].level_policy_strategy_id, "bounce")
        self.assertEqual(signals[0].side, "short")  # ASK -> Bounce SHORT.
        self.assertEqual(signals[0].entry_trigger_direction, "cross_up")
        self.assertEqual(traces[0]["combined_choice"], "bounce")

    def test_bid_false_breakout_is_mirrored(self):
        pair = settings()
        bid_wall = {**WALL, "side": "buy", "price": 1000.0}
        support = {**LEVEL, "type": "SUPPORT"}
        strat = FalseBreakoutStrategy()
        def bid_context(price):
            return StrategyContext(SYMBOL, price, (bid_wall,), (), pair,
                                  historical_levels=(support,), tick_size=0.01)
        strat.evaluate(bid_context(1000.20))
        strat.evaluate(bid_context(999.70))
        signals = strat.evaluate(bid_context(999.40))
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].side, "long")
        self.assertEqual(signals[0].entry_trigger_direction, "cross_up")

    def test_bid_combined_breakout_opens_short_on_downward_entry_cross(self):
        pair = settings()
        bid_wall = {**WALL, "side": "buy", "price": 1000.0}
        support = {**LEVEL, "type": "SUPPORT"}
        ctx = StrategyContext(SYMBOL, 1000.20, (bid_wall,), (), pair,
                              historical_levels=(support,), tick_size=0.01)
        signal, = CombinedStrategy().evaluate(ctx)
        self.assertEqual(signal.level_policy_strategy_id, "breakout")
        self.assertEqual(signal.side, "short")
        self.assertEqual(signal.entry_trigger_direction, "cross_down")
        manager = TradeManager(TradingSettings())
        manager.last_prices[SYMBOL] = 1000.20
        trade = create_wall_pending(manager, signal, pair, 1000.20)
        self.assertAlmostEqual(trade["entry_price"], 1000.15)
        self.assertIn("ENTRY_ACTIVATED", [
            item["event"] for item in manager.on_price(SYMBOL, trade["entry_price"])
        ])

    def test_complete_scanner_update_can_cancel_pending_before_later_ticker_crossing(self):
        """Orderbook reconciliation and ticker-entry processing are separate events."""
        pair = settings()
        combined_signal = CombinedStrategy().evaluate(context(999.80, pair))[0]
        manager = TradeManager(TradingSettings())
        manager.last_prices[SYMBOL] = 999.80
        pending = create_wall_pending(manager, combined_signal, pair, 999.80)
        engine = TradingEngine(
            {SYMBOL: pair}, enabled_strategies={SYMBOL: {"combined"}},
            trade_manager=manager,
        )
        engine.mode = EngineMode.RUNNING
        unrelated_stable = {"side": "sell", "price": 1001.0,
                             "first_seen_at": "other-wall", "age": 80,
                             "current_size": 100_000, "wall_ratio": 900}
        events = engine.on_market_update(
            SYMBOL, 999.80, [unrelated_stable], (), [unrelated_stable],
            price_updated=False,
        )
        cancel = next(event for event in events if event["event"] == "ENTRY_CANCELLED")
        self.assertEqual(cancel["trade"]["signal_id"], pending["signal_id"])
        self.assertEqual(cancel["trade"]["close_reason"], "WALL_NO_LONGER_QUALIFIES")
        crossed = manager.on_price(SYMBOL, pending["entry_price"])
        self.assertNotIn("ENTRY_ACTIVATED", [event["event"] for event in crossed])

    def test_ticker_crossing_first_can_open_same_prior_pending(self):
        pair = settings()
        signal = CombinedStrategy().evaluate(context(999.80, pair))[0]
        manager = TradeManager(TradingSettings())
        manager.last_prices[SYMBOL] = 999.80
        pending = create_wall_pending(manager, signal, pair, 999.80)
        events = manager.on_price(SYMBOL, pending["entry_price"])
        self.assertIn("ENTRY_ACTIVATED", [event["event"] for event in events])


if __name__ == "__main__":
    unittest.main()
