import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from script.engine import TradingEngine
from script.historical_levels import calculate_horizontal_levels
from app.runtime import RuntimeController
from script.settings import PairSettings, TradingSettings
from script.strategies import (
    BOUNCE_STRATEGY_ID, BREAKOUT_STRATEGY_ID, COMBINED_STRATEGY_ID,
    FALSE_BREAKOUT_PENDING_TIMEOUT_SECONDS, FALSE_BREAKOUT_STRATEGY_ID,
    BounceStrategy, BreakoutStrategy, CombinedStrategy, FalseBreakoutStrategy,
    StrategyContext,
)
from script.trade_manager import TradeManager


def run(coro):
    try:
        coro.send(None)
    except StopIteration as result:
        return result.value
    finally:
        coro.close()
    raise AssertionError("test coroutine unexpectedly blocked")


def make_context(*, strategy_settings=None, side="sell", price=100.0,
                 levels=None, tick_size=0.01, price_updated=True, trades=()):
    settings = strategy_settings or PairSettings(
        "ETHUSDT", max_wall_distance_bps=1000,
        max_wall_level_distance_bps=13,
    )
    wall = {
        "side": side, "price": 100.0, "first_seen_at": "fixed-wall",
        "current_size": 5000.0, "initial_size": 5000.0,
        "wall_ratio": 900.0, "age": 60.0,
    }
    return StrategyContext(
        symbol="ETHUSDT", last_price=price, stable_walls=(wall,), wall_events=(),
        settings=settings, price_updated=price_updated,
        historical_levels=tuple(levels or ({
            "price": 100.0, "type": "RESISTANCE" if side == "sell" else "SUPPORT",
            "timeframe": "5m", "touch_count": 2,
        },)),
        trade_states=tuple(trades), tick_size=tick_size,
    )


class HistoricalLevelAdapterTests(unittest.TestCase):
    def test_trendln_values_are_passed_through_without_recounting(self):
        fake = SimpleNamespace(get_horizontal_levels=Mock(return_value=(
            [(99.5, 1, [2])], [(101.0, 3, [1, 4, 7])],
        )))
        candles = [{"low": 99.5, "high": 101.0}] * 5
        with patch("script.historical_levels.importlib.import_module", return_value=fake):
            levels = calculate_horizontal_levels(candles, "5m")
        self.assertEqual(levels, (
            {"price": 99.5, "type": "SUPPORT", "timeframe": "5m", "touch_count": 1},
            {"price": 101.0, "type": "RESISTANCE", "timeframe": "5m", "touch_count": 3},
        ))
        fake.get_horizontal_levels.assert_called_once()
        self.assertEqual(fake.get_horizontal_levels.call_args.kwargs, {
            "pctbound": 0.05,
            "min_touches": 1,
            "extmethod": "METHOD_NUMDIFF",
            "accuracy": 2,
        })

    def test_pair_trendln_parameters_are_validated_and_independent(self):
        btc = PairSettings("BTCUSDT")
        eth = PairSettings(
            "ETHUSDT", historical_level_pctbound=0.005,
            historical_level_min_touches=3,
            historical_level_extmethod="METHOD_NAIVE",
            historical_level_accuracy=4,
        )
        self.assertEqual(
            (btc.historical_level_pctbound, btc.historical_level_min_touches,
             btc.historical_level_extmethod, btc.historical_level_accuracy),
            (0.05, 1, "METHOD_NUMDIFF", 2),
        )
        self.assertEqual(
            (eth.historical_level_pctbound, eth.historical_level_min_touches,
             eth.historical_level_extmethod, eth.historical_level_accuracy),
            (0.005, 3, "METHOD_NAIVE", 4),
        )
        invalid = (
            {"historical_level_pctbound": 0},
            {"historical_level_min_touches": True},
            {"historical_level_extmethod": "METHOD_NCUBED"},
            {"historical_level_accuracy": 3},
        )
        for values in invalid:
            with self.subTest(values=values), self.assertRaises(ValueError):
                PairSettings("BTCUSDT", **values)

    def test_trendln_snapshot_records_exact_applied_settings_and_candle_count(self):
        from script import historical_levels

        settings = PairSettings(
            "ETHUSDT", historical_level_pctbound=0.01,
            historical_level_min_touches=2,
            historical_level_extmethod="METHOD_NAIVECONSEC",
            historical_level_accuracy=4,
        )
        candles = [{"low": 99.0, "high": 101.0}] * 17
        levels = ({"price": 100.0, "type": "SUPPORT", "touch_count": 2},)
        with (
            patch.object(historical_levels, "fetch_closed_bybit_candles", new=AsyncMock(return_value=candles)),
            patch.object(historical_levels.asyncio, "to_thread", new=AsyncMock(return_value=levels)) as to_thread,
        ):
            snapshot = run(historical_levels.fetch_historical_level_snapshot("ETHUSDT", settings))
        self.assertEqual(snapshot["levels"], levels)
        self.assertEqual(snapshot["candles_analyzed"], 17)
        self.assertEqual(snapshot["settings"]["historical_level_pctbound"], 0.01)
        self.assertEqual(snapshot["settings"]["historical_level_min_touches"], 2)
        self.assertEqual(snapshot["settings"]["historical_level_extmethod"], "METHOD_NAIVECONSEC")
        self.assertEqual(snapshot["settings"]["historical_level_accuracy"], 4)
        self.assertEqual(to_thread.call_args.kwargs["accuracy"], 4)

    def test_wall_requires_matching_level_type_and_distance(self):
        strategy = BreakoutStrategy()
        context = make_context(levels=(
            {"price": 100.0, "type": "SUPPORT", "timeframe": "5m", "touch_count": 4},
        ))
        signals, _ = strategy.evaluate_with_diagnostics(context)
        self.assertEqual(signals, [])
        context = make_context(levels=(
            {"price": 100.3, "type": "RESISTANCE", "timeframe": "5m", "touch_count": 4},
        ))
        signals, _ = strategy.evaluate_with_diagnostics(context)
        self.assertEqual(signals, [])


class StrategyLevelFilterTests(unittest.TestCase):
    def test_bounce_accepts_one_touch_and_breakout_requires_two(self):
        one_touch = make_context(price=99.5, levels=(
            {"price": 100.0, "type": "RESISTANCE", "timeframe": "5m", "touch_count": 1},
        ))
        two_touches = make_context(price=99.5, levels=(
            {"price": 100.0, "type": "RESISTANCE", "timeframe": "5m", "touch_count": 2},
        ))
        self.assertEqual(len(BounceStrategy().evaluate(one_touch)), 1)
        self.assertEqual(BounceStrategy().evaluate(two_touches), [])
        self.assertEqual(BreakoutStrategy().evaluate(one_touch), [])
        self.assertEqual(len(BreakoutStrategy().evaluate(two_touches)), 1)

    def test_combined_chooses_exactly_one_policy_from_touch_count(self):
        strategy = CombinedStrategy()
        one_touch = make_context(price=99.5, levels=(
            {"price": 100.0, "type": "RESISTANCE", "timeframe": "5m", "touch_count": 1},
        ))
        two_touches = make_context(price=99.5, levels=(
            {"price": 100.0, "type": "RESISTANCE", "timeframe": "5m", "touch_count": 2},
        ))
        bounce_signal, = strategy.evaluate(one_touch)
        breakout_signal, = strategy.evaluate(two_touches)
        self.assertEqual(bounce_signal.level_policy_strategy_id, BOUNCE_STRATEGY_ID)
        self.assertEqual(breakout_signal.level_policy_strategy_id, BREAKOUT_STRATEGY_ID)


class FalseBreakoutTests(unittest.TestCase):
    def configured_settings(self):
        return PairSettings(
            "ETHUSDT", max_wall_distance_bps=1000,
            max_wall_level_distance_bps=13,
            false_breakout_point_1_offset_bps=5,
            false_breakout_point_2_distance_bps=10,
            false_breakout_observation_timeout_seconds=300,
            false_breakout_take_profit_bps=20,
            false_breakout_stop_loss_bps=15,
        )

    def test_unset_observation_parameters_fail_closed(self):
        strategy = FalseBreakoutStrategy()
        signals, _ = strategy.evaluate_with_diagnostics(make_context())
        self.assertEqual(signals, [])
        self.assertEqual(strategy.diagnostics("ETHUSDT")["observing"], 0)

    def test_two_points_create_reverse_pending_only_at_point_two(self):
        strategy = FalseBreakoutStrategy()
        settings = self.configured_settings()
        first = make_context(strategy_settings=settings, price=100.0, price_updated=True)
        self.assertEqual(strategy.evaluate(first), [])
        point1 = make_context(strategy_settings=settings, price=100.06)
        self.assertEqual(strategy.evaluate(point1), [])
        self.assertEqual(strategy.diagnostics("ETHUSDT")["point_1_reached"], 1)
        point2 = make_context(strategy_settings=settings, price=100.16)
        signal, = strategy.evaluate(point2)
        self.assertEqual(signal.strategy_id, FALSE_BREAKOUT_STRATEGY_ID)
        self.assertEqual(signal.side, "short")
        self.assertEqual(signal.entry_trigger_direction, "cross_down")
        self.assertAlmostEqual(signal.entry_price, 100.05)

    def test_point2_pending_opens_only_on_subsequent_reverse_cross_at_last_price(self):
        settings = self.configured_settings()
        engine = TradingEngine(
            {"ETHUSDT": settings},
            TradingSettings(stake_usdt=10, initial_balance_usdt=1000, max_open_trades=1),
            enabled_strategies={"ETHUSDT": {FALSE_BREAKOUT_STRATEGY_ID}},
        )
        engine.instrument_tick_sizes["ETHUSDT"] = 0.01
        engine.set_historical_levels("ETHUSDT", (
            {"price": 100.0, "type": "RESISTANCE", "timeframe": "5m", "touch_count": 1},
        ))
        engine.start()
        wall = dict(make_context(strategy_settings=settings).stable_walls[0])
        engine.on_market_update("ETHUSDT", 100.0, [wall], (), [wall], price_updated=True)
        engine.on_market_update("ETHUSDT", 100.06, [wall], (), [wall])
        events = engine.on_market_update("ETHUSDT", 100.16, [wall], (), [wall])
        pending, = engine.trade_manager.get_pending_trades()
        self.assertEqual([e["event"] for e in events if e["event"] == "TRADE_CREATED"], ["TRADE_CREATED"])
        self.assertEqual(pending["side"], "short")
        self.assertEqual(pending["status"], "pending")
        eligible = {pending["signal_id"]}
        events = engine.trade_manager.on_price(
            "ETHUSDT", 100.10, process_open=False, eligible_signal_ids=eligible,
        )
        self.assertFalse(any(e["event"] == "ENTRY_ACTIVATED" for e in events))
        events = engine.trade_manager.on_price(
            "ETHUSDT", 100.04, process_open=False, eligible_signal_ids=eligible,
        )
        opened, = engine.trade_manager.get_open_trades()
        self.assertTrue(any(e["event"] == "ENTRY_ACTIVATED" for e in events))
        self.assertEqual(opened["entry_fill_price"], 100.04)
        from decimal import Decimal

        self.assertEqual(Decimal(str(opened["take_profit"])) % Decimal("0.01"), 0)
        self.assertEqual(Decimal(str(opened["stop_loss"])) % Decimal("0.01"), 0)
        self.assertLess(opened["take_profit"], opened["entry_fill_price"])
        self.assertGreater(opened["stop_loss"], opened["entry_fill_price"])

    def test_persisted_scenario_restores_without_inventing_price_crossing(self):
        settings = self.configured_settings()
        first = FalseBreakoutStrategy()
        first.evaluate(make_context(strategy_settings=settings, price=100.0, price_updated=False))
        exported = first.export_symbol_state("ETHUSDT")
        restored = FalseBreakoutStrategy()
        restored.restore_symbol_state("ETHUSDT", exported)
        signals = restored.evaluate(make_context(strategy_settings=settings, price=100.16))
        self.assertEqual(signals, [])
        self.assertEqual(restored.diagnostics("ETHUSDT")["point_1_reached"], 0)

    def test_engine_persists_scenario_state_atomically_even_without_trade_event(self):
        class Repository:
            def __init__(self):
                self.saved = None

            async def save_trades_with_strategy_states(self, trades, balance, mode, states):
                self.saved = (trades, balance, mode, states)

            async def save_market_price(self, symbol, price):
                return None

        settings = self.configured_settings()
        repository = Repository()
        engine = TradingEngine(
            {"ETHUSDT": settings}, enabled_strategies={"ETHUSDT": {FALSE_BREAKOUT_STRATEGY_ID}},
            repository=repository,
        )
        wall = dict(make_context(strategy_settings=settings).stable_walls[0])
        engine.instrument_tick_sizes["ETHUSDT"] = 0.01
        strategy = next(
            strategy for strategy in engine.strategy_registry.get_enabled()
            if strategy.strategy_id == FALSE_BREAKOUT_STRATEGY_ID
        )
        strategy.evaluate(make_context(
            strategy_settings=settings, price=100.0, price_updated=False,
        ))
        states = {"ETHUSDT": engine.export_strategy_states("ETHUSDT")}
        run(engine.persist_events([], strategy_states=states))
        self.assertIsNotNone(repository.saved)
        self.assertIn("false_breakout", repository.saved[3]["ETHUSDT"])
        restored = TradingEngine(
            {"ETHUSDT": settings}, enabled_strategies={"ETHUSDT": {FALSE_BREAKOUT_STRATEGY_ID}},
            strategy_states={"ETHUSDT": repository.saved[3]["ETHUSDT"]},
        )
        strategy = next(s for s in restored.strategy_registry.get_enabled() if s.strategy_id == FALSE_BREAKOUT_STRATEGY_ID)
        self.assertEqual(strategy.diagnostics("ETHUSDT")["observing"], 1)


class FalseBreakoutTimeoutTests(unittest.TestCase):
    def make_pending(self, created_at):
        manager = TradeManager(TradingSettings(stake_usdt=10, initial_balance_usdt=1000))
        settings = PairSettings("ETHUSDT")
        trade = manager.create_trade({
            "signal_id": "fb-timeout", "strategy_id": FALSE_BREAKOUT_STRATEGY_ID,
            "symbol": "ETHUSDT", "wall_side": "sell", "wall_price": 100,
            "wall_id": "ETHUSDT:sell:100:timeout", "wall_size": 1000,
            "wall_ratio": 900, "side": "short", "entry_price": 100.05,
            "stop_loss": 101.0, "take_profit": 99.0,
            "entry_trigger_direction": "cross_down", "reference_price": 100.2,
            "take_profit_bps": 20, "stop_loss_bps": 15, "tick_size": 0.01,
        }, settings)
        trade["created_at"] = created_at
        return manager, trade

    def test_fixed_timeout_is_900_seconds_and_pending_survives_until_expiry(self):
        self.assertEqual(FALSE_BREAKOUT_PENDING_TIMEOUT_SECONDS, 900)
        now = datetime.now(timezone.utc)
        manager, trade = self.make_pending(now - timedelta(seconds=899))
        self.assertEqual(manager.cancel_expired_false_breakout_pending(now), [])
        events = manager.cancel_expired_false_breakout_pending(now + timedelta(seconds=1))
        self.assertEqual(len(events), 1)
        self.assertEqual(trade["status"], "cancelled")
        self.assertEqual(trade["close_reason"], "FALSE_BREAKOUT_ENTRY_TIMEOUT")

    def test_open_trade_is_never_cancelled_by_pending_timeout(self):
        now = datetime.now(timezone.utc)
        manager, trade = self.make_pending(now - timedelta(seconds=1000))
        trade["status"] = "open"
        self.assertEqual(manager.cancel_expired_false_breakout_pending(now), [])
        self.assertEqual(trade["status"], "open")

    def test_restart_uses_persisted_created_at_and_cancelled_plan_cannot_reopen(self):
        now = datetime.now(timezone.utc)
        manager, trade = self.make_pending(now - timedelta(seconds=901))
        restored = TradeManager(manager.trading_settings)
        restored.restore_state([trade], 1000)
        events = restored.cancel_expired_false_breakout_pending(now)
        self.assertEqual(events[0]["trade"]["close_reason"], "FALSE_BREAKOUT_ENTRY_TIMEOUT")
        restored.on_price("ETHUSDT", 100.04)
        restored.on_price("ETHUSDT", 100.2)
        self.assertEqual(restored.get_open_trades(), [])
        self.assertEqual(restored.get_pending_trades(), [])

    def test_expiry_persists_cancel_and_rolls_back_on_database_error(self):
        class Repository:
            def __init__(self, fail=False):
                self.fail = fail
                self.saved = []

            async def save_trades(self, trades, balance, engine_mode=None):
                if self.fail:
                    raise OSError("db down")
                self.saved.extend(trades)

        now = datetime.now(timezone.utc)
        manager, trade = self.make_pending(now - timedelta(seconds=901))
        repo = Repository()
        engine = TradingEngine({"ETHUSDT": PairSettings("ETHUSDT")}, trade_manager=manager, repository=repo)
        self.assertEqual(run(engine.expire_false_breakout_pending()), 1)
        self.assertEqual(repo.saved[-1]["close_reason"], "FALSE_BREAKOUT_ENTRY_TIMEOUT")

        manager2, trade2 = self.make_pending(now - timedelta(seconds=901))
        failed_engine = TradingEngine(
            {"ETHUSDT": PairSettings("ETHUSDT")}, trade_manager=manager2,
            repository=Repository(fail=True),
        )
        self.assertEqual(run(failed_engine.expire_false_breakout_pending()), 0)
        self.assertEqual(manager2.find_trade("fb-timeout")["status"], "pending")


class StrategySelectionTests(unittest.TestCase):
    def test_multiple_strategies_can_be_enabled_for_one_pair(self):
        engine = TradingEngine({"ETHUSDT": PairSettings("ETHUSDT")})
        runtime = RuntimeController(engine)
        selected = run(runtime.update_enabled_strategies(
            "ETHUSDT", {BOUNCE_STRATEGY_ID, COMBINED_STRATEGY_ID, FALSE_BREAKOUT_STRATEGY_ID}
        ))
        self.assertEqual(selected, {BOUNCE_STRATEGY_ID, COMBINED_STRATEGY_ID, FALSE_BREAKOUT_STRATEGY_ID})


if __name__ == "__main__":
    unittest.main()
