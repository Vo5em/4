import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

os.environ.setdefault(
    "DATABASE_URL", "postgresql+asyncpg://127.0.0.1/crypto_bot_test"
)

from database import request as repository
from config import validate_database_url
from database.models import (
    MarketPrice,
    PairStrategyConfig,
    RuntimeState,
    StrategyChangeEvent,
    TradingPairSetting,
    VirtualTrade,
)
from script.settings import PairSettings, TradingSettings


def run_immediate(coroutine):
    try:
        coroutine.send(None)
    except StopIteration as completed:
        return completed.value
    finally:
        coroutine.close()
    raise AssertionError("Coroutine suspended; test session must be immediate")


class FakeResult:
    def __init__(self, values):
        self.values = values

    def all(self):
        return list(self.values)


class FakeSession:
    def __init__(self, rows=None):
        self.rows = {model: list(values) for model, values in (rows or {}).items()}
        self.commits = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    async def get(self, model, key):
        for row in self.rows.get(model, []):
            if model is RuntimeState and row.id == key:
                return row
            if model in {TradingPairSetting, MarketPrice, PairStrategyConfig} and row.symbol == key:
                return row
        return None

    async def scalar(self, statement):
        return None

    async def scalars(self, statement):
        model = statement.column_descriptions[0]["entity"]
        return FakeResult(self.rows.get(model, []))

    def add(self, row):
        self.rows.setdefault(type(row), []).append(row)

    async def commit(self):
        self.commits += 1


class DatabaseRepositoryTests(unittest.TestCase):
    def test_database_url_requires_postgresql_asyncpg_without_exposing_values(self):
        self.assertEqual(
            validate_database_url(
                " postgresql+asyncpg://user@localhost/crypto_bot "
            ),
            "postgresql+asyncpg://user@localhost/crypto_bot",
        )
        with self.assertRaisesRegex(RuntimeError, r"postgresql\+asyncpg"):
            validate_database_url("postgresql://user@localhost/crypto_bot")
        with self.assertRaisesRegex(RuntimeError, "Set DATABASE_URL"):
            validate_database_url(" ")

    def test_initialize_creates_defaults_and_returns_recoverable_state(self):
        session = FakeSession()

        async def create_tables():
            return None

        with (
            patch.object(repository, "async_main", create_tables),
            patch.object(repository, "read_legacy_symbol_rows", return_value=[]),
            patch.object(repository, "async_session", return_value=session),
        ):
            state = run_immediate(
                repository.initialize_database(
                    TradingSettings(stake_usdt=15, initial_balance_usdt=500),
                    {"BTCUSDT": PairSettings("BTCUSDT")},
                )
            )

        self.assertEqual(state["trading_settings"].stake_usdt, 15)
        self.assertEqual(state["balance_usdt"], 500)
        self.assertFalse(state["initialized"])
        self.assertEqual(set(state["pair_settings"]), {"BTCUSDT"})
        self.assertEqual(state["enabled_strategies"], {"BTCUSDT": {"stable_wall_v1"}})
        self.assertEqual(state["trades"], [])
        self.assertEqual(state["last_prices"], {})
        self.assertGreaterEqual(session.commits, 1)

    def test_initialize_restores_trade_state_price_and_explicit_empty_strategy_set(self):
        runtime_row = RuntimeState(
            id=1,
            balance_usdt=125,
            initial_balance_usdt=100,
            stake_usdt=10,
            max_open_trades=3,
            engine_mode="running",
            initialized=True,
        )
        pair_row = TradingPairSetting(
            symbol="BTCUSDT",
            enabled=True,
            min_wall_ratio=888,
            min_wall_notional_usdt=0,
            max_wall_distance_bps=100,
            min_wall_age_seconds=20,
            consumed_ratio=0.9,
            entry_distance_bps=1.5,
            stop_distance_bps=2,
            max_open_trades=1,
        )
        strategy_row = PairStrategyConfig(
            symbol="BTCUSDT", enabled_strategy_ids=[]
        )
        market_price = MarketPrice(symbol="BTCUSDT", last_price=100.25)
        created_at = datetime.now(timezone.utc)
        trade = VirtualTrade(
            signal_id="BTCUSDT:buy:100:restore-test",
            strategy_id="stable_wall_v1",
            symbol="BTCUSDT",
            side="long",
            wall_side="buy",
            status="open",
            wall_price=100,
            wall_size=1000,
            wall_ratio=900,
            stake_usdt=10,
            entry_price=100.015,
            stop_loss=99.98,
            take_profit=100.05,
            exit_price=None,
            profit_usdt=0,
            profit_percent=0,
            close_reason=None,
            created_at=created_at,
            opened_at=created_at,
            closed_at=None,
        )
        session = FakeSession(
            {
                RuntimeState: [runtime_row],
                TradingPairSetting: [pair_row],
                PairStrategyConfig: [strategy_row],
                MarketPrice: [market_price],
                VirtualTrade: [trade],
            }
        )

        async def create_tables():
            return None

        with (
            patch.object(repository, "async_main", create_tables),
            patch.object(repository, "read_legacy_symbol_rows", return_value=[]),
            patch.object(repository, "async_session", return_value=session),
        ):
            state = run_immediate(
                repository.initialize_database(
                    TradingSettings(), {"BTCUSDT": PairSettings("BTCUSDT")}
                )
            )

        self.assertEqual(state["balance_usdt"], 125)
        self.assertTrue(state["initialized"])
        self.assertEqual(state["enabled_strategies"], {"BTCUSDT": set()})
        self.assertEqual(state["last_prices"], {"BTCUSDT": 100.25})
        self.assertEqual(len(state["trades"]), 1)
        self.assertEqual(state["trades"][0]["status"], "open")
        self.assertEqual(state["trades"][0]["signal_id"], trade.signal_id)

    def test_initialize_imports_legacy_bybit_pair_without_overwriting_existing(self):
        legacy_pair = TradingPairSetting(
            symbol="SOLUSDT",
            enabled=False,
            min_wall_ratio=888,
            min_wall_notional_usdt=0,
            max_wall_distance_bps=100,
            min_wall_age_seconds=20,
            consumed_ratio=0.9,
            entry_distance_bps=1.5,
            stop_distance_bps=2,
            max_open_trades=1,
        )
        session = FakeSession({TradingPairSetting: [legacy_pair]})

        async def create_tables():
            return None

        with (
            patch.object(repository, "async_main", create_tables),
            patch.object(
                repository,
                "read_legacy_symbol_rows",
                return_value=[
                    {"symbol": "solusdt", "exchange": "Bybit", "enabled": True},
                    {"symbol": "ETHUSDT", "exchange": "Binance", "enabled": True},
                    {"symbol": "BTC/USD", "exchange": "Bybit", "enabled": True},
                ],
            ),
            patch.object(repository, "async_session", return_value=session),
        ):
            state = run_immediate(
                repository.initialize_database(
                    TradingSettings(), {"BTCUSDT": PairSettings("BTCUSDT")}
                )
            )

        self.assertEqual(set(state["pair_settings"]), {"SOLUSDT", "BTCUSDT"})
        self.assertFalse(state["pair_settings"]["SOLUSDT"].enabled)

    def test_legacy_pair_conversion_accepts_only_bybit_linear_usdt_pairs(self):
        imported = repository._legacy_pair_settings(
            [
                {"symbol": " btcusdt ", "exchange": "bybit", "enabled": 1},
                {"symbol": "ETHUSDT", "exchange": "binance", "enabled": 1},
                {"symbol": "BTC/USD", "exchange": "bybit", "enabled": 1},
            ]
        )

        self.assertEqual([(item.symbol, item.enabled) for item in imported], [("BTCUSDT", True)])

    def test_save_market_price_inserts_normalized_symbol(self):
        session = FakeSession()
        with patch.object(repository, "async_session", return_value=session):
            run_immediate(repository.save_market_price("ethusdt", 2500.5))

        row = session.rows[MarketPrice][0]
        self.assertEqual(row.symbol, "ETHUSDT")
        self.assertEqual(row.last_price, 2500.5)
        self.assertEqual(session.commits, 1)

    def test_save_market_price_updates_existing_row_timestamp(self):
        old_time = datetime(2000, 1, 1, tzinfo=timezone.utc)
        row = MarketPrice(
            symbol="ETHUSDT", last_price=2500, observed_at=old_time
        )
        session = FakeSession({MarketPrice: [row]})
        with patch.object(repository, "async_session", return_value=session):
            run_immediate(repository.save_market_price("ETHUSDT", 2501.0))

        self.assertEqual(row.last_price, 2501.0)
        self.assertGreater(row.observed_at, old_time)

    def test_save_pair_and_global_settings(self):
        pair_row = TradingPairSetting(
            symbol="BTCUSDT",
            enabled=True,
            min_wall_ratio=888,
            min_wall_notional_usdt=0,
            max_wall_distance_bps=100,
            min_wall_age_seconds=20,
            consumed_ratio=0.9,
            entry_distance_bps=1.5,
            stop_distance_bps=2,
            max_open_trades=1,
        )
        runtime_row = RuntimeState(
            id=1,
            balance_usdt=100,
            initial_balance_usdt=100,
            stake_usdt=10,
            max_open_trades=3,
            engine_mode="running",
            initialized=True,
        )
        session = FakeSession(
            {TradingPairSetting: [pair_row], RuntimeState: [runtime_row]}
        )
        pair_settings = PairSettings(
            "BTCUSDT",
            enabled=False,
            min_wall_ratio=900,
            max_open_trades=2,
        )
        trading_settings = TradingSettings(
            stake_usdt=25, initial_balance_usdt=100, max_open_trades=5
        )

        with patch.object(repository, "async_session", return_value=session):
            run_immediate(repository.save_pair_settings(pair_settings))
            run_immediate(repository.save_trading_settings(trading_settings))

        self.assertFalse(pair_row.enabled)
        self.assertEqual(pair_row.min_wall_ratio, 900)
        self.assertEqual(pair_row.max_open_trades, 2)
        self.assertEqual(runtime_row.stake_usdt, 25)
        self.assertEqual(runtime_row.max_open_trades, 5)
        self.assertEqual(session.commits, 2)

    def test_save_enabled_strategies_inserts_and_updates(self):
        session = FakeSession()
        with patch.object(repository, "async_session", return_value=session):
            run_immediate(repository.save_enabled_strategies("btcusdt", {"stable_wall_v1"}))
        row = session.rows[PairStrategyConfig][0]
        self.assertEqual(row.symbol, "BTCUSDT")
        self.assertEqual(row.enabled_strategy_ids, ["stable_wall_v1"])
        with patch.object(repository, "async_session", return_value=session):
            run_immediate(repository.save_enabled_strategies("BTCUSDT", set()))
        self.assertEqual(row.enabled_strategy_ids, [])
        self.assertEqual(session.commits, 2)

    def test_strategy_selection_and_cancelled_trade_commit_together(self):
        runtime_state = RuntimeState(
            id=1,
            balance_usdt=100,
            initial_balance_usdt=100,
            stake_usdt=10,
            max_open_trades=3,
            engine_mode="running",
            initialized=True,
        )
        session = FakeSession({RuntimeState: [runtime_state]})
        trade = {
            "signal_id": "BTCUSDT:buy:100:strategy-off",
            "strategy_id": "stable_wall_v1",
            "symbol": "BTCUSDT",
            "side": "long",
            "wall_side": "buy",
            "status": "cancelled",
            "wall_price": 100,
            "wall_size": 1000,
            "wall_ratio": 900,
            "stake_usdt": 10,
            "entry_price": 100.015,
            "stop_loss": 99.98,
            "take_profit": 100.05,
            "profit_usdt": 0,
            "close_reason": "STRATEGY_DISABLED",
        }
        with patch.object(repository, "async_session", return_value=session):
            run_immediate(
                repository.save_enabled_strategies_with_trades(
                    "btcusdt", set(), [trade], 100, "running"
                )
            )

        self.assertEqual(
            session.rows[PairStrategyConfig][0].enabled_strategy_ids, []
        )
        self.assertEqual(session.rows[VirtualTrade][0].close_reason, "STRATEGY_DISABLED")
        self.assertEqual(runtime_state.balance_usdt, 100)
        self.assertEqual(session.commits, 1)

    def test_strategy_change_history_commits_admin_and_cancellation_reason(self):
        strategy_row = PairStrategyConfig(
            symbol="BTCUSDT", enabled_strategy_ids=["stable_wall_v1"]
        )
        runtime_state = RuntimeState(
            id=1, balance_usdt=100, initial_balance_usdt=100,
            stake_usdt=10, max_open_trades=3, engine_mode="running", initialized=True,
        )
        session = FakeSession({PairStrategyConfig: [strategy_row], RuntimeState: [runtime_state]})
        trade = {
            "signal_id": "legacy-pending", "strategy_id": "stable_wall_v1",
            "symbol": "BTCUSDT", "side": "long", "wall_side": "buy",
            "status": "cancelled", "wall_price": 100, "wall_size": 1000,
            "wall_ratio": 900, "stake_usdt": 10, "entry_price": 100.015,
            "stop_loss": 99.98, "take_profit": 100.05, "profit_usdt": 0,
            "close_reason": "STRATEGY_DISABLED",
        }
        with patch.object(repository, "async_session", return_value=session):
            run_immediate(repository.save_enabled_strategies_with_trades(
                "BTCUSDT", {"bounce"}, [trade], 100, "running", admin_user_id=7319
            ))
        event = session.rows[StrategyChangeEvent][0]
        self.assertEqual(event.previous_strategy, "stable_wall_v1")
        self.assertEqual(event.new_strategy, "bounce")
        self.assertEqual(event.admin_user_id, 7319)
        self.assertEqual(event.cancelled_plans, 1)
        self.assertEqual(event.cancellation_reasons, {"STRATEGY_DISABLED": 1})
        self.assertEqual(strategy_row.enabled_strategy_ids, ["bounce"])
        self.assertEqual(session.commits, 1)

    def test_strategy_selection_initializes_missing_pair_record_and_version(self):
        session = FakeSession()
        with patch.object(repository, "async_session", return_value=session):
            run_immediate(repository.save_enabled_strategies_with_trades(
                "BTCUSDT", {"breakout"}, [], 100, "running",
                admin_user_id=7319, expected_version=0,
            ))
        row = session.rows[PairStrategyConfig][0]
        self.assertEqual(row.enabled_strategy_ids, ["breakout"])
        self.assertEqual(row.version, 1)
        event = session.rows[StrategyChangeEvent][0]
        self.assertIsNone(event.previous_strategy)
        self.assertEqual(event.new_strategy, "breakout")
        self.assertEqual(event.admin_user_id, 7319)

    def test_strategy_repository_rejects_stale_version(self):
        row = PairStrategyConfig(
            symbol="BTCUSDT", enabled_strategy_ids=["bounce"], version=3
        )
        session = FakeSession({PairStrategyConfig: [row]})
        with patch.object(repository, "async_session", return_value=session):
            with self.assertRaisesRegex(RuntimeError, "configuration changed"):
                run_immediate(repository.save_enabled_strategies_with_trades(
                    "BTCUSDT", {"breakout"}, [], 100, "running",
                    admin_user_id=7319, expected_version=2,
                ))
        self.assertEqual(row.enabled_strategy_ids, ["bounce"])
        self.assertEqual(row.version, 3)
        self.assertEqual(session.commits, 0)

    def test_save_trade_updates_trade_row_and_runtime_balance(self):
        runtime_state = RuntimeState(
            id=1,
            balance_usdt=100,
            initial_balance_usdt=100,
            stake_usdt=10,
            max_open_trades=3,
            engine_mode="running",
            initialized=True,
        )
        session = FakeSession({RuntimeState: [runtime_state]})
        trade = {
            "signal_id": "BTCUSDT:buy:100:test",
            "strategy_id": "stable_wall_v1",
            "symbol": "BTCUSDT",
            "side": "long",
            "wall_side": "buy",
            "status": "closed",
            "wall_price": 100,
            "wall_size": 1000,
            "wall_ratio": 900,
            "stake_usdt": 10,
            "entry_price": 100.015,
            "stop_loss": 99.98,
            "take_profit": 100.05,
            "exit_price": 100.05,
            "profit_usdt": 0.0035,
            "profit_percent": 0.035,
            "close_reason": "TAKE_PROFIT",
        }
        with patch.object(repository, "async_session", return_value=session):
            run_immediate(
                repository.save_trades([trade], 100.0035, engine_mode="running")
            )

        saved = session.rows[VirtualTrade][0]
        self.assertEqual(saved.signal_id, trade["signal_id"])
        self.assertEqual(saved.status, "closed")
        self.assertEqual(saved.profit_usdt, 0.0035)
        self.assertEqual(runtime_state.balance_usdt, 100.0035)
        self.assertEqual(runtime_state.engine_mode, "running")
        self.assertEqual(session.commits, 1)

    def test_pair_settings_and_cancelled_trade_commit_together(self):
        runtime_state = RuntimeState(
            id=1,
            balance_usdt=100,
            initial_balance_usdt=100,
            stake_usdt=10,
            max_open_trades=3,
            engine_mode="running",
            initialized=True,
        )
        pair_row = TradingPairSetting(
            symbol="BTCUSDT",
            enabled=True,
            min_wall_ratio=888,
            min_wall_notional_usdt=0,
            max_wall_distance_bps=100,
            min_wall_age_seconds=20,
            consumed_ratio=0.9,
            entry_distance_bps=1.5,
            stop_distance_bps=2,
            max_open_trades=1,
        )
        session = FakeSession(
            {RuntimeState: [runtime_state], TradingPairSetting: [pair_row]}
        )
        trade = {
            "signal_id": "BTCUSDT:buy:100:test",
            "strategy_id": "stable_wall_v1",
            "symbol": "BTCUSDT",
            "side": "long",
            "wall_side": "buy",
            "status": "cancelled",
            "wall_price": 100,
            "wall_size": 1000,
            "wall_ratio": 900,
            "stake_usdt": 10,
            "entry_price": 100.015,
            "stop_loss": 99.98,
            "take_profit": 100.05,
            "profit_usdt": 0,
            "close_reason": "PAIR_DISABLED",
        }
        with patch.object(repository, "async_session", return_value=session):
            run_immediate(
                repository.save_pair_settings_with_trades(
                    PairSettings("BTCUSDT", enabled=False),
                    [trade],
                    100,
                    "running",
                )
            )

        self.assertFalse(pair_row.enabled)
        self.assertEqual(session.rows[VirtualTrade][0].status, "cancelled")
        self.assertEqual(runtime_state.balance_usdt, 100)
        self.assertEqual(session.commits, 1)


if __name__ == "__main__":
    unittest.main()
