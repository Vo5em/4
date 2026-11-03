import os
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault(
    "DATABASE_URL", "postgresql+asyncpg://127.0.0.1/crypto_bot_test"
)

from database import request as repository
from config import (
    CANCELLED_TRADE_CLEANUP_BATCH_SIZE,
    CANCELLED_TRADE_CLEANUP_INTERVAL_SECONDS,
    CANCELLED_TRADE_RETENTION_MINUTES,
    validate_database_url,
)
from database.models import (
    MarketPrice,
    ManualPlanBlock,
    PairStrategyConfig,
    RuntimeState,
    StrategyChangeEvent,
    TradingPairSetting,
    VirtualTrade,
)
from script.settings import PairSettings, TradingSettings
from script.trade_manager import TradeManager


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
        self.executed = []

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
            if model is ManualPlanBlock and row.signal_id == key:
                return row
        return None

    async def scalar(self, statement):
        return None

    async def scalars(self, statement):
        model = statement.column_descriptions[0]["entity"]
        return FakeResult(self.rows.get(model, []))

    def add(self, row):
        self.rows.setdefault(type(row), []).append(row)

    async def execute(self, statement):
        self.executed.append(statement)
        values = list(statement.compile().params.values())
        signal_ids = next((value for value in values if isinstance(value, (list, tuple, set))), ())
        if getattr(getattr(statement, "table", None), "name", None) == ManualPlanBlock.__tablename__:
            self.rows[ManualPlanBlock] = [
                row for row in self.rows.get(ManualPlanBlock, [])
                if row.signal_id not in set(signal_ids)
            ]

    async def commit(self):
        self.commits += 1


class DatabaseRepositoryTests(unittest.TestCase):
    def test_cancelled_cleanup_uses_one_hour_retention_and_minute_interval(self):
        if "CANCELLED_TRADE_RETENTION_MINUTES" not in os.environ:
            self.assertEqual(CANCELLED_TRADE_RETENTION_MINUTES, 60)
        if "CANCELLED_TRADE_CLEANUP_BATCH_SIZE" not in os.environ:
            self.assertEqual(CANCELLED_TRADE_CLEANUP_BATCH_SIZE, 500)
        self.assertEqual(CANCELLED_TRADE_CLEANUP_INTERVAL_SECONDS, 60)

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
        self.assertEqual(state["cancelled_trade_count"], 0)
        self.assertIsNone(state["last_cancelled_cleanup_at"])
        self.assertEqual(state["last_cancelled_cleanup_deleted"], 0)
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
            default_take_profit_bps=32,
            default_stop_loss_bps=24,
            cancelled_trade_count=0,
            last_cancelled_cleanup_at=datetime(2026, 11, 3, 10, 30, tzinfo=timezone.utc),
            last_cancelled_cleanup_deleted=9,
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
            leverage=17.5,
            take_profit_bps=48,
            stop_loss_bps=None,
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
                ManualPlanBlock: [ManualPlanBlock(
                    signal_id=trade.signal_id, symbol="BTCUSDT",
                    strategy_id="stable_wall_v1", wall_id="BTCUSDT:buy:100:restore-test",
                    wall_side="buy", wall_price=100,
                )],
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
        self.assertEqual(state["last_cancelled_cleanup_deleted"], 9)
        self.assertEqual(
            state["last_cancelled_cleanup_at"],
            datetime(2026, 11, 3, 10, 30, tzinfo=timezone.utc),
        )
        self.assertEqual(state["pair_settings"]["BTCUSDT"].leverage, 17.5)
        self.assertEqual(state["trading_settings"].default_take_profit_bps, 32)
        self.assertEqual(state["trading_settings"].default_stop_loss_bps, 24)
        self.assertEqual(state["pair_settings"]["BTCUSDT"].take_profit_bps, 48)
        self.assertIsNone(state["pair_settings"]["BTCUSDT"].stop_loss_bps)
        self.assertEqual(len(state["trades"]), 1)
        self.assertEqual(state["trades"][0]["status"], "open")
        self.assertEqual(state["manual_plan_blocks"][0]["signal_id"], trade.signal_id)
        manager = TradeManager(state["trading_settings"])
        manager.restore_state(
            state["trades"], state["balance_usdt"], state["last_prices"],
            state["manual_plan_blocks"],
        )
        self.assertTrue(manager.is_wall_manually_cancelled(
            "BTCUSDT", "stable_wall_v1", trade.wall_id, "buy", trade.wall_price
        ))
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
            leverage=35,
            take_profit_bps=55,
        )
        trading_settings = TradingSettings(
            stake_usdt=25, initial_balance_usdt=100, max_open_trades=5,
            leverage=40, default_take_profit_bps=42, default_stop_loss_bps=36,
        )

        with patch.object(repository, "async_session", return_value=session):
            run_immediate(repository.save_pair_settings(pair_settings))
            run_immediate(repository.save_trading_settings(trading_settings))

        self.assertFalse(pair_row.enabled)
        self.assertEqual(pair_row.min_wall_ratio, 900)
        self.assertEqual(pair_row.max_open_trades, 2)
        self.assertEqual(pair_row.leverage, 35)
        self.assertEqual(pair_row.take_profit_bps, 55)
        self.assertIsNone(pair_row.stop_loss_bps)
        self.assertEqual(runtime_row.stake_usdt, 25)
        self.assertEqual(runtime_row.max_open_trades, 5)
        self.assertEqual(runtime_row.default_leverage, 40)
        self.assertEqual(runtime_row.default_take_profit_bps, 42)
        self.assertEqual(runtime_row.default_stop_loss_bps, 36)
        self.assertEqual(session.commits, 2)

    def test_global_trade_level_settings_and_pending_replacement_commit_together(self):
        runtime_row = RuntimeState(
            id=1, balance_usdt=100, initial_balance_usdt=100,
            stake_usdt=10, max_open_trades=3, default_leverage=25,
            default_take_profit_bps=5, default_stop_loss_bps=5,
            engine_mode="running", initialized=True,
        )
        session = FakeSession({RuntimeState: [runtime_row]})
        settings = TradingSettings(
            default_take_profit_bps=12, default_stop_loss_bps=8,
        )
        replacement = {
            "signal_id": "replacement", "strategy_id": "bounce",
            "symbol": "BTCUSDT", "side": "long", "wall_side": "buy",
            "status": "pending", "wall_price": 100, "wall_size": 1000,
            "wall_ratio": 900, "stake_usdt": 10, "leverage": 25,
            "position_notional_usdt": 250, "entry_price": 100.02,
            "stop_loss": 99.94, "take_profit": 100.14,
            "accounting_version": 1, "profit_usdt": 0,
            "profit_percent": 0, "entry_armed": True,
        }
        with patch.object(repository, "async_session", return_value=session):
            run_immediate(repository.save_trading_settings_with_trades(
                settings, [replacement], 100, "running",
            ))

        saved_trade = session.rows[VirtualTrade][0]
        self.assertEqual(runtime_row.default_take_profit_bps, 12)
        self.assertEqual(runtime_row.default_stop_loss_bps, 8)
        self.assertEqual(saved_trade.status, "pending")
        self.assertEqual(saved_trade.accounting_version, 1)
        self.assertEqual(session.commits, 1)

    def test_trade_level_schema_defaults_and_nullable_pair_overrides(self):
        runtime_columns = RuntimeState.__table__.columns
        pair_columns = TradingPairSetting.__table__.columns
        self.assertEqual(
            runtime_columns["default_take_profit_bps"].server_default.arg.text,
            "5.0",
        )
        self.assertEqual(
            runtime_columns["default_stop_loss_bps"].server_default.arg.text,
            "5.0",
        )
        self.assertTrue(pair_columns["take_profit_bps"].nullable)
        self.assertTrue(pair_columns["stop_loss_bps"].nullable)

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

    def test_strategy_change_event_persists_telegram_id_above_int32(self):
        admin_user_id = 8445920246
        pair_row = PairStrategyConfig(
            symbol="BTCUSDT", enabled_strategy_ids=["bounce"]
        )
        session = FakeSession({PairStrategyConfig: [pair_row]})

        with patch.object(repository, "async_session", return_value=session):
            run_immediate(repository.save_enabled_strategies_with_trades(
                "BTCUSDT", {"breakout"}, [], 100, "running",
                admin_user_id=admin_user_id,
            ))

        event = session.rows[StrategyChangeEvent][0]
        self.assertEqual(event.admin_user_id, admin_user_id)
        self.assertEqual(
            StrategyChangeEvent.__table__.c.admin_user_id.type.__class__.__name__,
            "BigInteger",
        )

    def test_admin_user_id_postgres_migration_alters_column_without_dropping_data(self):
        migration = (
            Path(__file__).parents[1]
            / "database"
            / "migrations"
            / "20261029_strategy_change_admin_user_id_bigint.sql"
        ).read_text(encoding="utf-8").upper()
        self.assertIn("ALTER TABLE STRATEGY_CHANGE_EVENTS", migration)
        self.assertIn("ALTER COLUMN ADMIN_USER_ID TYPE BIGINT", migration)
        self.assertNotIn("DROP TABLE", migration)

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
            "leverage": 25,
            "position_notional_usdt": 250,
            "entry_fee_usdt": 0.1375,
            "exit_fee_usdt": 0.1376,
            "gross_pnl_usdt": 0.2786,
            "fee_rate": 0.00055,
            "accounting_version": 1,
        }
        with patch.object(repository, "async_session", return_value=session):
            run_immediate(
                repository.save_trades([trade], 100.0035, engine_mode="running")
            )

        saved = session.rows[VirtualTrade][0]
        self.assertEqual(saved.signal_id, trade["signal_id"])
        self.assertEqual(saved.status, "closed")
        self.assertEqual(saved.entry_price, trade["entry_price"])
        self.assertEqual(saved.exit_price, trade["exit_price"])
        self.assertEqual(saved.profit_usdt, 0.0035)
        self.assertEqual(saved.leverage, 25)
        self.assertEqual(saved.position_notional_usdt, 250)
        self.assertEqual(saved.entry_fee_usdt, 0.1375)
        self.assertEqual(saved.exit_fee_usdt, 0.1376)
        self.assertEqual(saved.gross_pnl_usdt, 0.2786)
        self.assertEqual(saved.fee_rate, 0.00055)
        self.assertEqual(runtime_state.balance_usdt, 100.0035)
        self.assertEqual(runtime_state.engine_mode, "running")
        self.assertEqual(session.commits, 1)

    def test_save_trade_persists_entry_crossing_state_and_simulated_fill(self):
        session = FakeSession()
        trade = {
            "signal_id": "plan-1", "strategy_id": "bounce", "symbol": "BTCUSDT",
            "side": "long", "wall_side": "buy", "status": "open",
            "wall_id": "BTCUSDT:buy:100:lifecycle", "wall_price": 100,
            "wall_size": 1000, "wall_ratio": 900, "stake_usdt": 10,
            "entry_price": 100.02, "entry_fill_price": 100.01,
            "entry_trigger_direction": "cross_down", "entry_armed": True,
            "stop_loss": 99.98, "take_profit": 100.06, "profit_usdt": 0,
        }
        with patch.object(repository, "async_session", return_value=session):
            run_immediate(repository.save_trades([trade], 99.8625))

        row = session.rows[VirtualTrade][0]
        self.assertEqual(row.wall_id, trade["wall_id"])
        self.assertEqual(row.entry_trigger_direction, "cross_down")
        self.assertTrue(row.entry_armed)
        self.assertEqual(row.entry_fill_price, 100.01)

    def test_manual_pending_cancellation_is_persisted_as_history(self):
        session = FakeSession()
        trade = {
            "signal_id": "manual-cancel-plan", "strategy_id": "bounce",
            "symbol": "BTCUSDT", "side": "long", "wall_side": "buy",
            "status": "cancelled", "wall_id": "BTCUSDT:buy:100:lifecycle-1",
            "wall_price": 100, "wall_size": 1000, "wall_ratio": 900,
            "stake_usdt": 10, "entry_price": 100.01, "stop_loss": 99.9,
            "take_profit": 100.2, "close_reason": "MANUAL_CANCELLED",
            "accounting_version": 1,
        }
        with patch.object(repository, "async_session", return_value=session):
            run_immediate(repository.save_trades([trade], 100.0))

        saved = session.rows[VirtualTrade][0]
        self.assertEqual(saved.status, "cancelled")
        self.assertEqual(saved.close_reason, "MANUAL_CANCELLED")
        self.assertEqual(saved.wall_id, trade["wall_id"])
        block = session.rows[ManualPlanBlock][0]
        self.assertEqual(block.signal_id, trade["signal_id"])
        self.assertEqual(block.wall_price, trade["wall_price"])

    def test_releasing_manual_plan_block_keeps_cancelled_trade_row(self):
        trade = VirtualTrade(
            signal_id="released-block-plan", strategy_id="bounce", symbol="BTCUSDT",
            side="long", wall_side="buy", status="cancelled", wall_price=100,
            wall_size=1000, stake_usdt=10, entry_price=100.01, stop_loss=99.9,
            take_profit=100.2, close_reason="MANUAL_CANCELLED",
        )
        block = ManualPlanBlock(
            signal_id=trade.signal_id, symbol="BTCUSDT", strategy_id="bounce",
            wall_id="wall-life", wall_side="buy", wall_price=100,
        )
        session = FakeSession({VirtualTrade: [trade], ManualPlanBlock: [block]})
        with patch.object(repository, "async_session", return_value=session):
            run_immediate(repository.release_manual_plan_blocks([trade.signal_id]))

        self.assertEqual(session.rows[VirtualTrade], [trade])
        self.assertEqual(session.rows[ManualPlanBlock], [])
        self.assertEqual(session.commits, 1)

    def test_partial_trade_update_preserves_accounting_version_and_insert_defaults_it(self):
        existing = VirtualTrade(
            signal_id="old-plan", strategy_id="bounce", symbol="BTCUSDT",
            side="long", wall_side="buy", status="pending", wall_price=100,
            wall_size=1000, stake_usdt=10, entry_price=100.02,
            stop_loss=99.98, take_profit=100.06, accounting_version=1,
        )

        class ReplacementSession(FakeSession):
            def __init__(self):
                super().__init__({VirtualTrade: [existing]})
                self.trade_selects = 0

            async def scalar(self, statement):
                self.trade_selects += 1
                return existing if self.trade_selects == 1 else None

        session = ReplacementSession()
        cancelled = {
            "signal_id": "old-plan", "status": "cancelled",
            "close_reason": "PLAN_REPLACED",
        }
        replacement = {
            "signal_id": "new-plan", "strategy_id": "bounce",
            "symbol": "BTCUSDT", "side": "long", "wall_side": "buy",
            "status": "pending", "wall_price": 100.01, "wall_size": 1100,
            "stake_usdt": 10, "entry_price": 100.03, "stop_loss": 99.99,
            "take_profit": 100.07,
        }
        with patch.object(repository, "async_session", return_value=session):
            run_immediate(repository.save_trades([cancelled, replacement], 100))

        self.assertEqual(existing.status, "cancelled")
        self.assertEqual(existing.close_reason, "PLAN_REPLACED")
        self.assertEqual(existing.accounting_version, 1)
        inserted = session.rows[VirtualTrade][1]
        self.assertEqual(inserted.accounting_version, 1)
        self.assertEqual(session.commits, 1)

    def test_trade_value_mapping_omits_unprovided_accounting_version_for_update(self):
        values = repository._virtual_trade_values(
            {"signal_id": "partial", "status": "cancelled"}
        )
        self.assertNotIn("accounting_version", values)
        self.assertNotIn(
            "accounting_version",
            repository._virtual_trade_values({"accounting_version": None}),
        )
        self.assertEqual(
            repository._virtual_trade_values({"signal_id": "new"}, is_insert=True)[
                "accounting_version"
            ],
            1,
        )

    def test_cancelled_cleanup_deletes_only_selected_old_cancelled_batch(self):
        now = datetime.now(timezone.utc)
        cancelled = VirtualTrade(
            signal_id="expired-cancelled", strategy_id="bounce", symbol="BTCUSDT",
            side="long", wall_side="buy", status="cancelled", wall_price=100,
            wall_size=1000, stake_usdt=10, entry_price=100,
            stop_loss=99, take_profit=101, closed_at=now - timedelta(minutes=120),
        )
        recent = VirtualTrade(
            signal_id="recent-cancelled", strategy_id="bounce", symbol="BTCUSDT",
            side="long", wall_side="buy", status="cancelled", wall_price=100,
            wall_size=1000, stake_usdt=10, entry_price=100,
            stop_loss=99, take_profit=101, closed_at=now - timedelta(minutes=2),
        )
        manual = VirtualTrade(
            signal_id="manual-blocked", strategy_id="bounce", symbol="BTCUSDT",
            side="long", wall_side="buy", status="cancelled", wall_price=100,
            wall_size=1000, stake_usdt=10, entry_price=100,
            stop_loss=99, take_profit=101, closed_at=now - timedelta(minutes=120),
        )
        block = ManualPlanBlock(
            signal_id=manual.signal_id, symbol="BTCUSDT", strategy_id="bounce",
            wall_side="buy", wall_price=100,
        )

        class CleanupSession(FakeSession):
            def __init__(self):
                super().__init__({
                    VirtualTrade: [cancelled, recent, manual],
                    ManualPlanBlock: [block],
                })
                self.candidates = ["expired-cancelled"]
                self.select_statement = None
                self.deleted = []

            async def scalars(self, statement):
                self.select_statement = statement
                return FakeResult(self.candidates)

            async def scalar(self, statement):
                return 2

            async def execute(self, statement):
                self.executed.append(statement)
                self.deleted = list(self.candidates)
                self.rows[VirtualTrade] = [
                    row for row in self.rows[VirtualTrade]
                    if row.signal_id not in self.deleted
                ]

        session = CleanupSession()
        with patch.object(repository, "async_session", return_value=session):
            deleted = run_immediate(repository.cleanup_cancelled_trades(
                older_than_minutes=60, batch_size=1, now=now,
            ))

        from sqlalchemy.dialects import postgresql

        statement = str(session.select_statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        ))
        self.assertEqual(deleted["deleted_signal_ids"], ["expired-cancelled"])
        self.assertEqual(deleted["cancelled_trade_count"], 2)
        self.assertEqual(deleted["deleted_count"], 1)
        self.assertEqual(session.deleted, ["expired-cancelled"])
        self.assertEqual(
            {row.signal_id for row in session.rows[VirtualTrade]},
            {"recent-cancelled", "manual-blocked"},
        )
        self.assertEqual(session.commits, 1)
        self.assertIn("cancelled", statement)
        self.assertIn("closed_at", statement)
        self.assertIn("manual_plan_blocks", statement)
        self.assertIn("FOR UPDATE SKIP LOCKED", statement)
        self.assertEqual(session.select_statement._limit, 1)
        self.assertTrue(session.select_statement._for_update_arg.skip_locked)

    def test_cancelled_cleanup_database_error_propagates_for_service_retry(self):
        class FailingCleanupSession(FakeSession):
            async def scalars(self, statement):
                return FakeResult(["expired-cancelled"])

            async def execute(self, statement):
                raise OSError("database unavailable")

        with patch.object(repository, "async_session", return_value=FailingCleanupSession()):
            with self.assertRaisesRegex(OSError, "database unavailable"):
                run_immediate(repository.cleanup_cancelled_trades())

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
