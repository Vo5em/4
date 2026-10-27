"""Opt-in integration coverage against a disposable local PostgreSQL database."""

import asyncio
import os
import unittest
from datetime import datetime, timezone

from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError

os.environ.setdefault(
    "DATABASE_URL", "postgresql+asyncpg://127.0.0.1/crypto_bot_test"
)

from database import request as repository
from database.models import Base, async_close, async_main
from script.settings import PairSettings, TradingSettings


RUN_INTEGRATION = os.getenv("RUN_POSTGRES_INTEGRATION") == "1"


@unittest.skipUnless(
    RUN_INTEGRATION,
    "Set RUN_POSTGRES_INTEGRATION=1 with a disposable *_test PostgreSQL database",
)
class PostgreSQLRepositoryIntegrationTests(unittest.TestCase):
    def test_schema_transactions_and_restart_recovery(self):
        database_url = make_url(os.environ["DATABASE_URL"])
        if (
            database_url.drivername != "postgresql+asyncpg"
            or database_url.host not in {"localhost", "127.0.0.1", "::1"}
            or not (database_url.database or "").endswith("_test")
        ):
            self.fail(
                "Destructive PostgreSQL integration test requires localhost and a database ending in _test"
            )

        async def exercise_repository():
            async with repository.engine.begin() as connection:
                await connection.run_sync(Base.metadata.drop_all)
            await async_main()

            defaults = TradingSettings(
                stake_usdt=10, initial_balance_usdt=100, max_open_trades=3
            )
            state = await repository.initialize_database(
                defaults, {"BTCUSDT": PairSettings("BTCUSDT")}
            )
            self.assertFalse(state["initialized"])
            self.assertEqual(
                state["enabled_strategies"], {"BTCUSDT": {"stable_wall_v1"}}
            )

            now = datetime.now(timezone.utc)
            trade = {
                "signal_id": "BTCUSDT:buy:100:postgres-integration",
                "strategy_id": "stable_wall_v1",
                "symbol": "BTCUSDT",
                "side": "long",
                "wall_side": "buy",
                "status": "pending",
                "wall_price": 100.0,
                "wall_size": 1000.0,
                "wall_ratio": 900.0,
                "stake_usdt": 10.0,
                "entry_price": 100.015,
                "stop_loss": 99.98,
                "take_profit": 100.05,
                "exit_price": None,
                "profit_usdt": 0.0,
                "profit_percent": 0.0,
                "close_reason": None,
                "created_at": now,
                "opened_at": None,
                "closed_at": None,
            }
            await repository.save_trades([trade], 100.0, engine_mode="running")
            await repository.save_market_price("BTCUSDT", 100.02)

            cancelled = dict(
                trade,
                status="cancelled",
                close_reason="STRATEGY_DISABLED",
                closed_at=now,
            )
            await repository.save_enabled_strategies_with_trades(
                "BTCUSDT", set(), [cancelled], 100.0, "running"
            )

            restored = await repository.initialize_database(
                defaults, {"BTCUSDT": PairSettings("BTCUSDT")}
            )
            self.assertEqual(restored["enabled_strategies"], {"BTCUSDT": set()})
            self.assertEqual(restored["last_prices"], {"BTCUSDT": 100.02})
            self.assertEqual(restored["trades"][0]["status"], "cancelled")
            self.assertEqual(
                restored["trades"][0]["close_reason"], "STRATEGY_DISABLED"
            )

            invalid = dict(cancelled, signal_id="x" * 201)
            with self.assertRaises(SQLAlchemyError):
                await repository.save_enabled_strategies_with_trades(
                    "BTCUSDT", {"stable_wall_v1"}, [invalid], 100.0, "running"
                )
            after_rollback = await repository.initialize_database(
                defaults, {"BTCUSDT": PairSettings("BTCUSDT")}
            )
            self.assertEqual(after_rollback["enabled_strategies"], {"BTCUSDT": set()})

            async with repository.engine.begin() as connection:
                await connection.run_sync(Base.metadata.drop_all)
            await async_close()

        asyncio.run(exercise_repository())
