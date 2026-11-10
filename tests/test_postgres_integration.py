"""Opt-in integration coverage against a disposable local PostgreSQL database."""

import asyncio
import os
import unittest
from datetime import datetime, timedelta, timezone

from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import select, text

os.environ.setdefault(
    "DATABASE_URL", "postgresql+asyncpg://127.0.0.1/crypto_bot_test"
)

from database import request as repository
from database.models import (
    Base, ManualPlanBlock, RuntimeState, StrategyChangeEvent, VirtualTrade,
    async_close, async_main,
)
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
            # Simulate the deployed legacy schema, then verify startup migration
            # upgrades the existing column without recreating its table.
            async with repository.engine.begin() as connection:
                await connection.execute(text(
                    "ALTER TABLE strategy_change_events "
                    "ALTER COLUMN admin_user_id TYPE INTEGER"
                ))
            await async_main()
            async with repository.engine.connect() as connection:
                column_type = await connection.scalar(text(
                    "SELECT data_type FROM information_schema.columns "
                    "WHERE table_name = 'strategy_change_events' "
                    "AND column_name = 'admin_user_id'"
                ))
            self.assertEqual(column_type, "bigint")

            async with repository.engine.begin() as connection:
                await connection.execute(text(
                    "ALTER TABLE trading_pairs "
                    "DROP COLUMN historical_level_pctbound, "
                    "DROP COLUMN historical_level_min_touches, "
                    "DROP COLUMN historical_level_extmethod, "
                    "DROP COLUMN historical_level_accuracy"
                ))
            await async_main()
            async with repository.engine.connect() as connection:
                trendln_columns = (await connection.execute(text(
                    "SELECT column_name, data_type, column_default, is_nullable "
                    "FROM information_schema.columns "
                    "WHERE table_name = 'trading_pairs' AND column_name IN "
                    "('historical_level_pctbound', 'historical_level_min_touches', "
                    "'historical_level_extmethod', 'historical_level_accuracy')"
                ))).all()
            trendln_schema = {name: (data_type, default, nullable)
                              for name, data_type, default, nullable in trendln_columns}
            self.assertEqual(set(trendln_schema), {
                "historical_level_pctbound", "historical_level_min_touches",
                "historical_level_extmethod", "historical_level_accuracy",
            })
            self.assertEqual(trendln_schema["historical_level_pctbound"][0], "numeric")
            self.assertEqual(trendln_schema["historical_level_min_touches"][0], "integer")
            self.assertEqual(trendln_schema["historical_level_extmethod"][0], "character varying")
            self.assertEqual(trendln_schema["historical_level_accuracy"][0], "integer")
            self.assertTrue(all(column[2] == "NO" for column in trendln_schema.values()))
            self.assertIn("0.05", trendln_schema["historical_level_pctbound"][1])
            self.assertIn("1", trendln_schema["historical_level_min_touches"][1])
            self.assertIn("METHOD_NUMDIFF", trendln_schema["historical_level_extmethod"][1])
            self.assertIn("2", trendln_schema["historical_level_accuracy"][1])

            defaults = TradingSettings(
                stake_usdt=10, initial_balance_usdt=100, max_open_trades=3
            )
            state = await repository.initialize_database(
                defaults, {"BTCUSDT": PairSettings("BTCUSDT", leverage=17)}
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
                "leverage": 17.0,
                "position_notional_usdt": 170.0,
                "entry_fee_usdt": 0.0935,
                "exit_fee_usdt": 0.0,
                "gross_pnl_usdt": 0.0,
                "fee_rate": 0.00055,
                "accounting_version": 1,
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
                "BTCUSDT", set(), [cancelled], 100.0, "running",
                admin_user_id=8445920246,
            )
            async with repository.async_session() as session:
                saved_admin_user_id = await session.scalar(
                    select(StrategyChangeEvent.admin_user_id)
                    .order_by(StrategyChangeEvent.id.desc())
                    .limit(1)
                )
            self.assertEqual(saved_admin_user_id, 8445920246)

            restored = await repository.initialize_database(
                defaults, {"BTCUSDT": PairSettings("BTCUSDT", leverage=17)}
            )
            self.assertEqual(restored["enabled_strategies"], {"BTCUSDT": set()})
            self.assertEqual(restored["last_prices"], {"BTCUSDT": 100.02})
            self.assertEqual(restored["trades"][0]["status"], "cancelled")
            self.assertEqual(restored["trades"][0]["leverage"], 17.0)
            self.assertEqual(restored["trades"][0]["position_notional_usdt"], 170.0)
            self.assertEqual(restored["trades"][0]["entry_fee_usdt"], 0.0935)
            self.assertEqual(restored["trades"][0]["taker_fee_rate"], 0.00055)
            self.assertEqual(restored["pair_settings"]["BTCUSDT"].leverage, 17.0)
            self.assertEqual(
                restored["trades"][0]["close_reason"], "STRATEGY_DISABLED"
            )

            # Exercise bounded cleanup against PostgreSQL while preserving a
            # recent cancelled row, active manual suppression, and active/
            # financial trade rows.
            expired_cancelled = dict(
                trade, signal_id="expired-cleanup", status="cancelled",
                close_reason="PLAN_REPLACED", closed_at=now - timedelta(minutes=120),
            )
            expired_manual = dict(
                trade, signal_id="expired-manual-cleanup", status="cancelled",
                close_reason="MANUAL_CANCELLED", closed_at=now - timedelta(minutes=120),
            )
            recent_cancelled = dict(
                trade, signal_id="recent-cleanup", status="cancelled",
                close_reason="PLAN_REPLACED", closed_at=now,
            )
            pending = dict(
                trade, signal_id="pending-cleanup-protection", status="pending",
                closed_at=None,
            )
            open_trade = dict(
                trade, signal_id="open-cleanup-protection", status="open",
                opened_at=now, closed_at=None,
            )
            closed_trade = dict(
                trade, signal_id="closed-cleanup-protection", status="closed",
                exit_price=100.1, profit_usdt=0.1, closed_at=now,
            )
            await repository.save_trades(
                [expired_cancelled, expired_manual, recent_cancelled,
                 pending, open_trade, closed_trade],
                100.0, engine_mode="running",
            )
            deleted = await repository.cleanup_cancelled_trades(
                older_than_minutes=60, batch_size=10, now=now,
            )
            self.assertEqual(deleted["deleted_signal_ids"], ["expired-cleanup"])
            self.assertEqual(deleted["deleted_count"], 1)
            self.assertEqual(deleted["cancelled_trade_count"], 3)
            async with repository.async_session() as session:
                remaining = {
                    row.signal_id: row.status
                    for row in (await session.scalars(
                        select(VirtualTrade).where(VirtualTrade.signal_id.in_([
                            "expired-cleanup", "expired-manual-cleanup",
                            "recent-cleanup", "pending-cleanup-protection",
                            "open-cleanup-protection", "closed-cleanup-protection",
                        ]))
                    )).all()
                }
                block_count = await session.scalar(
                    select(ManualPlanBlock.signal_id).where(
                        ManualPlanBlock.signal_id == "expired-manual-cleanup"
                    )
                )
                runtime_state = await session.get(RuntimeState, 1)
            self.assertNotIn("expired-cleanup", remaining)
            self.assertEqual(remaining["expired-manual-cleanup"], "cancelled")
            self.assertEqual(remaining["recent-cleanup"], "cancelled")
            self.assertEqual(remaining["pending-cleanup-protection"], "pending")
            self.assertEqual(remaining["open-cleanup-protection"], "open")
            self.assertEqual(remaining["closed-cleanup-protection"], "closed")
            self.assertEqual(block_count, "expired-manual-cleanup")
            self.assertEqual(runtime_state.cancelled_trade_count, 3)
            self.assertEqual(runtime_state.last_cancelled_cleanup_at, now)
            self.assertEqual(runtime_state.last_cancelled_cleanup_deleted, 1)

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
