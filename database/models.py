from datetime import datetime, timezone

from sqlalchemy import BigInteger, Boolean, DateTime, ForeignKey, Index, JSON, Numeric, String, text
from sqlalchemy.ext.asyncio import AsyncAttrs, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from config import DATABASE_URL


engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
async_session = async_sessionmaker(engine, expire_on_commit=False)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Base(AsyncAttrs, DeclarativeBase):
    pass


class RuntimeState(Base):
    __tablename__ = "runtime_state"

    id: Mapped[int] = mapped_column(primary_key=True, default=1)
    balance_usdt: Mapped[float] = mapped_column(Numeric(24, 8, asdecimal=False), nullable=False)
    initial_balance_usdt: Mapped[float] = mapped_column(Numeric(24, 8, asdecimal=False), nullable=False)
    stake_usdt: Mapped[float] = mapped_column(Numeric(24, 8, asdecimal=False), nullable=False)
    max_open_trades: Mapped[int] = mapped_column(nullable=False, default=3)
    default_leverage: Mapped[float] = mapped_column(Numeric(10, 4, asdecimal=False), nullable=False, default=25.0)
    default_take_profit_bps: Mapped[float] = mapped_column(
        Numeric(18, 8, asdecimal=False), nullable=False, default=5.0, server_default=text("5.0")
    )
    default_stop_loss_bps: Mapped[float] = mapped_column(
        Numeric(18, 8, asdecimal=False), nullable=False, default=5.0, server_default=text("5.0")
    )
    engine_mode: Mapped[str] = mapped_column(String(20), nullable=False, default="stopped")
    initialized: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now)


class TradingPairSetting(Base):
    __tablename__ = "trading_pairs"

    symbol: Mapped[str] = mapped_column(String(30), primary_key=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    min_wall_ratio: Mapped[float] = mapped_column(Numeric(18, 8, asdecimal=False), nullable=False, default=888.0)
    min_wall_notional_usdt: Mapped[float] = mapped_column(Numeric(24, 8, asdecimal=False), nullable=False, default=0)
    max_wall_distance_bps: Mapped[float] = mapped_column(Numeric(18, 8, asdecimal=False), nullable=False, default=100.0)
    min_wall_age_seconds: Mapped[float] = mapped_column(Numeric(18, 8, asdecimal=False), nullable=False, default=40.0)
    consumed_ratio: Mapped[float] = mapped_column(Numeric(8, 6, asdecimal=False), nullable=False, default=0.9)
    entry_distance_bps: Mapped[float] = mapped_column(Numeric(18, 8, asdecimal=False), nullable=False, default=1.5)
    stop_distance_bps: Mapped[float] = mapped_column(Numeric(18, 8, asdecimal=False), nullable=False, default=2.0)
    max_open_trades: Mapped[int] = mapped_column(nullable=False, default=1)
    bounce_reversal_bps: Mapped[float] = mapped_column(Numeric(18, 8, asdecimal=False), nullable=False, default=1.0)
    breakout_confirm_seconds: Mapped[float] = mapped_column(Numeric(18, 8, asdecimal=False), nullable=False, default=5.0)
    breakout_buffer_bps: Mapped[float] = mapped_column(Numeric(18, 8, asdecimal=False), nullable=False, default=0.5)
    breakout_max_update_gap_seconds: Mapped[float] = mapped_column(Numeric(18, 8, asdecimal=False), nullable=False, default=2.0)
    leverage: Mapped[float | None] = mapped_column(Numeric(10, 4, asdecimal=False), nullable=True)
    maker_fee_rate: Mapped[float] = mapped_column(Numeric(18, 10, asdecimal=False), nullable=False, default=0.0002)
    taker_fee_rate: Mapped[float] = mapped_column(Numeric(18, 10, asdecimal=False), nullable=False, default=0.00055)
    take_profit_bps: Mapped[float | None] = mapped_column(Numeric(18, 8, asdecimal=False), nullable=True)
    stop_loss_bps: Mapped[float | None] = mapped_column(Numeric(18, 8, asdecimal=False), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now)


class PairStrategyConfig(Base):
    __tablename__ = "pair_strategy_configs"

    symbol: Mapped[str] = mapped_column(String(30), primary_key=True)
    enabled_strategy_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    version: Mapped[int] = mapped_column(nullable=False, default=1)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now)


class StrategyChangeEvent(Base):
    __tablename__ = "strategy_change_events"

    id: Mapped[int] = mapped_column(primary_key=True)
    symbol: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    previous_strategy: Mapped[str | None] = mapped_column(String(60), nullable=True)
    new_strategy: Mapped[str | None] = mapped_column(String(60), nullable=True)
    admin_user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    result: Mapped[str] = mapped_column(String(24), nullable=False, default="applied")
    cancelled_plans: Mapped[int] = mapped_column(nullable=False, default=0)
    cancellation_reasons: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    changed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)


class MarketPrice(Base):
    """Last price observed per symbol, used to recover safe virtual stop state."""

    __tablename__ = "market_prices"

    symbol: Mapped[str] = mapped_column(String(30), primary_key=True)
    last_price: Mapped[float] = mapped_column(Numeric(30, 12, asdecimal=False), nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)


class ManualPlanBlock(Base):
    """Active manual PENDING suppressions; deleted when their wall disappears."""

    __tablename__ = "manual_plan_blocks"

    signal_id: Mapped[str] = mapped_column(
        String(200), ForeignKey("virtual_trades.signal_id", ondelete="CASCADE"),
        primary_key=True,
    )
    symbol: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    strategy_id: Mapped[str] = mapped_column(String(60), nullable=False)
    wall_id: Mapped[str | None] = mapped_column(String(240), nullable=True)
    wall_side: Mapped[str] = mapped_column(String(10), nullable=False)
    wall_price: Mapped[float] = mapped_column(Numeric(30, 12, asdecimal=False), nullable=False)


class VirtualTrade(Base):
    __tablename__ = "virtual_trades"
    __table_args__ = (
        Index("ix_virtual_trades_symbol_status", "symbol", "status"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    signal_id: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    strategy_id: Mapped[str] = mapped_column(String(60), nullable=False)
    symbol: Mapped[str] = mapped_column(String(30), nullable=False)
    side: Mapped[str] = mapped_column(String(10), nullable=False)
    wall_side: Mapped[str] = mapped_column(String(10), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    wall_price: Mapped[float] = mapped_column(Numeric(30, 12, asdecimal=False), nullable=False)
    wall_size: Mapped[float] = mapped_column(Numeric(30, 8, asdecimal=False), nullable=False)
    wall_ratio: Mapped[float] = mapped_column(Numeric(24, 8, asdecimal=False), nullable=False, default=0)
    stake_usdt: Mapped[float] = mapped_column(Numeric(24, 8, asdecimal=False), nullable=False)
    leverage: Mapped[float | None] = mapped_column(Numeric(10, 4, asdecimal=False), nullable=True)
    position_notional_usdt: Mapped[float | None] = mapped_column(Numeric(24, 8, asdecimal=False), nullable=True)
    entry_fee_usdt: Mapped[float | None] = mapped_column(Numeric(24, 8, asdecimal=False), nullable=True)
    exit_fee_usdt: Mapped[float | None] = mapped_column(Numeric(24, 8, asdecimal=False), nullable=True)
    gross_pnl_usdt: Mapped[float | None] = mapped_column(Numeric(24, 8, asdecimal=False), nullable=True)
    fee_rate: Mapped[float | None] = mapped_column(Numeric(18, 10, asdecimal=False), nullable=True)
    accounting_version: Mapped[int] = mapped_column(
        nullable=False, default=1, server_default=text("1")
    )
    entry_price: Mapped[float] = mapped_column(Numeric(30, 12, asdecimal=False), nullable=False)
    entry_fill_price: Mapped[float | None] = mapped_column(Numeric(30, 12, asdecimal=False), nullable=True)
    wall_id: Mapped[str | None] = mapped_column(String(240), nullable=True)
    entry_trigger_direction: Mapped[str | None] = mapped_column(String(12), nullable=True)
    entry_armed: Mapped[bool] = mapped_column(nullable=False, default=False)
    stop_loss: Mapped[float] = mapped_column(Numeric(30, 12, asdecimal=False), nullable=False)
    take_profit: Mapped[float] = mapped_column(Numeric(30, 12, asdecimal=False), nullable=False)
    exit_price: Mapped[float | None] = mapped_column(Numeric(30, 12, asdecimal=False), nullable=True)
    profit_usdt: Mapped[float] = mapped_column(Numeric(24, 8, asdecimal=False), nullable=False, default=0)
    profit_percent: Mapped[float] = mapped_column(Numeric(18, 8, asdecimal=False), nullable=False, default=0)
    close_reason: Mapped[str | None] = mapped_column(String(40), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    opened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


async def async_main() -> None:
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
        # `create_all` does not add columns to an existing installation.
        # Keep this small additive migration idempotent for deployed databases.
        await connection.execute(text(
            "ALTER TABLE trading_pairs ADD COLUMN IF NOT EXISTS "
            "bounce_reversal_bps NUMERIC(18, 8) NOT NULL DEFAULT 1.0"
        ))
        await connection.execute(text(
            "ALTER TABLE trading_pairs ADD COLUMN IF NOT EXISTS "
            "breakout_confirm_seconds NUMERIC(18, 8) NOT NULL DEFAULT 5.0"
        ))
        await connection.execute(text(
            "ALTER TABLE trading_pairs ADD COLUMN IF NOT EXISTS "
            "breakout_buffer_bps NUMERIC(18, 8) NOT NULL DEFAULT 0.5"
        ))
        await connection.execute(text(
            "ALTER TABLE trading_pairs ADD COLUMN IF NOT EXISTS "
            "breakout_max_update_gap_seconds NUMERIC(18, 8) NOT NULL DEFAULT 2.0"
        ))
        await connection.execute(text("ALTER TABLE runtime_state ADD COLUMN IF NOT EXISTS default_leverage NUMERIC(10,4) NOT NULL DEFAULT 25.0"))
        await connection.execute(text(
            "ALTER TABLE runtime_state ADD COLUMN IF NOT EXISTS "
            "default_take_profit_bps NUMERIC(18,8) NOT NULL DEFAULT 5.0"
        ))
        await connection.execute(text(
            "ALTER TABLE runtime_state ADD COLUMN IF NOT EXISTS "
            "default_stop_loss_bps NUMERIC(18,8) NOT NULL DEFAULT 5.0"
        ))
        await connection.execute(text(
            "ALTER TABLE runtime_state ALTER COLUMN default_take_profit_bps SET DEFAULT 5.0"
        ))
        await connection.execute(text(
            "ALTER TABLE runtime_state ALTER COLUMN default_stop_loss_bps SET DEFAULT 5.0"
        ))
        await connection.execute(text("ALTER TABLE trading_pairs ADD COLUMN IF NOT EXISTS leverage NUMERIC(10,4)"))
        await connection.execute(text("ALTER TABLE trading_pairs ADD COLUMN IF NOT EXISTS maker_fee_rate NUMERIC(18,10) NOT NULL DEFAULT 0.0002"))
        await connection.execute(text("ALTER TABLE trading_pairs ADD COLUMN IF NOT EXISTS taker_fee_rate NUMERIC(18,10) NOT NULL DEFAULT 0.00055"))
        await connection.execute(text(
            "ALTER TABLE trading_pairs ADD COLUMN IF NOT EXISTS take_profit_bps NUMERIC(18,8)"
        ))
        await connection.execute(text(
            "ALTER TABLE trading_pairs ADD COLUMN IF NOT EXISTS stop_loss_bps NUMERIC(18,8)"
        ))
        for column, sql_type in (("leverage", "NUMERIC(10,4)"), ("position_notional_usdt", "NUMERIC(24,8)"), ("entry_fee_usdt", "NUMERIC(24,8)"), ("exit_fee_usdt", "NUMERIC(24,8)"), ("gross_pnl_usdt", "NUMERIC(24,8)"), ("fee_rate", "NUMERIC(18,10)")):
            await connection.execute(text(f"ALTER TABLE virtual_trades ADD COLUMN IF NOT EXISTS {column} {sql_type}"))
        await connection.execute(text("ALTER TABLE virtual_trades ADD COLUMN IF NOT EXISTS accounting_version INTEGER NOT NULL DEFAULT 1"))
        await connection.execute(text(
            "ALTER TABLE virtual_trades ALTER COLUMN accounting_version SET DEFAULT 1"
        ))
        await connection.execute(text("ALTER TABLE virtual_trades ADD COLUMN IF NOT EXISTS entry_fill_price NUMERIC(30,12)"))
        await connection.execute(text("ALTER TABLE virtual_trades ADD COLUMN IF NOT EXISTS wall_id VARCHAR(240)"))
        await connection.execute(text("ALTER TABLE virtual_trades ADD COLUMN IF NOT EXISTS entry_trigger_direction VARCHAR(12)"))
        await connection.execute(text("ALTER TABLE virtual_trades ADD COLUMN IF NOT EXISTS entry_armed BOOLEAN NOT NULL DEFAULT FALSE"))
        await connection.execute(text(
            "ALTER TABLE pair_strategy_configs ADD COLUMN IF NOT EXISTS "
            "version INTEGER NOT NULL DEFAULT 1"
        ))
        await connection.execute(text(
            "ALTER TABLE strategy_change_events "
            "ALTER COLUMN admin_user_id TYPE BIGINT"
        ))
        await connection.execute(text(
            "UPDATE trading_pairs SET min_wall_age_seconds = 40 "
            "WHERE min_wall_age_seconds < 40"
        ))


async def async_close() -> None:
    await engine.dispose()
