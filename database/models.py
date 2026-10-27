from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, JSON, Numeric, String
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
    min_wall_age_seconds: Mapped[float] = mapped_column(Numeric(18, 8, asdecimal=False), nullable=False, default=20.0)
    consumed_ratio: Mapped[float] = mapped_column(Numeric(8, 6, asdecimal=False), nullable=False, default=0.9)
    entry_distance_bps: Mapped[float] = mapped_column(Numeric(18, 8, asdecimal=False), nullable=False, default=1.5)
    stop_distance_bps: Mapped[float] = mapped_column(Numeric(18, 8, asdecimal=False), nullable=False, default=2.0)
    max_open_trades: Mapped[int] = mapped_column(nullable=False, default=1)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now)


class PairStrategyConfig(Base):
    __tablename__ = "pair_strategy_configs"

    symbol: Mapped[str] = mapped_column(String(30), primary_key=True)
    enabled_strategy_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now)


class MarketPrice(Base):
    """Last price observed per symbol, used to recover safe virtual stop state."""

    __tablename__ = "market_prices"

    symbol: Mapped[str] = mapped_column(String(30), primary_key=True)
    last_price: Mapped[float] = mapped_column(Numeric(30, 12, asdecimal=False), nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)


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
    entry_price: Mapped[float] = mapped_column(Numeric(30, 12, asdecimal=False), nullable=False)
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


async def async_close() -> None:
    await engine.dispose()


# Compatibility names retained while application code migrates to explicit models.
Bot_setting = RuntimeState
Order = VirtualTrade
Completad_trades = VirtualTrade
Simbols = TradingPairSetting
