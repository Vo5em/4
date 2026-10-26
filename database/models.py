from sqlalchemy import ForeignKey, String, BigInteger, DateTime, Boolean
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase, relationship
from sqlalchemy.ext.asyncio import AsyncAttrs, async_sessionmaker, create_async_engine
from datetime import datetime
from zoneinfo import ZoneInfo
from config import DATABASE_URL

MOSCOW_TZ = ZoneInfo("Europe/Moscow")

engine = create_async_engine(DATABASE_URL, echo=True)

async_session = async_sessionmaker(engine)

class Base(AsyncAttrs, DeclarativeBase):
    pass

class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    balance: Mapped[int] = mapped_column(default=0)
    bet: Mapped[int] = mapped_column(default=0)
    successful_trades: Mapped[int] = mapped_column(default=0)
    failed_trades: Mapped[int] = mapped_column(default=0)


class Order(Base):
    __tablename__ = 'orders'
    id: Mapped[int] = mapped_column(primary_key=True)
    symbol_id: Mapped[int] = mapped_column(ForeignKey("symbols.id"),nullable=False)
    side: Mapped[str] = mapped_column(String(10), nullable=True)
    status: Mapped[str] = mapped_column(String(10), nullable=True)
    wall_price: Mapped[int] = mapped_column(default=0)
    wall_size: Mapped[int] = mapped_column(default=0)
    local_median: Mapped[int] = mapped_column(default=0)
    wall_ratio: Mapped[int] = mapped_column(default=0)
    distance_bps: Mapped[int] = mapped_column(default=0)
    entry_price: Mapped[int] = mapped_column(default=0)
    stop_loss: Mapped[int] = mapped_column(default=0)
    take_profit: Mapped[int] = mapped_column(default=0)
    created_at: Mapped[datetime] =  mapped_column(DateTime(timezone=True), default=lambda: datetime.now(MOSCOW_TZ))
    activated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True),
                                                   default=lambda: datetime.now(MOSCOW_TZ),
                                                   nullable=True)

class Completad_trades(Base):
    __tablename__ = 'completad_trades'
    id: Mapped[int] = mapped_column(primary_key=True)
    symbol: Mapped[str] = mapped_column(ForeignKey("symbols.id"),nullable=False)
    side: Mapped[str] = mapped_column(String(10), nullable=True)
    wall_price: Mapped[int] = mapped_column(default=0)
    wall_ratio: Mapped[int] = mapped_column(default=0)
    entry_price: Mapped[int] = mapped_column(default=0)
    exit_price: Mapped[int] = mapped_column(default=0)
    stop_loss: Mapped[int] = mapped_column(default=0)
    take_profit: Mapped[int] = mapped_column(default=0)
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    closed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    position_size: Mapped[int] = mapped_column(default=0)
    profit_usdt: Mapped[int] = mapped_column(default=0)
    profit_percent: Mapped[int] = mapped_column(default=0)
    result = Mapped[str] = mapped_column(String(15), nullable=True)
    close_reason: Mapped[str] = mapped_column(String(15), nullable=True)


class Simbols(Base):
    __tablename__ = 'simbols'
    id: Mapped[int] = mapped_column(primary_key=True)
    symbol: Mapped[str] = mapped_column(String(15), nullable=True)
    exchange: Mapped[str] = mapped_column(String(15), nullable=True)
    enabled: Mapped[bool] = mapped_column(default=0)


async def async_main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)