import asyncio
import os

from dotenv import load_dotenv

load_dotenv()

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from app.admin import admin
from app.runtime import RuntimeController
from script.engine import EngineMode, TradingEngine
from script.settings import PairSettings, TradingSettings, normalize_pair_symbol


def _symbols_from_environment() -> list[str]:
    values = os.getenv("TRADING_SYMBOLS", "BTCUSDT").split(",")
    symbols = []
    for value in values:
        if not value.strip():
            continue
        try:
            symbol = normalize_pair_symbol(value)
        except ValueError as error:
            raise RuntimeError("TRADING_SYMBOLS contains an invalid Bybit Linear USDT symbol.") from error
        if symbol not in symbols:
            symbols.append(symbol)
    if not symbols:
        raise RuntimeError("TRADING_SYMBOLS must contain at least one symbol")
    return symbols


async def build_runtime() -> tuple[RuntimeController, bool]:
    from database import request as database

    default_trading_settings = TradingSettings(
        stake_usdt=float(os.getenv("VIRTUAL_STAKE_USDT", "10")),
        initial_balance_usdt=float(os.getenv("VIRTUAL_INITIAL_BALANCE_USDT", "1000")),
        max_open_trades=int(os.getenv("VIRTUAL_MAX_OPEN_TRADES", "3")),
    )
    default_pairs = {
        symbol: PairSettings(symbol=symbol)
        for symbol in _symbols_from_environment()
    }
    state = await database.initialize_database(default_trading_settings, default_pairs)
    engine = TradingEngine(
        pair_settings=state["pair_settings"],
        trading_settings=state["trading_settings"],
        enabled_strategies=state["enabled_strategies"],
        repository=database,
        last_market_update_at=state.get("last_price_observed_at", {}),
    )
    engine.record_database_success()
    engine.trade_manager.restore_state(
        state["trades"], state["balance_usdt"], state["last_prices"]
    )

    mode = state["engine_mode"]
    if mode == EngineMode.STOPPING.value or mode not in {item.value for item in EngineMode}:
        mode = EngineMode.STOPPED.value
    engine.mode = EngineMode(mode)
    return RuntimeController(engine), state["initialized"]


async def main() -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError(
            "Set TELEGRAM_BOT_TOKEN in the process environment before starting the bot."
        )

    from database.models import async_close

    runtime, initialized = await build_runtime()
    bot = Bot(
        token=token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dispatcher = Dispatcher()
    dispatcher.include_router(admin)

    try:
        if not initialized:
            await runtime.start()
        elif runtime.engine.mode in {EngineMode.RUNNING, EngineMode.ENTRIES_PAUSED}:
            await runtime.start(preserve_mode=True)
        await dispatcher.start_polling(bot, runtime=runtime)
    finally:
        try:
            await runtime.shutdown()
        except Exception as error:
            print(f"Graceful shutdown persistence failed: {type(error).__name__}")
        finally:
            await bot.session.close()
            await async_close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
