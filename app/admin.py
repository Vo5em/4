import os
import hashlib
from math import isfinite
from datetime import timezone
from html import escape
from dataclasses import replace
import logging

from aiogram import F, Router
from aiogram.filters import Command, CommandStart, Filter, StateFilter
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

import app.keyboard as kb
from app.runtime import (
    RuntimeController,
    StrategyVersionConflict,
    StrategyWorkerSyncError,
)
from script.engine import StrategyPersistenceError
from script.engine import EngineMode
from script.settings import PairSettings
from script.trade_levels import (
    ENTRY_DISTANCE_BPS,
    effective_trade_level_bps,
    estimate_trade_outcomes,
)

admin = Router()
logger = logging.getLogger(__name__)


def _load_admin_user_ids() -> set[int]:
    raw_value = os.getenv("ADMIN_USER_ID", "").strip()
    if not raw_value:
        raise RuntimeError(
            "Set ADMIN_USER_ID to comma-separated Telegram admin user IDs."
        )

    values = [value.strip() for value in raw_value.split(",")]
    if any(not value.isascii() or not value.isdigit() for value in values):
        raise RuntimeError(
            "ADMIN_USER_ID must contain positive integers separated by commas."
        )

    user_ids = {int(value) for value in values}
    if any(user_id <= 0 for user_id in user_ids):
        raise RuntimeError(
            "ADMIN_USER_ID must contain positive integers separated by commas."
        )
    return user_ids


ADMIN_USER_IDS = _load_admin_user_ids()


class SettingsInput(StatesGroup):
    waiting_for_stake = State()
    waiting_for_pair = State()
    waiting_for_new_pair = State()
    waiting_for_strategy = State()
    waiting_for_pair_statistics = State()
    waiting_for_pair_leverage = State()
    waiting_for_trade_level = State()
    waiting_for_open_trade_level = State()


class Admin(Filter):
    async def __call__(self, event: Message | CallbackQuery) -> bool:
        user = event.from_user
        chat = event.chat if isinstance(event, Message) else (
            event.message.chat if event.message is not None else None
        )
        return (
            user is not None
            and user.id in ADMIN_USER_IDS
            and chat is not None
            and chat.type == "private"
        )


def _mode_label(mode: str) -> str:
    return {
        EngineMode.RUNNING.value: "Работает",
        EngineMode.ENTRIES_PAUSED.value: "Входы на паузе; открытые сделки сопровождаются",
        EngineMode.STOPPING.value: "Остановка",
        EngineMode.STOPPED.value: "Остановлен",
    }.get(mode, mode)


def _format_ratio(value: float | None, wins: int) -> str:
    if value is not None:
        return f"{value:.2f}"
    return "∞" if wins else "—"


def _feed_label(value: str) -> str:
    return {
        "connecting": "подключение",
        "connected": "подключён",
        "reconnecting": "переподключение",
        "stopped": "остановлен",
    }.get(value, value)


def edit_markup_key(markup) -> tuple:
    if markup is None:
        return ()
    return tuple(
        tuple((button.text, button.callback_data, button.url) for button in row)
        for row in markup.inline_keyboard
    )


async def edit_message_if_changed(message, text: str, reply_markup=None) -> bool:
    """Avoid Telegram's message-is-not-modified error on repeated panel refreshes."""
    current_markup = getattr(message, "reply_markup", None)
    if (
        getattr(message, "text", None) == text
        and edit_markup_key(current_markup) == edit_markup_key(reply_markup)
    ):
        return False
    try:
        await message.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest as error:
        if "message is not modified" in str(error).lower():
            return False
        raise
    return True


async def _refresh_dashboard_after_action(callback, runtime: RuntimeController) -> None:
    """A Telegram edit failure must not change an already committed engine action."""
    if callback.message is None:
        return
    try:
        await edit_message_if_changed(
            callback.message, dashboard_text(runtime), reply_markup=kb.main
        )
    except Exception:
        logger.exception("Could not refresh admin dashboard after state change")


def _date_label(value) -> str:
    if value is None:
        return "нет данных"
    return value.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z") if value.tzinfo else value.strftime("%Y-%m-%d %H:%M:%S")


def _algorithm_label(value: str) -> str:
    return {
        "working": "работает",
        "waiting": "ожидает данные",
        "stopped": "остановлен",
        "error": "ошибка",
    }.get(value, "нет данных")


def _market_health_label(symbol: str, health: dict) -> str:
    price_age = health.get("price_age_seconds")
    value_age = health.get("price_value_age_seconds")
    book_age = health.get("book_age_seconds")
    synchronized = "да" if health.get("book_synchronized") else "нет"
    if price_age is None or book_age is None:
        return f"{symbol}: возраст данных нет данных / синхр. {synchronized}"
    value_age_label = f"{value_age:.2f} с назад" if value_age is not None else "нет данных"
    return (
        f"{symbol}: цена {price_age:.2f} с; Last Price изменился {value_age_label}; "
        f"стакан {book_age:.2f} с; синхр. {synchronized}"
    )


def _database_label(status: str) -> str:
    return {
        "connected": "последняя операция успешна",
        "unknown": "состояние неизвестно",
        "error": "ошибка",
        "not_configured": "не настроена",
    }.get(status, "нет данных")


def pair_statistics_text(runtime: RuntimeController, symbol: str) -> str:
    symbol = symbol.strip().upper()
    settings = runtime.engine.pair_settings.get(symbol)
    if settings is None:
        available = ", ".join(runtime.engine.pair_settings) or "нет настроенных пар"
        return f"Неизвестная пара. Доступные пары: {available}"
    engine = runtime.engine
    status = engine.get_status()
    stats = engine.trade_manager.get_pair_statistics(symbol)
    metric = status["market_analysis"].get(symbol, {})
    strategies = ", ".join(sorted(engine.get_enabled_strategy_ids(symbol))) or "нет"
    feed_error = status["feed_errors"].get(symbol)
    strategy_errors = status["strategy_errors"].get(symbol, {})
    db_error = status.get("database_last_error")
    latest_error = feed_error or db_error or (next(iter(strategy_errors.values()), None))
    error_line = (
        f"Последняя ошибка: {escape(str(latest_error.get('type', 'Error')))}: "
        f"{escape(str(latest_error.get('message', '')))}"
        if latest_error else "Последняя ошибка: нет"
    )
    price = engine.trade_manager.last_prices.get(symbol)
    market_health = status.get("market_data_health", {}).get(symbol, {})
    price_age = market_health.get("price_age_seconds")
    price_value_age = market_health.get("price_value_age_seconds")
    book_age = market_health.get("book_age_seconds")
    potential = metric.get("potential_wall_count")
    stable = metric.get("stable_wall_count")
    win_rate = (
        f"{stats['win_rate_percent']:.1f}%"
        if stats["win_rate_percent"] is not None else "нет данных"
    )
    return (
        f"<b>Статистика пары · {symbol}</b>\n\n"
        f"Состояние: {'включена' if settings.enabled else 'выключена'}\n"
        f"Стратегии: {escape(strategies)}\n"
        f"Стена: {settings.min_wall_ratio:g}× от среднего; минимум "
        f"{settings.min_wall_notional_usdt:g} USDT\n"
        f"Дистанция поиска: {settings.max_wall_distance_bps:g} б.п.\n"
        f"Условие входа: подтверждённая устойчивая стена, затем касание цены "
        f"на расстоянии {settings.entry_distance_bps:g} б.п. от стены\n"
        f"Стоп за стеной: {settings.stop_distance_bps:g} б.п.\n"
        f"Время устойчивости: {max(40.0, settings.min_wall_age_seconds):g} с\n"
        f"Подтверждение отскока: {settings.bounce_reversal_bps:g} б.п.; пробоя: "
        f"удержание {settings.breakout_confirm_seconds:g} с за уровнем на "
        f"{settings.breakout_buffer_bps:g} б.п.; разрыв обновлений до "
        f"{settings.breakout_max_update_gap_seconds:g} с\n"
        f"Порог потребления: {settings.consumed_ratio:.0%}\n"
        f"Потенциальные стены: {potential if potential is not None else 'нет данных'}; "
        f"подтверждённые: {stable if stable is not None else 'нет данных'}\n"
        f"Сделки: открыто {stats['open_trades']}, ожидают входа "
        f"{stats['pending_trades']}, закрыто {stats['closed_trades']}\n"
        f"Прибыль: {stats['total_profit_usdt']:+.2f} USDT; за 7 дней "
        f"{stats['weekly_profit_usdt']:+.2f} USDT; плавающая "
        f"{stats['unrealized_profit_usdt'] if stats['unrealized_profit_usdt'] is not None else 'нет данных'}"
        f"{' USDT' if stats['unrealized_profit_usdt'] is not None else ''}\n"
        f"Победы / убытки: {stats['wins']} / {stats['losses']}; "
        f"доля прибыльных: {win_rate}; K/D {_format_ratio(stats['kd_ratio'], stats['wins'])}\n"
        f"Цена: {f'{price:g}' if price is not None else 'нет данных'}; обновлено "
        f"{_date_label(status['last_market_update_at'].get(symbol))}\n"
        f"Свежесть: цена {f'{price_age:.2f} с' if price_age is not None else 'нет данных'}; "
        f"последнее изменение Last Price {f'{price_value_age:.2f} с' if price_value_age is not None else 'нет данных'}; "
        f"стакан {f'{book_age:.2f} с' if book_age is not None else 'нет данных'}; "
        f"синхронизация {'да' if market_health.get('book_synchronized') else 'нет'}\n"
        f"Статус алгоритма: {_algorithm_label(status['algorithm_status'])}; "
        f"поток: {_feed_label(status['feed_status'].get(symbol, 'нет данных'))}\n"
        f"Причина отсутствия входа: {escape(engine.get_entry_block_reason(symbol))}\n"
        f"{error_line}"
    )


def dashboard_text(runtime: RuntimeController) -> str:
    status = runtime.engine.get_status()
    pairs = ", ".join(status["enabled_pairs"]) or "нет активных пар"
    feeds = ", ".join(
        f"{symbol}: {_feed_label(feed)}"
        for symbol, feed in status["feed_status"].items()
    ) or "нет потоков"
    market_health = status.get("market_data_health", {})
    freshness = ", ".join(
        _market_health_label(symbol, health)
        for symbol, health in market_health.items()
    ) or "нет данных"
    strategy_errors = status.get("strategy_errors", {})
    error_text = " | ".join(
        f"{symbol} {strategy_id}: {error_type}"
        for symbol, errors in strategy_errors.items()
        for strategy_id, error_type in errors.items()
    )
    strategy_error_line = (
        f"\nОшибки стратегий: {escape(error_text)}" if error_text else ""
    )
    feed_errors = status.get("feed_errors", {})
    feed_error_text = " | ".join(
        f"{symbol}: {escape(str(error['type']))}: {escape(str(error['message']))} "
        f"({_date_label(error.get('at'))})"
        for symbol, error in feed_errors.items() if error
    ) or "нет"
    analysis_active = [
        symbol for symbol, metric in status.get("market_analysis", {}).items()
        if metric.get("updated_at") is not None
    ]
    analysis_status = (
        "анализирует " + ", ".join(analysis_active)
        if analysis_active and status["algorithm_status"] == "working"
        else "ожидает обработку рынка" if status["algorithm_status"] == "waiting"
        else "остановлен" if status["algorithm_status"] == "stopped"
        else "ошибка"
    )
    timestamps = list(status.get("last_market_update_at", {}).values())
    last_update = max(
        timestamps,
        key=lambda value: value.timestamp() if value.tzinfo else value.replace(tzinfo=timezone.utc).timestamp(),
        default=None,
    )
    db_error = status.get("database_last_error")
    db_error_line = (
        f"Ошибка БД: {escape(str(db_error.get('type')))}: "
        f"{escape(str(db_error.get('message')))} ({_date_label(db_error.get('at'))})\n"
        if db_error else "Ошибка БД: нет\n"
    )
    cancelled_count = status.get("cancelled_trade_count")
    cleanup_at = status.get("last_cancelled_cleanup_at")
    cleanup_deleted = status.get("last_cancelled_cleanup_deleted")
    cancelled_line = (
        f"CANCELLED в PostgreSQL: {cancelled_count if cancelled_count is not None else 'нет данных'}\n"
        f"Последняя очистка: {_date_label(cleanup_at)}; удалено {cleanup_deleted if cleanup_deleted is not None else 'нет данных'}\n"
    )
    return (
        "<b>Админ-панель · виртуальная торговля</b>\n\n"
        f"Состояние: {_mode_label(status['mode'])}\n"
        f"Алгоритм: {_algorithm_label(status['algorithm_status'])}\n"
        f"Пары: {pairs}\n"
        f"Рыночные данные: {feeds}\n"
        f"Возраст данных: {freshness}\n"
        f"Поиск и анализ стен: {analysis_status}\n"
        f"Последнее обновление данных: {_date_label(last_update)}\n"
        f"Ошибки потока: {feed_error_text}\n"
        f"PostgreSQL: {_database_label(status['database_status'])}; "
        f"последний успех {_date_label(status['database_last_success_at'])}; "
        f"отложено записей: {status['persistence_pending']}\n"
        f"{cancelled_line}"
        f"Виртуальный баланс: {status['balance_usdt']:.2f} USDT\n"
        f"Плавающий P/L: {status['unrealized_profit_usdt']:+.2f} USDT · "
        f"оценка баланса: {status['equity_usdt']:.2f} USDT\n"
        f"Ставка на сделку: {status['stake_usdt']:.2f} USDT\n"
        "Комиссионная модель новых виртуальных сделок: Maker/Maker; "
        "ставка Maker пары фиксируется отдельно в каждой сделке.\n"
        "Общий лимит сделок не задан; новые входы зависят от сигналов и доступного баланса\n"
        f"Профит всего: {status['total_profit_usdt']:.2f} USDT\n"
        f"Профит за 7 дней: {status['weekly_profit_usdt']:.2f} USDT\n"
        f"Доходность: {status['total_return_percent']:+.2f}% · "
        f"прибыльных сделок: {status['win_rate_percent']:.1f}%\n"
        f"Доступно для новых сделок: {status['available_balance_usdt']:.2f} USDT\n"
        f"Зарезервировано маржи по OPEN: {status['reserved_margin_usdt']:.2f} USDT; "
        "PENDING маржу не резервируют\n"
        f"Победы / убытки: {status['wins']} / {status['losses']} · "
        f"K/D {_format_ratio(status['kd_ratio'], status['wins'])} · "
        f"K/D за 7 дней {_format_ratio(status['weekly_kd_ratio'], status['weekly_wins'])}\n"
        f"Сделки: открыто {status['open_trades']}, ожидают входа {status['pending_trades']}, "
        f"закрыто {status['closed_trades']}\n"
        f"Ожидают сохранения в БД: {status['persistence_pending']}\n"
        f"{db_error_line}{strategy_error_line}"
    )


def pair_settings_text(runtime: RuntimeController) -> str:
    pairs = []
    for symbol, settings in runtime.engine.pair_settings.items():
        state = "включена" if settings.enabled else "выключена"
        strategy_ids = runtime.engine.get_enabled_strategy_ids(symbol)
        strategies = ", ".join(sorted(strategy_ids)) or "нет"
        take_profit_bps, stop_loss_bps = effective_trade_level_bps(
            settings, runtime.engine.trading_settings
        )
        pairs.append(
            f"• {symbol}: {state}; стена {settings.min_wall_ratio:g}× / "
            f"от {settings.min_wall_notional_usdt:g} USDT; "
            f"дистанция поиска {settings.max_wall_distance_bps:g} б.п.; "
            f"вход {settings.entry_distance_bps:g} б.п.; "
            f"стоп за стеной {settings.stop_distance_bps:g} б.п.; "
            f"устойчивость {max(40.0, settings.min_wall_age_seconds):g} с; "
            f"порог потребления {settings.consumed_ratio:.0%}; "
            f"плечо {settings.leverage if settings.leverage is not None else runtime.engine.trading_settings.leverage:g}x; "
            f"комиссии Maker/Taker {settings.maker_fee_rate * 100:.4f}% / {settings.taker_fee_rate * 100:.4f}%; "
            "виртуальная модель Maker/Maker; "
            f"TP {take_profit_bps:g} / SL {stop_loss_bps:g} б.п.; "
            f"стратегии {escape(strategies)}"
        )
    return "\n".join(pairs) or "Торговые пары не настроены."


def _level_value_label(override: float | None, default: float) -> str:
    if override is None:
        return f"глобальное {default:g} б.п."
    return f"персональное {override:g} б.п."


def trade_level_pair_text(runtime: RuntimeController, symbol: str) -> str:
    symbol = symbol.strip().upper()
    pair = runtime.engine.pair_settings.get(symbol)
    if pair is None:
        return "Пара больше не подключена."
    trading = runtime.engine.trading_settings
    tp_bps, sl_bps = effective_trade_level_bps(pair, trading)
    leverage = pair.leverage or trading.leverage
    notional = trading.stake_usdt * leverage
    entry = 100.0
    tp_long = entry * (1 + tp_bps / 10000)
    sl_long = entry * (1 - sl_bps / 10000)
    tp_short = entry * (1 - tp_bps / 10000)
    sl_short = entry * (1 + sl_bps / 10000)
    long = estimate_trade_outcomes(
        side="long", entry_price=entry, take_profit_price=tp_long,
        stop_loss_price=sl_long, position_notional_usdt=notional,
        taker_fee_rate=pair.maker_fee_rate,
    )
    short = estimate_trade_outcomes(
        side="short", entry_price=entry, take_profit_price=tp_short,
        stop_loss_price=sl_short, position_notional_usdt=notional,
        taker_fee_rate=pair.maker_fee_rate,
    )
    strategies = runtime.engine.get_enabled_strategy_ids(symbol)
    warnings = []
    if "bounce" in strategies and sl_bps <= float(ENTRY_DISTANCE_BPS):
        warnings.append(
            "Bounce: SL может не выйти за стену; такие планы будут отклоняться."
        )
    if "breakout" in strategies and tp_bps <= float(ENTRY_DISTANCE_BPS):
        warnings.append(
            "Breakout: TP может не достичь стены; такие планы будут отклоняться."
        )
    lines = [
        f"<b>{escape(symbol)} · TP / SL</b>",
        f"TP: {_level_value_label(pair.take_profit_bps, trading.default_take_profit_bps)}",
        f"SL: {_level_value_label(pair.stop_loss_bps, trading.default_stop_loss_bps)}",
        f"Эффективно: TP {tp_bps:g} / SL {sl_bps:g} б.п. от Entry",
        f"Оценка: ставка {trading.stake_usdt:g} × {leverage:g}x = {notional:g} USDT; "
        f"Модель виртуальных комиссий: Maker/Maker — {pair.maker_fee_rate * 100:.4f}% на вход и выход. "
        f"Фактические тарифы пары: Maker {pair.maker_fee_rate * 100:.4f}% / "
        f"Taker {pair.taker_fee_rate * 100:.4f}%.",
        "Пример на Entry 100 USDT, до округления по tickSize:",
        f"LONG: TP до комиссий {long['tp_gross_pnl_usdt']:+.4f}, "
        f"TP после комиссий {long['tp_net_pnl_usdt']:+.4f}, "
        f"SL после комиссий {long['sl_net_pnl_usdt']:+.4f} USDT; "
        f"net TP/SL {long['net_reward_risk']:.3f}.",
        f"SHORT: TP до комиссий {short['tp_gross_pnl_usdt']:+.4f}, "
        f"TP после комиссий {short['tp_net_pnl_usdt']:+.4f}, "
        f"SL после комиссий {short['sl_net_pnl_usdt']:+.4f} USDT; "
        f"net TP/SL {short['net_reward_risk']:.3f}.",
        "Это расчётная оценка, не гарантия цены или исполнения.",
    ]
    lines.extend(f"⚠️ {escape(item)}" for item in warnings)
    return "\n".join(lines)


def trade_level_pair_keyboard(runtime: RuntimeController, symbol: str) -> InlineKeyboardMarkup:
    pair = runtime.engine.pair_settings[symbol]
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Изменить TP", callback_data=f"levels_set:pair:{symbol}:tp")],
        [InlineKeyboardButton(text="Изменить SL", callback_data=f"levels_set:pair:{symbol}:sl")],
        [InlineKeyboardButton(text="Сбросить TP к глобальному", callback_data=f"levels_reset:{symbol}:tp")],
        [InlineKeyboardButton(text="Сбросить SL к глобальному", callback_data=f"levels_reset:{symbol}:sl")],
        [InlineKeyboardButton(text="К списку пар", callback_data="levels_pairs")],
        [InlineKeyboardButton(text="Общие TP / SL", callback_data="settings_trade_levels")],
        [InlineKeyboardButton(text="Настройки", callback_data="settings")],
    ])


def trade_levels_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Изменить общий TP", callback_data="levels_set:global:tp")],
        [InlineKeyboardButton(text="Изменить общий SL", callback_data="levels_set:global:sl")],
        [InlineKeyboardButton(text="Настройки пар", callback_data="levels_pairs")],
        [InlineKeyboardButton(text="Назад", callback_data="settings")],
    ])


def trade_levels_text(runtime: RuntimeController) -> str:
    settings = runtime.engine.trading_settings
    lines = [
        "<b>Общие настройки TP / SL</b>",
        f"Глобальный TP от Entry: <b>{settings.default_take_profit_bps:g} б.п.</b>",
        f"Глобальный SL от Entry: <b>{settings.default_stop_loss_bps:g} б.п.</b>",
        "Пара может переопределить TP и SL независимо. Сброс отдельного значения "
        "возвращает его к глобальному.",
    ]
    for symbol, pair in runtime.engine.pair_settings.items():
        tp, sl = effective_trade_level_bps(pair, settings)
        lines.append(f"{escape(symbol)}: эффективно TP {tp:g} / SL {sl:g} б.п.")
    return "\n".join(lines)


def trade_levels_pairs_keyboard(runtime: RuntimeController) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(
        text=f"{symbol} · TP {effective_trade_level_bps(pair, runtime.engine.trading_settings)[0]:g} / "
             f"SL {effective_trade_level_bps(pair, runtime.engine.trading_settings)[1]:g} б.п.",
        callback_data=f"levels_pair:{symbol}",
    )] for symbol, pair in runtime.engine.pair_settings.items()]
    rows.append([InlineKeyboardButton(text="Общие TP / SL", callback_data="settings_trade_levels")])
    rows.append([InlineKeyboardButton(text="Настройки", callback_data="settings")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@admin.message(F.chat.type == "private", Command("id"))
async def dayid(message: Message):
    tg_id = message.from_user.id
    await message.answer(f"{tg_id}")

@admin.message(Admin(), CommandStart())
async def cmd_start(
    message: Message, runtime: RuntimeController, state: FSMContext
) -> None:
    await state.clear()
    await message.answer(dashboard_text(runtime), reply_markup=kb.main)


@admin.callback_query(Admin(), F.data == "cancel_input")
async def cancel_input(
    callback: CallbackQuery, runtime: RuntimeController, state: FSMContext
) -> None:
    await state.clear()
    await _refresh_dashboard_after_action(callback, runtime)
    await callback.answer("Ввод отменён")


@admin.callback_query(Admin(), F.data == "settings")
async def show_settings(
    callback: CallbackQuery, runtime: RuntimeController, state: FSMContext
) -> None:
    await state.clear()
    pair_text = pair_settings_text(runtime)
    if callback.message is not None:
        await edit_message_if_changed(
            callback.message,
            "<b>Настройки виртуальной торговли</b>\n\n"
            f"Ставка на сделку: {runtime.engine.trading_settings.stake_usdt:.2f} USDT\n"
            f"Начальный баланс: {runtime.engine.trading_settings.initial_balance_usdt:.2f} USDT\n\n"
            f"Глобальный TP / SL: "
            f"{runtime.engine.trading_settings.default_take_profit_bps:g} / "
            f"{runtime.engine.trading_settings.default_stop_loss_bps:g} б.п. от Entry\n\n"
            f"<b>Пары</b>\n{pair_text}\n\n"
            "Настройки сохраняются в PostgreSQL. Пороги, дистанции и плечо "
            "настраиваются отдельно для каждой пары; плечо новых сделок "
            "не меняет уже созданные. Стратегии пары можно "
            "включить или отключить отдельной кнопкой.",
            reply_markup=kb.settings,
        )
    await callback.answer()


@admin.callback_query(Admin(), F.data == "settings_stake")
async def request_stake(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(SettingsInput.waiting_for_stake)
    if callback.message is not None:
        await callback.message.answer(
            "Отправьте новую ставку в USDT одним числом, например: <code>25</code>.",
            reply_markup=kb.cancel_input,
        )
    await callback.answer()


@admin.message(Admin(), StateFilter(SettingsInput.waiting_for_stake))
async def update_stake(message: Message, state: FSMContext, runtime: RuntimeController) -> None:
    try:
        stake = float(message.text or "")
        await runtime.update_stake(stake)
    except (ValueError, TypeError):
        await message.answer("Введите положительную сумму в USDT, например <code>25</code>.")
        return
    except Exception:
        await message.answer("Не удалось сохранить ставку в базе данных.")
        return
    await state.clear()
    await message.answer(dashboard_text(runtime), reply_markup=kb.main)


@admin.callback_query(Admin(), F.data == "settings_trade_levels")
async def show_trade_level_settings(
    callback: CallbackQuery, runtime: RuntimeController, state: FSMContext
) -> None:
    await state.clear()
    if callback.message is not None:
        await edit_message_if_changed(
            callback.message, trade_levels_text(runtime), trade_levels_keyboard()
        )
    await callback.answer()


@admin.callback_query(Admin(), F.data == "levels_pairs")
async def show_trade_level_pairs(
    callback: CallbackQuery, runtime: RuntimeController, state: FSMContext
) -> None:
    await state.clear()
    if callback.message is not None:
        await edit_message_if_changed(
            callback.message,
            "<b>Настройки TP / SL по парам</b>\nВыберите подключённую пару.",
            trade_levels_pairs_keyboard(runtime),
        )
    await callback.answer()


@admin.callback_query(Admin(), F.data.startswith("levels_pair:"))
async def show_trade_level_pair(
    callback: CallbackQuery, runtime: RuntimeController, state: FSMContext
) -> None:
    await state.clear()
    symbol = callback.data.split(":", 1)[1].strip().upper()
    pair = runtime.engine.pair_settings.get(symbol)
    if pair is None:
        await callback.answer("Пара больше не подключена", show_alert=True)
        return
    if callback.message is not None:
        await edit_message_if_changed(
            callback.message,
            trade_level_pair_text(runtime, symbol),
            trade_level_pair_keyboard(runtime, symbol),
        )
    await callback.answer()


@admin.callback_query(Admin(), F.data.startswith("levels_set:"))
async def request_trade_level_value(
    callback: CallbackQuery, state: FSMContext, runtime: RuntimeController
) -> None:
    parts = callback.data.split(":")
    if len(parts) not in {3, 4} or parts[1] not in {"global", "pair"}:
        await callback.answer("Некорректная настройка", show_alert=True)
        return
    scope = parts[1]
    if scope == "global":
        if len(parts) != 3:
            await callback.answer("Некорректная настройка", show_alert=True)
            return
        field = parts[2]
        symbol = None
    else:
        if len(parts) != 4:
            await callback.answer("Некорректная настройка", show_alert=True)
            return
        symbol, field = parts[2].strip().upper(), parts[3]
        if symbol not in runtime.engine.pair_settings:
            await callback.answer("Пара больше не подключена", show_alert=True)
            return
    if field not in {"tp", "sl"}:
        await callback.answer("Неизвестный параметр", show_alert=True)
        return
    await state.update_data(level_scope=scope, level_symbol=symbol, level_field=field)
    await state.set_state(SettingsInput.waiting_for_trade_level)
    prompt = (
        "Введите положительное значение в bps, например <code>5</code>."
        if scope == "global" else
        f"Введите персональное значение {field.upper()} для <code>{escape(symbol)}</code> в bps."
    )
    if callback.message is not None:
        await callback.message.answer(
            prompt + "\nДля возврата к глобальному значению используйте отдельную кнопку сброса.",
            reply_markup=kb.cancel_input,
        )
    await callback.answer()


@admin.callback_query(Admin(), F.data.startswith("levels_reset:"))
async def reset_pair_trade_level(
    callback: CallbackQuery, runtime: RuntimeController, state: FSMContext
) -> None:
    parts = callback.data.split(":")
    if len(parts) != 3 or parts[2] not in {"tp", "sl"}:
        await callback.answer("Некорректная настройка", show_alert=True)
        return
    symbol, field = parts[1].strip().upper(), parts[2]
    current = runtime.engine.pair_settings.get(symbol)
    if current is None:
        await callback.answer("Пара больше не подключена", show_alert=True)
        return
    attribute = "take_profit_bps" if field == "tp" else "stop_loss_bps"
    try:
        await runtime.update_pair_settings(replace(current, **{attribute: None}))
    except Exception:
        logger.exception("Could not reset %s bps override for %s", field, symbol)
        await callback.answer("Не удалось сохранить настройку; состояние не изменено", show_alert=True)
        return
    if callback.message is not None:
        await edit_message_if_changed(
            callback.message,
            trade_level_pair_text(runtime, symbol),
            trade_level_pair_keyboard(runtime, symbol),
        )
    await callback.answer(f"{field.upper()} снова использует глобальное значение")


@admin.message(Admin(), StateFilter(SettingsInput.waiting_for_trade_level))
async def update_trade_level_value(
    message: Message, state: FSMContext, runtime: RuntimeController
) -> None:
    data = await state.get_data()
    try:
        value = float(message.text or "")
        if not isfinite(value) or value <= 0:
            raise ValueError("Значение должно быть положительным конечным числом")
        scope, symbol, field = (
            data.get("level_scope"), data.get("level_symbol"), data.get("level_field")
        )
        if field not in {"tp", "sl"} or scope not in {"global", "pair"}:
            raise ValueError("Сессия настройки устарела; откройте меню заново")
        if scope == "global":
            await runtime.update_trade_level_defaults(
                take_profit_bps=value if field == "tp" else None,
                stop_loss_bps=value if field == "sl" else None,
            )
            text = trade_levels_text(runtime)
            markup = trade_levels_keyboard()
        else:
            current = runtime.engine.pair_settings.get(str(symbol).upper())
            if current is None:
                raise ValueError("Пара больше не подключена")
            attribute = "take_profit_bps" if field == "tp" else "stop_loss_bps"
            await runtime.update_pair_settings(replace(current, **{attribute: value}))
            text = trade_level_pair_text(runtime, str(symbol))
            markup = trade_level_pair_keyboard(runtime, str(symbol).upper())
    except (ValueError, TypeError) as error:
        await message.answer(f"Настройка отклонена: {escape(str(error))}")
        return
    except Exception:
        logger.exception("Could not save TP/SL settings")
        await message.answer("Не удалось сохранить TP/SL и связанные планы; прежние настройки сохранены.")
        return
    await state.clear()
    await message.answer(text, reply_markup=markup)


@admin.callback_query(Admin(), F.data == "settings_pair")
async def request_pair_settings(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(SettingsInput.waiting_for_pair)
    if callback.message is not None:
        await callback.message.answer(
            "Отправьте: <code>ПАРА параметр значение</code>.\n"
            "Параметры: <code>enabled, min_wall_ratio, min_wall_notional_usdt, max_wall_distance_bps, "
            "min_wall_age_seconds, consumed_ratio, entry_distance_bps, "
            "stop_distance_bps, bounce_reversal_bps, "
            "breakout_confirm_seconds, breakout_buffer_bps, "
            "breakout_max_update_gap_seconds</code>.\n"
            "Пример: <code>BTCUSDT entry_distance_bps 1.5</code>",
            reply_markup=kb.cancel_input,
        )
    await callback.answer()


@admin.callback_query(Admin(), F.data == "settings_add_pair")
async def request_add_pair(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(SettingsInput.waiting_for_new_pair)
    if callback.message is not None:
        await callback.message.answer(
            "Отправьте символ линейного USDT-контракта Bybit, например "
            "<code>ETHUSDT</code>. Пара добавится с настройками по умолчанию; "
            "параметры можно будет изменить в разделе «Настроить пару».",
            reply_markup=kb.cancel_input,
        )
    await callback.answer()


@admin.callback_query(Admin(), F.data == "settings_strategy")
async def request_strategy_settings(
    callback: CallbackQuery, state: FSMContext, runtime: RuntimeController
) -> None:
    await state.clear()
    rows = []
    statuses = []
    for symbol in runtime.engine.pair_settings:
        selected = runtime.engine.get_enabled_strategy_ids(symbol)
        mode = _strategy_label(selected)
        feed = _feed_label(runtime.engine.feed_status.get(symbol, "нет данных"))
        statuses.append(f"{symbol}: {mode} · данные: {feed}")
        version = runtime.engine.strategy_versions.get(symbol, 0)
        rows.append([InlineKeyboardButton(
            text=f"{symbol} · {mode}", callback_data=f"stratpair:{symbol}:{version}"
        )])
    rows.append([InlineKeyboardButton(text="На главную", callback_data="dashboard")])
    if callback.message is not None:
        await edit_message_if_changed(
            callback.message,
            "<b>Стратегии по парам</b>\n\n" + ("\n".join(statuses) or "Пары не настроены."),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
        )
    await callback.answer()


def _strategy_label(strategy_ids: set[str]) -> str:
    if not strategy_ids:
        return "Отключено"
    names = {"bounce": "Отскок", "breakout": "Пробой", "stable_wall_v1": "Устаревший режим"}
    return ", ".join(names.get(item, item) for item in sorted(strategy_ids))


@admin.callback_query(Admin(), F.data.startswith("stratpair:"))
async def choose_strategy_pair(callback: CallbackQuery, runtime: RuntimeController) -> None:
    _, raw_symbol, _ = (callback.data or "").split(":", 2)
    symbol = raw_symbol.upper()
    if symbol not in runtime.engine.pair_settings:
        await callback.answer("Пара больше не настроена", show_alert=True)
        return
    current = runtime.engine.get_enabled_strategy_ids(symbol)
    buttons = [
        [InlineKeyboardButton(text=("✓ " if current == {strategy_id} else "") + label,
                              callback_data=f"stratset:{symbol}:{strategy_id}:{runtime.engine.strategy_versions.get(symbol, 0)}")]
        for strategy_id, label in (("bounce", "Отскок от стены"), ("breakout", "Пробой стены"), ("off", "Отключено"))
    ]
    buttons.append([InlineKeyboardButton(text="К списку пар", callback_data="settings_strategy")])
    if callback.message is not None:
        await edit_message_if_changed(
            callback.message,
            f"<b>{escape(symbol)}</b> · выбран режим: {_strategy_label(current)}\nВыберите режим:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        )
    await callback.answer()


@admin.callback_query(Admin(), F.data.startswith("stratset:"))
async def set_pair_strategy(callback: CallbackQuery, runtime: RuntimeController) -> None:
    parts = (callback.data or "").split(":", 3)
    if len(parts) != 4:
        await callback.answer("Некорректная команда. Обновите список пар.", show_alert=True)
        return
    _, raw_symbol, strategy_id, raw_version = parts
    symbol = raw_symbol.upper()
    try:
        expected_version = int(raw_version)
    except ValueError:
        await callback.answer("Некорректная версия настройки", show_alert=True)
        return
    if symbol not in runtime.engine.pair_settings or strategy_id not in {"bounce", "breakout", "off"}:
        await callback.answer("Некорректная пара или стратегия", show_alert=True)
        return
    selected = set() if strategy_id == "off" else {strategy_id}
    worker_error = False
    try:
        await runtime.update_enabled_strategies(
            symbol, selected, callback.from_user.id, expected_version
        )
    except StrategyVersionConflict:
        await callback.answer(
            "Настройка пары уже изменилась. Откройте список пар заново.",
            show_alert=True,
        )
        return
    except StrategyPersistenceError:
        await callback.answer(
            "Не удалось сохранить режим в PostgreSQL. Настройка не применена; техническая причина записана в журнал.",
            show_alert=True,
        )
        return
    except StrategyWorkerSyncError:
        # The database commit and in-memory selection already succeeded.
        worker_error = True
    except Exception:
        logger.exception("Unexpected strategy switch failure for %s", symbol)
        await callback.answer(
            "Не удалось применить режим. Техническая причина записана в журнал.",
            show_alert=True,
        )
        return
    current_selection = runtime.engine.get_enabled_strategy_ids(symbol)
    mode_text = _strategy_label(current_selection)
    feed_text = _feed_label(runtime.engine.feed_status.get(symbol, "нет данных"))
    confirmation = (
        f"Режим {mode_text} для {symbol} сохранён."
        if not worker_error
        else f"Режим {mode_text} для {symbol} сохранён, но поток данных требует проверки ({feed_text})."
    )
    try:
        await callback.answer(confirmation, show_alert=worker_error)
    except Exception:
        logger.exception("Telegram could not acknowledge saved strategy for %s", symbol)
    rows = [
        [InlineKeyboardButton(text=("✓ " if current_selection == {key} else "") + label,
                              callback_data=f"stratset:{symbol}:{key}:{runtime.engine.strategy_versions.get(symbol, 0)}")]
        for key, label in (("bounce", "Отскок от стены"), ("breakout", "Пробой стены"), ("off", "Отключено"))
    ]
    rows.append([InlineKeyboardButton(text="К списку пар", callback_data="settings_strategy")])
    if callback.message is not None:
        text = (
            f"<b>{escape(symbol)}</b> · режим сохранён: {escape(mode_text)}\n"
            f"Данные: {escape(feed_text)}\n"
            "Ожидающие входы прежней стратегии отменены; открытые виртуальные сделки продолжают сопровождаться."
        )
        markup = InlineKeyboardMarkup(inline_keyboard=rows)
        try:
            await edit_message_if_changed(callback.message, text, reply_markup=markup)
        except Exception as error:
            logger.error(
                "Strategy for %s was saved, but Telegram message refresh failed (%s)",
                symbol, type(error).__name__,
            )
            try:
                await callback.message.answer(
                    f"Режим {_strategy_label(current_selection)} для {escape(symbol)} сохранён в PostgreSQL. "
                    "Сообщение панели не обновилось; откройте раздел «Стратегии по парам» заново.",
                    reply_markup=markup,
                )
            except Exception as error:
                logger.error(
                    "Could not send fallback confirmation for saved strategy %s (%s)",
                    symbol, type(error).__name__,
                )


@admin.message(Admin(), StateFilter(SettingsInput.waiting_for_strategy))
async def update_pair_strategy(
    message: Message, state: FSMContext, runtime: RuntimeController
) -> None:
    parts = (message.text or "").split()
    if len(parts) != 3 or parts[2].lower() not in {"on", "off"}:
        await message.answer("Формат: <code>ПАРА strategy_id on|off</code>.")
        return
    symbol, strategy_id, action = parts[0].upper(), parts[1], parts[2].lower()
    if symbol not in runtime.engine.pair_settings:
        await message.answer("Сначала добавьте эту пару в настройках.")
        return
    if strategy_id not in runtime.engine.strategy_registry.strategy_ids:
        await message.answer("Неизвестный strategy_id. Проверьте список в подсказке.")
        return
    selected = {strategy_id} if action == "on" else set()
    try:
        await runtime.update_enabled_strategies(symbol, selected, message.from_user.id if message.from_user else None)
    except KeyError:
        await message.answer("Неизвестный strategy_id. Проверьте список в подсказке.")
        return
    except Exception:
        await message.answer("Не удалось сохранить стратегии пары в базе данных.")
        return
    await state.clear()
    await message.answer(
        f"Стратегии для {symbol} сохранены: "
        f"{escape(', '.join(sorted(selected)) or 'нет')}\n\n"
        + dashboard_text(runtime),
        reply_markup=kb.main,
    )


@admin.message(Admin(), StateFilter(SettingsInput.waiting_for_new_pair))
async def add_pair(
    message: Message,
    state: FSMContext,
    runtime: RuntimeController,
) -> None:
    try:
        settings = await runtime.add_pair(message.text or "")
    except ValueError as error:
        await message.answer(f"Не удалось добавить пару: {error}")
        return
    except Exception:
        await message.answer("Не удалось сохранить новую пару в базе данных.")
        return
    await state.clear()
    await message.answer(
        f"Пара {settings.symbol} добавлена.\n\n" + dashboard_text(runtime),
        reply_markup=kb.main,
    )


@admin.message(Admin(), StateFilter(SettingsInput.waiting_for_pair))
async def update_pair_settings(
    message: Message,
    state: FSMContext,
    runtime: RuntimeController,
) -> None:
    parts = (message.text or "").split()
    if len(parts) != 3:
        await message.answer("Неверный формат. Пример: <code>BTCUSDT entry_distance_bps 1.5</code>.")
        return
    symbol, parameter, raw_value = parts[0].upper(), parts[1], parts[2]
    current = runtime.engine.pair_settings.get(symbol)
    if current is None:
        await message.answer(
            "Эта пара не добавлена. Сначала добавьте её кнопкой "
            "«Добавить USDT-пару» в настройках."
        )
        return

    numeric_fields = {
        "min_wall_ratio", "min_wall_notional_usdt", "max_wall_distance_bps", "min_wall_age_seconds",
        "consumed_ratio", "entry_distance_bps", "stop_distance_bps", "bounce_reversal_bps",
        "breakout_confirm_seconds", "breakout_buffer_bps", "breakout_max_update_gap_seconds",
    }
    if parameter == "enabled":
        if raw_value.lower() not in {"true", "false", "1", "0", "on", "off"}:
            await message.answer("Для enabled используйте true/false.")
            return
        value = raw_value.lower() in {"true", "1", "on"}
    elif parameter in numeric_fields:
        try:
            value = float(raw_value)
        except ValueError:
            await message.answer("Для этого параметра нужно числовое значение.")
            return
        if parameter == "min_wall_age_seconds" and value < 40:
            await message.answer("Минимальная устойчивость стены для стратегий — 40 секунд.")
            return
    else:
        await message.answer("Неизвестный параметр. Используйте список из подсказки.")
        return

    try:
        updated = replace(current, **{parameter: value})
        await runtime.update_pair_settings(updated)
    except ValueError as error:
        await message.answer(f"Настройка отклонена: {error}")
        return
    except Exception:
        await message.answer("Не удалось сохранить настройки пары в базе данных.")
        return
    await state.clear()
    await message.answer("Настройки пары сохранены.\n\n" + dashboard_text(runtime), reply_markup=kb.main)


@admin.callback_query(Admin(), F.data == "moreinfo")
async def show_statistics(
    callback: CallbackQuery, runtime: RuntimeController, state: FSMContext
) -> None:
    await state.clear()
    stats = runtime.engine.get_status()
    if callback.message is not None:
        await edit_message_if_changed(
            callback.message,
            "<b>Статистика виртуальных сделок</b>\n\n"
            f"Закрыто сделок: {stats['closed_trades']}\n"
            f"Прибыль: {stats['total_profit_usdt']:.2f} USDT\n"
            f"Профит за 7 дней: {stats['weekly_profit_usdt']:.2f} USDT\n"
            f"Побед / убытков: {stats['wins']} / {stats['losses']}\n"
            f"K/D всего: {_format_ratio(stats['kd_ratio'], stats['wins'])}\n"
            f"K/D за 7 дней: {_format_ratio(stats['weekly_kd_ratio'], stats['weekly_wins'])}\n"
            f"Доходность к начальному балансу: {stats['total_return_percent']:.2f}%\n"
            f"Доля прибыльных: {stats['win_rate_percent']:.1f}%\n"
            f"Плавающий P/L: {stats['unrealized_profit_usdt']:+.2f} USDT\n"
            f"Доступно для новых сделок: {stats['available_balance_usdt']:.2f} USDT\n"
            f"Открыто / ожидают входа: {stats['open_trades']} / {stats['pending_trades']}\n"
            f"Баланс: {stats['balance_usdt']:.2f} USDT",
            reply_markup=kb.back,
        )
    await callback.answer()


_DEALS_PAGE_SIZE = 5


def _deal_token(trade: dict) -> str:
    return hashlib.sha256(str(trade.get("signal_id", "")).encode()).hexdigest()[:20]


def _trade_for_token(runtime: RuntimeController, token: str) -> dict | None:
    matches = [
        trade for trade in runtime.engine.trade_manager.trades
        if _deal_token(trade) == token
    ]
    return matches[0] if len(matches) == 1 else None


def _strategy_name(strategy_id: str) -> str:
    return {"bounce": "Bounce", "breakout": "Breakout"}.get(
        strategy_id, strategy_id or "нет данных"
    )


def _wall_side_label(wall_side: str) -> str:
    return {"buy": "BID", "sell": "ASK"}.get(str(wall_side).lower(), str(wall_side).upper())


def _deal_list_text(runtime: RuntimeController, section: str, page: int) -> tuple[str, InlineKeyboardMarkup]:
    manager = runtime.engine.trade_manager
    trades = manager.get_open_trades() if section == "open" else manager.get_pending_trades()
    trades = sorted(trades, key=lambda item: (str(item.get("created_at") or ""), item.get("signal_id", "")))
    page_count = max(1, (len(trades) + _DEALS_PAGE_SIZE - 1) // _DEALS_PAGE_SIZE)
    page = max(0, min(page, page_count - 1))
    title = "Открытые · OPEN" if section == "open" else "Запланированные · PENDING"
    lines = [f"<b>Менеджер сделок · {title}</b> · стр. {page + 1}/{page_count}", f"Всего: {len(trades)}"]
    start = page * _DEALS_PAGE_SIZE
    for trade in trades[start:start + _DEALS_PAGE_SIZE]:
        token = _deal_token(trade)
        price = manager.last_prices.get(trade["symbol"])
        if section == "open":
            pnl = manager.trade_unrealized_pnl(trade, price) if price is not None else None
            measure = f"PnL {pnl:+.2f} USDT" if pnl is not None else "PnL нет данных"
        else:
            measure = f"Entry {float(trade['entry_price']):g} · маржа при входе {float(trade['stake_usdt']):.2f}"
        lines.append(
            f"• <code>{escape(trade['symbol'])}</code> {_strategy_name(trade.get('strategy_id'))} "
            f"{trade.get('side', '').upper()} · {measure}"
        )
    if not trades:
        lines.append("Сделок в этом разделе нет.")
    rows = [
        [InlineKeyboardButton(text=f"{escape(t['symbol'])} · {_strategy_name(t.get('strategy_id'))} · {t.get('side','').upper()}", callback_data=f"deal:view:{_deal_token(t)}")]
        for t in trades[start:start + _DEALS_PAGE_SIZE]
    ]
    rows.append([
        InlineKeyboardButton(text="OPEN", callback_data="deals:open:0"),
        InlineKeyboardButton(text="PENDING", callback_data="deals:pending:0"),
    ])
    rows.append([
        InlineKeyboardButton(text="Назад", callback_data=f"deals:{section}:{max(0, page - 1)}"),
        InlineKeyboardButton(text=f"{page + 1}/{page_count}", callback_data=f"deals:{section}:{page}"),
        InlineKeyboardButton(text="Вперёд", callback_data=f"deals:{section}:{min(page_count - 1, page + 1)}"),
    ])
    rows.append([InlineKeyboardButton(text="На главную", callback_data="dashboard")])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows)


def _deal_card(runtime: RuntimeController, trade: dict) -> tuple[str, InlineKeyboardMarkup]:
    manager = runtime.engine.trade_manager
    status = str(trade.get("status", "unknown")).upper()
    symbol = escape(str(trade.get("symbol", "нет данных")))
    strategy = _strategy_name(str(trade.get("strategy_id", "")))
    side = str(trade.get("side", "нет данных")).upper()
    price = manager.last_prices.get(str(trade.get("symbol", "")).upper())
    commission_model = str(trade.get("commission_model") or "LEGACY_SAVED_RATE")
    applied_fee_rate = trade.get("applied_fee_rate")
    if applied_fee_rate is None:
        applied_fee_rate = trade.get("fee_rate") or 0.0
    if trade.get("maker_fee_rate") is not None and trade.get("taker_fee_rate") is not None:
        commission_tariffs = (
            f"Maker {float(trade['maker_fee_rate']) * 100:.4f}% / "
            f"Taker {float(trade['taker_fee_rate']) * 100:.4f}%"
        )
    else:
        commission_tariffs = "исходные тарифы не сохранены"
    lines = [
        f"<b>{symbol} · {strategy} · {side}</b>",
        f"ID: <code>{escape(str(trade.get('signal_id', 'нет данных')))}</code>",
        f"Статус: <b>{status}</b>",
        f"Создана: {_date_label(trade.get('created_at'))}",
        f"Entry: {float(trade.get('entry_price') or 0):g} · текущая цена: {price if price is not None else 'нет данных'}",
        f"TP: {float(trade.get('take_profit') or 0):g} · SL: {float(trade.get('stop_loss') or 0):g}",
    ]
    if status == "OPEN":
        notional = float(trade.get("position_notional_usdt") or trade.get("stake_usdt") or 0)
        pnl = manager.trade_unrealized_pnl(trade, price) if price is not None else None
        lines.extend([
            f"Плечо: {float(trade.get('leverage') or 1):g}x · маржа: {float(trade.get('stake_usdt') or 0):.2f} USDT",
            f"Номинал: {notional:.2f} USDT · плавающий PnL: {f'{pnl:+.4f} USDT' if pnl is not None else 'нет данных'}",
            f"Комиссия {escape(commission_model)}: применено "
            f"{float(applied_fee_rate) * 100:.4f}% на вход/выход; {commission_tariffs}",
            f"Комиссия входа: {float(trade.get('entry_fee_usdt') or 0):.6f} USDT · ожидаемая комиссия выхода: "
            + (f"{notional * float(price) / float(trade.get('entry_fill_price') or trade.get('entry_price')) * float(applied_fee_rate):.6f} USDT" if price is not None else "нет данных"),
            f"Wall: {_wall_side_label(trade.get('wall_side', ''))} {float(trade.get('wall_price') or 0):g} · {escape(str(trade.get('wall_id') or 'нет ID'))}",
        ])
        rows = [
            [InlineKeyboardButton(text="Закрыть сделку", callback_data=f"deal:confirm_close:{_deal_token(trade)}")],
            [InlineKeyboardButton(text="Изменить TP", callback_data=f"deal:edit_tp:{_deal_token(trade)}"), InlineKeyboardButton(text="Изменить SL", callback_data=f"deal:edit_sl:{_deal_token(trade)}")],
            [InlineKeyboardButton(text="К OPEN", callback_data="deals:open:0")],
        ]
    elif status == "PENDING":
        distance = abs(float(price) - float(trade["entry_price"])) / float(trade["entry_price"]) * 10000 if price is not None else None
        lines.extend([
            f"Стабильная стена: {_wall_side_label(trade.get('wall_side', ''))} {float(trade.get('wall_price') or 0):g} · объём {float(trade.get('wall_size') or 0):g} · ratio {float(trade.get('wall_ratio') or 0):g}x",
            f"Расстояние до Entry: {f'{distance:.2f} bps' if distance is not None else 'нет данных'}",
            f"Триггер: {'ARMED' if trade.get('entry_armed') else 'WAITING'} · направление: {escape(str(trade.get('entry_trigger_direction') or 'нет данных'))}",
            f"Маржа при активации: {float(trade.get('stake_usdt') or 0):.2f} USDT · "
            f"сейчас не резервируется · плечо {float(trade.get('leverage') or 1):g}x",
            f"Диагностика: reference={trade.get('entry_previous_price') if trade.get('entry_previous_price') is not None else 'нет данных'}; wall_id={escape(str(trade.get('wall_id') or 'нет данных'))}",
        ])
        rows = [[InlineKeyboardButton(text="Отменить план", callback_data=f"deal:confirm_cancel:{_deal_token(trade)}")], [InlineKeyboardButton(text="К PENDING", callback_data="deals:pending:0")]]
    else:
        lines.extend([
            f"Стена: {escape(str(trade.get('wall_side', '')).upper())} {float(trade.get('wall_price') or 0):g}",
            f"Финансовый результат: {float(trade.get('profit_usdt') or 0):+.4f} USDT",
            f"Причина завершения: {escape(str(trade.get('close_reason') or 'нет данных'))}",
            f"Закрыта: {_date_label(trade.get('closed_at'))}",
        ])
        rows = [[InlineKeyboardButton(text="К OPEN", callback_data="deals:open:0")], [InlineKeyboardButton(text="К PENDING", callback_data="deals:pending:0")]]
    rows.append([InlineKeyboardButton(text="Обновить карточку", callback_data=f"deal:view:{_deal_token(trade)}")])
    rows.append([InlineKeyboardButton(text="В менеджер", callback_data="deals:home")])
    text = "\n".join(lines)
    if len(text) > 4096:
        raise ValueError("Trade card exceeded Telegram message limit")
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


async def _refresh_deal_after_stale(callback: CallbackQuery, runtime: RuntimeController, token: str) -> None:
    if callback.message is None:
        return
    trade = _trade_for_token(runtime, token)
    if trade is not None:
        text, keyboard = _deal_card(runtime, trade)
    else:
        text, keyboard = _deal_list_text(runtime, "open", 0)
    await edit_message_if_changed(callback.message, text, keyboard)


@admin.callback_query(Admin(), F.data == "deals:home")
async def show_deal_manager(callback: CallbackQuery, runtime: RuntimeController, state: FSMContext) -> None:
    await state.clear()
    if callback.message is not None:
        text, keyboard = _deal_list_text(runtime, "open", 0)
        await edit_message_if_changed(callback.message, text, keyboard)
    await callback.answer()


@admin.callback_query(Admin(), F.data.startswith("deals:"))
async def show_deal_list(callback: CallbackQuery, runtime: RuntimeController, state: FSMContext) -> None:
    parts = callback.data.split(":")
    if len(parts) != 3 or parts[1] not in {"open", "pending"}:
        await callback.answer("Некорректный раздел", show_alert=True)
        return
    try:
        page = max(0, int(parts[2]))
    except ValueError:
        page = 0
    await state.clear()
    if callback.message is not None:
        text, keyboard = _deal_list_text(runtime, parts[1], page)
        await edit_message_if_changed(callback.message, text, keyboard)
    await callback.answer()


@admin.callback_query(Admin(), F.data.startswith("deal:view:"))
async def show_deal_card(callback: CallbackQuery, runtime: RuntimeController, state: FSMContext) -> None:
    await state.clear()
    trade = _trade_for_token(runtime, callback.data.rsplit(":", 1)[-1])
    if trade is None:
        await callback.answer("Сделка не найдена. Обновите список.", show_alert=True)
        return
    if callback.message is not None:
        text, keyboard = _deal_card(runtime, trade)
        await edit_message_if_changed(callback.message, text, keyboard)
    await callback.answer()


@admin.callback_query(Admin(), F.data.startswith("deal:confirm_close:"))
async def confirm_close_deal(callback: CallbackQuery, runtime: RuntimeController, state: FSMContext) -> None:
    token = callback.data.rsplit(":", 1)[-1]
    trade = _trade_for_token(runtime, token)
    if trade is None or trade.get("status") != "open":
        await _refresh_deal_after_stale(callback, runtime, token)
        await callback.answer("Сделка уже изменилась. Обновите список.", show_alert=True)
        return
    token = _deal_token(trade)
    if callback.message is not None:
        await edit_message_if_changed(
            callback.message,
            f"Закрыть виртуальную сделку <code>{escape(str(trade['symbol']))}</code> "
            f"{trade['side'].upper()} по текущей цене {runtime.engine.trade_manager.last_prices.get(trade['symbol'], 'нет данных')}? "
            "PnL и комиссия будут рассчитаны штатным механизмом TradeManager.",
            InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Подтвердить закрытие", callback_data=f"deal:close:{token}")],
                [InlineKeyboardButton(text="Отмена", callback_data=f"deal:view:{token}")],
            ]),
        )
    await callback.answer()


@admin.callback_query(Admin(), F.data.startswith("deal:close:"))
async def close_one_deal(callback: CallbackQuery, runtime: RuntimeController, state: FSMContext) -> None:
    token = callback.data.rsplit(":", 1)[-1]
    trade = _trade_for_token(runtime, token)
    if trade is None or trade.get("status") != "open":
        await _refresh_deal_after_stale(callback, runtime, token)
        await callback.answer("Сделка уже изменилась; список обновлён", show_alert=True)
        return
    signal_id = trade["signal_id"]
    try:
        await runtime.close_virtual_trade(signal_id)
    except Exception as error:
        logger.exception("Manual virtual close failed for %s", signal_id)
        trade = _trade_for_token(runtime, _deal_token(trade))
        if callback.message is not None and trade is not None:
            text, keyboard = _deal_card(runtime, trade)
            await edit_message_if_changed(callback.message, f"{escape(str(error))}\n\n{text}", keyboard)
        await callback.answer("Не удалось закрыть; показано актуальное состояние", show_alert=True)
        return
    await callback.answer("Сделка закрыта по доступной виртуальной цене")
    if callback.message is not None:
        text, keyboard = _deal_list_text(runtime, "open", 0)
        await edit_message_if_changed(callback.message, text, keyboard)


@admin.callback_query(Admin(), F.data.startswith("deal:confirm_cancel:"))
async def confirm_cancel_plan(callback: CallbackQuery, runtime: RuntimeController, state: FSMContext) -> None:
    token = callback.data.rsplit(":", 1)[-1]
    trade = _trade_for_token(runtime, token)
    if trade is None or trade.get("status") != "pending":
        await _refresh_deal_after_stale(callback, runtime, token)
        await callback.answer("План уже изменился. Обновите список.", show_alert=True)
        return
    token = _deal_token(trade)
    if callback.message is not None:
        await edit_message_if_changed(
            callback.message,
            f"Отменить PENDING <code>{escape(str(trade['symbol']))}</code> на стену "
            f"{float(trade['wall_price']):g}? Резерв маржи отсутствует, так как план не открыт. Эта стена не будет "
            "снова запланирована в рамках текущего жизненного цикла.",
            InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Подтвердить отмену", callback_data=f"deal:cancel:{token}")],
                [InlineKeyboardButton(text="Назад", callback_data=f"deal:view:{token}")],
            ]),
        )
    await callback.answer()


@admin.callback_query(Admin(), F.data.startswith("deal:cancel:"))
async def cancel_one_plan(callback: CallbackQuery, runtime: RuntimeController, state: FSMContext) -> None:
    token = callback.data.rsplit(":", 1)[-1]
    trade = _trade_for_token(runtime, token)
    if trade is None or trade.get("status") != "pending":
        await _refresh_deal_after_stale(callback, runtime, token)
        await callback.answer("План уже изменился; список обновлён", show_alert=True)
        return
    signal_id = trade["signal_id"]
    try:
        await runtime.cancel_pending_trade(signal_id)
    except Exception as error:
        logger.exception("Manual PENDING cancellation failed for %s", signal_id)
        await callback.answer(f"Отмена не сохранена: {str(error)[:160]}", show_alert=True)
        return
    await callback.answer("PENDING отменён; резерв маржи освобождён")
    if callback.message is not None:
        text, keyboard = _deal_list_text(runtime, "pending", 0)
        await edit_message_if_changed(callback.message, text, keyboard)


@admin.callback_query(Admin(), F.data.regexp(r"^deal:edit_(tp|sl):[0-9a-f]{20}$"))
async def request_open_trade_level(callback: CallbackQuery, runtime: RuntimeController, state: FSMContext) -> None:
    action, token = callback.data.rsplit(":", 2)[1:]
    trade = _trade_for_token(runtime, token)
    if trade is None or trade.get("status") != "open":
        await _refresh_deal_after_stale(callback, runtime, token)
        await callback.answer("Сделка больше не OPEN", show_alert=True)
        return
    await state.update_data(open_trade_signal_id=trade["signal_id"], open_trade_level=action)
    await state.set_state(SettingsInput.waiting_for_open_trade_level)
    if callback.message is not None:
        await callback.message.answer(
            f"Введите новый {action.upper()} цены для <code>{escape(trade['symbol'])}</code>. "
            "Значение должно соответствовать tickSize Bybit и оставаться по правильную сторону текущей цены.",
            reply_markup=kb.cancel_input,
        )
    await callback.answer()


@admin.message(Admin(), StateFilter(SettingsInput.waiting_for_open_trade_level))
async def update_open_trade_level(message: Message, state: FSMContext, runtime: RuntimeController) -> None:
    data = await state.get_data()
    signal_id = data.get("open_trade_signal_id")
    field = data.get("open_trade_level")
    try:
        value = float((message.text or "").strip())
        if not isfinite(value) or value <= 0:
            raise ValueError("Введите положительную цену")
        trade = runtime.engine.trade_manager.find_trade(signal_id)
        if trade is None or trade.get("status") != "open":
            await state.clear()
            if trade is not None:
                text, keyboard = _deal_card(runtime, trade)
                await message.answer("Сделка уже изменилась.\n\n" + text, reply_markup=keyboard)
            else:
                await message.answer("Сделка уже изменилась и больше не OPEN. Откройте менеджер и обновите список.")
            return
        await runtime.update_open_trade_level(signal_id, field=field, value=value)
    except ValueError as error:
        await message.answer(f"Значение не принято: {escape(str(error))}. Повторите ввод или нажмите «Отмена».")
        return
    except Exception as error:
        logger.exception("Could not edit levels for virtual trade %s", signal_id)
        await state.clear()
        await message.answer(f"Уровни не изменены: {escape(str(error))}")
        return
    await state.clear()
    trade = runtime.engine.trade_manager.find_trade(signal_id)
    if trade is not None:
        text, keyboard = _deal_card(runtime, trade)
        await message.answer("TP/SL сохранены в PostgreSQL.\n\n" + text, reply_markup=keyboard)


@admin.callback_query(Admin(), F.data == "pair_statistics")
async def request_pair_statistics(
    callback: CallbackQuery, state: FSMContext, runtime: RuntimeController
) -> None:
    await state.set_state(SettingsInput.waiting_for_pair_statistics)
    pairs = ", ".join(runtime.engine.pair_settings) or "нет настроенных пар"
    if callback.message is not None:
        await edit_message_if_changed(
            callback.message,
            f"<b>Статистика пар</b>\n\nДоступные пары: {pairs}\n"
            "Отправьте символ пары, например <code>BTCUSDT</code>.",
            reply_markup=kb.cancel_input,
        )
    await callback.answer()


@admin.message(Admin(), StateFilter(SettingsInput.waiting_for_pair_statistics))
async def show_pair_statistics(
    message: Message, state: FSMContext, runtime: RuntimeController
) -> None:
    symbol = (message.text or "").strip().upper()
    if symbol not in runtime.engine.pair_settings:
        pairs = ", ".join(runtime.engine.pair_settings) or "нет настроенных пар"
        await message.answer(f"Неизвестная пара. Доступные пары: {pairs}")
        return
    await state.clear()
    await message.answer(
        pair_statistics_text(runtime, symbol), reply_markup=kb.pair_statistics
    )


@admin.callback_query(Admin(), F.data == "engine_start")
async def start_engine(
    callback: CallbackQuery, runtime: RuntimeController, state: FSMContext
) -> None:
    await state.clear()
    try:
        await runtime.start()
        message = "Движок запущен. Запускаются потоки данных включённых пар."
    except RuntimeError as error:
        message = str(error)
    except Exception:
        message = "Не удалось запустить движок: проверьте состояние конфигурации и базы данных."
    await _refresh_dashboard_after_action(callback, runtime)
    await callback.answer(message, show_alert=runtime.engine.mode == EngineMode.STOPPED)


@admin.callback_query(Admin(), F.data == "entries_pause")
async def pause_entries(
    callback: CallbackQuery, runtime: RuntimeController, state: FSMContext
) -> None:
    await state.clear()
    try:
        await runtime.pause_new_entries()
        message = (
            "Входы приостановлены: ожидающие входы отменены, "
            "открытые сделки продолжают сопровождаться."
        )
    except RuntimeError as error:
        message = str(error)
    except Exception:
        message = "Не удалось сохранить режим паузы в базе данных."
    await _refresh_dashboard_after_action(callback, runtime)
    await callback.answer(message, show_alert=runtime.engine.mode == EngineMode.STOPPED)


@admin.callback_query(Admin(), F.data == "entries_resume")
async def resume_entries(
    callback: CallbackQuery, runtime: RuntimeController, state: FSMContext
) -> None:
    await state.clear()
    try:
        await runtime.resume_new_entries()
        message = "Входы разрешены после получения свежих данных и пересчёта стакана."
    except RuntimeError as error:
        message = str(error)
    except Exception:
        message = "Не удалось сохранить режим работы в базе данных."
    await _refresh_dashboard_after_action(callback, runtime)
    await callback.answer(message, show_alert=runtime.engine.mode == EngineMode.STOPPED)


@admin.callback_query(Admin(), F.data == "engine_stop")
async def stop_engine(
    callback: CallbackQuery, runtime: RuntimeController, state: FSMContext
) -> None:
    await state.clear()
    try:
        events = await runtime.stop_all()
        closed = sum(event["event"] == "TRADE_CLOSED" for event in events)
        cancelled = sum(event["event"] == "ENTRY_CANCELLED" for event in events)
        message = f"Остановлено. Закрыто виртуальных сделок: {closed}; отменено входов: {cancelled}."
    except RuntimeError as error:
        message = f"Остановка не завершена: {error}"
    except Exception:
        message = "Остановка не завершена: не удалось сохранить состояние в базе данных."
    await _refresh_dashboard_after_action(callback, runtime)
    await callback.answer(message, show_alert=runtime.engine.mode != EngineMode.STOPPED)


@admin.callback_query(Admin(), F.data == "close_open_trades")
async def close_open_trades(
    callback: CallbackQuery, runtime: RuntimeController, state: FSMContext
) -> None:
    await state.clear()
    try:
        events = await runtime.close_open_trades()
        closed = sum(event["event"] == "TRADE_CLOSED" for event in events)
        message = f"Закрыто открытых виртуальных сделок: {closed}. Движок продолжает работу."
    except RuntimeError as error:
        message = f"Сделки не закрыты: {error}"
    except Exception:
        message = "Сделки не закрыты: не удалось сохранить состояние в базе данных."
    await _refresh_dashboard_after_action(callback, runtime)
    await callback.answer(message, show_alert="не закрыты" in message)


@admin.callback_query(Admin(), F.data == "dashboard")
async def show_dashboard(
    callback: CallbackQuery, runtime: RuntimeController, state: FSMContext
) -> None:
    await state.clear()
    if callback.message is not None:
        await edit_message_if_changed(callback.message, dashboard_text(runtime), reply_markup=kb.main)
    await callback.answer()


@admin.callback_query(Admin(), F.data == "settings_leverage")
async def request_pair_leverage(callback: CallbackQuery, state: FSMContext, runtime: RuntimeController) -> None:
    await state.clear()
    rows = []
    for symbol, settings in runtime.engine.pair_settings.items():
        leverage = settings.leverage or runtime.engine.trading_settings.leverage or 25.0
        rows.append([InlineKeyboardButton(
            text=f"{symbol} · {leverage:g}x",
            callback_data=f"leverage_pair:{symbol}",
        )])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="settings")])
    if callback.message is not None:
        await edit_message_if_changed(
            callback.message,
            "<b>Плечо по торговым парам</b>\n\n"
            "Изменение применяется только к новым сделкам; у созданных сделок "
            "параметры позиции остаются зафиксированными.",
            InlineKeyboardMarkup(inline_keyboard=rows),
        )
    await callback.answer()


@admin.callback_query(Admin(), F.data.startswith("leverage_pair:"))
async def choose_pair_leverage(callback: CallbackQuery, state: FSMContext, runtime: RuntimeController) -> None:
    symbol = callback.data.split(":", 1)[1].strip().upper()
    settings = runtime.engine.pair_settings.get(symbol)
    if settings is None:
        await callback.answer("Пара больше не настроена", show_alert=True)
        return
    current = settings.leverage or runtime.engine.trading_settings.leverage or 25.0
    choices = (1, 2, 3, 5, 10, 20, 25, 50, 75, 100, 125)
    rows = [
        [InlineKeyboardButton(
            text=("✓ " if float(value) == float(current) else "") + f"{value}x",
            callback_data=f"leverage_set:{symbol}:{value}",
        ) for value in choices[index:index + 3]]
        for index in range(0, len(choices), 3)
    ]
    rows.append([InlineKeyboardButton(text="Ввести своё значение", callback_data=f"leverage_custom:{symbol}")])
    rows.append([InlineKeyboardButton(text="К списку пар", callback_data="settings_leverage")])
    if callback.message is not None:
        await edit_message_if_changed(
            callback.message,
            f"<b>{escape(symbol)} · плечо {current:g}x</b>\n\nВыберите значение до 125x или введите своё.",
            InlineKeyboardMarkup(inline_keyboard=rows),
        )
    await callback.answer()


async def _save_pair_leverage(symbol: str, raw_value: str, runtime: RuntimeController) -> float:
    value = float(raw_value)
    if not 0 < value <= 125:
        raise ValueError("Плечо должно быть больше 0 и не больше 125x")
    current = runtime.engine.pair_settings.get(symbol.upper())
    if current is None:
        raise ValueError("Пара больше не настроена")
    await runtime.update_pair_settings(replace(current, leverage=value))
    return value


@admin.callback_query(Admin(), F.data.startswith("leverage_set:"))
async def set_pair_leverage(callback: CallbackQuery, runtime: RuntimeController) -> None:
    _, symbol, raw_value = callback.data.split(":", 2)
    try:
        value = await _save_pair_leverage(symbol, raw_value, runtime)
    except ValueError as error:
        await callback.answer(str(error), show_alert=True)
        return
    except Exception:
        logger.exception("Could not save leverage for %s", symbol)
        await callback.answer("Не удалось сохранить плечо в PostgreSQL", show_alert=True)
        return
    if callback.message is not None:
        await edit_message_if_changed(
            callback.message,
            f"Плечо {escape(symbol)} сохранено: <b>{value:g}x</b>. Применится к новым сделкам.",
            InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="К списку пар", callback_data="settings_leverage")
            ]]),
        )
    await callback.answer("Сохранено")


@admin.callback_query(Admin(), F.data.startswith("leverage_custom:"))
async def request_custom_leverage(callback: CallbackQuery, state: FSMContext) -> None:
    symbol = callback.data.split(":", 1)[1].strip().upper()
    await state.update_data(leverage_symbol=symbol)
    await state.set_state(SettingsInput.waiting_for_pair_leverage)
    if callback.message is not None:
        await callback.message.answer(
            f"Отправьте плечо для {escape(symbol)} числом от 0 до 125, например <code>17.5</code>.",
            reply_markup=kb.cancel_input,
        )
    await callback.answer()


@admin.message(Admin(), StateFilter(SettingsInput.waiting_for_pair_leverage))
async def set_custom_pair_leverage(message: Message, state: FSMContext, runtime: RuntimeController) -> None:
    data = await state.get_data()
    symbol = str(data.get("leverage_symbol", "")).upper()
    try:
        value = await _save_pair_leverage(symbol, message.text or "", runtime)
    except (ValueError, TypeError) as error:
        await message.answer(f"Некорректное плечо: {escape(str(error))}")
        return
    except Exception:
        logger.exception("Could not save custom leverage for %s", symbol)
        await message.answer("Не удалось сохранить плечо в PostgreSQL.")
        return
    await state.clear()
    await message.answer(
        f"Плечо {escape(symbol)} сохранено: <b>{value:g}x</b>. Применится к новым сделкам.",
        reply_markup=kb.main,
    )


WALLS_PAGE_SIZE = 5
TELEGRAM_TEXT_LIMIT = 4096


def _wall_snapshot(runtime: RuntimeController, symbol: str) -> tuple[dict, dict]:
    engine = runtime.engine
    return (
        engine.get_wall_diagnostics(symbol).get(symbol, {}),
        engine.get_market_data_health(symbol),
    )


def _wall_counts(data: dict) -> tuple[list[dict], list[dict]]:
    walls = [wall for side in ("bid", "ask") for wall in data.get(side, [])]
    stable = [wall for wall in walls if wall.get("status") == "STABLE"]
    candidates = [wall for wall in walls if wall.get("status") == "CANDIDATE"]
    return stable, candidates


def _compact_planner_decision(value: object) -> str:
    decision = str(value or "нет данных")
    if decision.startswith("PENDING создан:"):
        return "PENDING создан"
    if decision.startswith("Рыночные данные:"):
        return "Планировщик ждёт рыночные данные (подробности на главной панели)."
    if len(decision) > 180:
        return "Планировщик вернул подробную причину; проверьте журнал приложения."
    return decision


def _wall_header(symbol: str, data: dict, health: dict, section: str, page: int) -> str:
    stable, candidates = _wall_counts(data)
    sync = "синхронизирован" if data.get("book_synchronized") else "не синхронизирован"
    feed = _feed_label(str(data.get("feed_state", "нет данных")))
    age = health.get("book_age_seconds")
    age_text = f"{age:.2f} с назад" if age is not None else "нет данных"
    label = {"stable": "STABLE", "candidate": "CANDIDATE", "diagnostics": "Диагностика"}[section]
    page_label = f" · стр. {page + 1}" if section != "diagnostics" else ""
    return (
        f"<b>Стены · {escape(symbol)}</b> · <b>{label}</b>{page_label}\n"
        f"STABLE: {len(stable)} · CANDIDATE: {len(candidates)}\n"
        f"WebSocket: {escape(feed)} · стакан: {sync}\n"
        f"Последнее обновление: {age_text} · цена: "
        f"{data.get('price') if data.get('price') is not None else 'нет данных'}\n"
    )


def _planner_condition_line(condition: dict) -> str:
    """Format recorded strategy facts; this function does not evaluate rules."""
    names = {
        "wall_age_at_least_40s": "возраст стены",
        "entry_distance_bps": "дистанция входа",
        "approach_from_valid_side": "сторона подхода",
        "previous_approach_seen": "предыдущий подход",
        "reversal_confirmed": "отскок",
        "price_beyond_wall_buffer": "выход за стену",
        "continuous_confirmation_started": "непрерывность",
        "continuous_confirmation_time": "время подтверждения",
        "risk_levels_valid": "валидность уровней",
        "risk_distance_positive": "риск",
        "stop_target_direction": "направление SL/TP",
        "bounce_reward_to_risk_after_obstacle": "R:R до препятствия",
        "nearest_wall_minimum_age": "минимальный возраст",
        "wall_on_valid_side_of_price": "положение стены",
        "nearest_wall_for_side": "ближайшая стена",
    }
    name = names.get(condition.get("condition"), str(condition.get("condition", "условие")))
    status = "✓" if condition.get("passed") else "✗"
    if "actual_bps" in condition:
        actual = f"{condition['actual_bps']:.2f}/{condition.get('threshold_bps', '—')} bps"
    elif "actual_seconds" in condition:
        threshold = condition.get("threshold_seconds", "—")
        actual = f"{condition['actual_seconds']:.1f}/{threshold} с"
    elif "elapsed_seconds" in condition:
        actual = (
            f"{condition['elapsed_seconds']:.1f}/{condition.get('confirmation_seconds', '—')} с; "
            f"gap {condition.get('actual_gap_seconds') if condition.get('actual_gap_seconds') is not None else '—'}"
            f"/{condition.get('maximum_gap_seconds', '—')} с"
        )
    elif "actual_reward" in condition:
        actual = f"{condition['actual_reward']:.4g}/{condition.get('minimum_reward', 0):.4g} USDT"
    elif "actual_wall_price" in condition:
        actual = (
            f"стена {condition['actual_wall_price']:g}, "
            f"цена {condition['current_price']:g} ({condition.get('wall_side', '?')})"
        )
    elif "actual_selected_wall_id" in condition:
        selected = condition.get("actual_selected_wall_id") or "нет"
        actual = f"выбрана {selected}"
    elif "actual" in condition and isinstance(condition["actual"], (str, int, float)):
        actual = escape(f"{condition['actual']}/{condition.get('threshold', '—')}")
    else:
        actual = str(condition.get("reason", "порог не применён"))
    return f"{status} {name}: {actual}"


def _planner_wall_line(wall: dict) -> str:
    side = "BID" if wall.get("side") == "buy" else "ASK"
    lifecycle = " · CONSUMED" if wall.get("event") == "CONSUMED" else ""
    lines = [
        f"<b>{side} {float(wall.get('price') or 0):.8g}</b>{lifecycle} · "
        f"{float(wall.get('notional') or 0):.4g} USDT · "
        f"{float(wall.get('age_seconds') or 0):.1f} с"
    ]
    strategies = wall.get("strategies") or []
    if not strategies:
        lines.append("  стратегиям не передана")
    for trace in strategies:
        strategy_id = escape(str(trace.get("strategy_id", "?")))
        if not trace.get("evaluated"):
            summary = (trace.get("reasons") or ["не оценивалась"])[0]
            lines.append(f"  {strategy_id}: не оценивалась — {escape(str(summary))}")
            continue
        conditions = trace.get("conditions") or []
        failed = [condition for condition in conditions if not condition.get("passed")]
        chosen = failed if failed else conditions[-1:]
        status = "сигнал" if trace.get("signal_formed") else "нет сигнала"
        if trace.get("signal_formed") and not trace.get("signal_accepted"):
            status += "; стратегия не выбрала стену"
        if trace.get("pending_created"):
            status += "; PENDING создан"
        elif trace.get("trade_planner_reason"):
            status += "; TradeManager отказал"
        lines.append(f"  {strategy_id}: стена передана · {status}")
        for condition in chosen:
            lines.append(f"    {_planner_condition_line(condition)}")
        for reason in (trace.get("reasons") or [])[:1]:
            lines.append(f"    {escape(str(reason))}")
        if trace.get("trade_planner_reason") and not trace.get("pending_created"):
            lines.append(f"    {escape(str(trace['trade_planner_reason']))}")
    return "\n".join(lines)


def stable_walls_text(
    runtime: RuntimeController,
    symbol: str | None = None,
    section: str = "stable",
    page: int = 0,
) -> str:
    """Build one bounded page from the shared live scanner diagnostic snapshot."""
    if section not in {"stable", "candidate", "diagnostics"}:
        raise ValueError("Unknown wall diagnostics page")
    pairs = list(runtime.engine.pair_settings)
    if not pairs:
        return "<b>Стабильные стены</b>\nТорговые пары не настроены."
    symbol = (symbol or ("BTCUSDT" if "BTCUSDT" in pairs else pairs[0])).strip().upper()
    if symbol not in runtime.engine.pair_settings:
        symbol = pairs[0]
    page = max(0, int(page))
    data, health = _wall_snapshot(runtime, symbol)
    stable, candidates = _wall_counts(data)

    if section == "diagnostics":
        rejected = data.get("rejected") or {}
        rejected_text = (
            ", ".join(f"{escape(str(key))}: {int(value)}" for key, value in rejected.items())
            if rejected else "нет"
        )
        planner_walls = data.get("planner_wall_decisions") or []
        scanner = data.get("scanner_diagnostics") or {}
        medians = scanner.get("side_medians") or {}
        sweep = scanner.get("ratio_sweep") or {}
        thresholds = (5, 10, 15, 20, 30, 50, 100)
        sweep_text = " ".join(
            f"{threshold}:{sweep.get('buy', {}).get(threshold, 0)}/"
            f"{sweep.get('sell', {}).get(threshold, 0)}"
            for threshold in thresholds
        )
        median_buy = medians.get("buy")
        median_sell = medians.get("sell")
        median_text = (
            f"{median_buy:.4g}" if median_buy is not None else "нет"
        ) + "/" + (
            f"{median_sell:.4g}" if median_sell is not None else "нет"
        )
        largest = []
        for level in (scanner.get("largest_levels") or [])[:5]:
            side_label = "BID" if level.get("side") == "buy" else "ASK"
            ratio = level.get("wall_ratio")
            largest.append(
                f"{side_label} {float(level['price']):.8g} "
                f"{float(level['notional']):.4g}USDT "
                f"ratio {float(ratio):.2f}x · {float(level.get('distance_bps', 0)):.2f}bps · "
                f"{escape(str(level.get('rejection') or 'проходит Scanner'))}" if ratio is not None else
                f"{side_label} {float(level['price']):.8g} "
                f"{float(level['notional']):.4g}USDT ratio нет · "
                f"{float(level.get('distance_bps', 0)):.2f}bps · "
                f"{escape(str(level.get('rejection') or 'проходит Scanner'))}"
            )
        total_pages = max(1, (len(planner_walls) + WALLS_PAGE_SIZE - 1) // WALLS_PAGE_SIZE)
        page = min(page, total_pages - 1)
        selected = planner_walls[page * WALLS_PAGE_SIZE:(page + 1) * WALLS_PAGE_SIZE]
        summary = (
            f"Причина статуса: {escape(str(data.get('reason', 'нет данных')))}\n"
            f"Отклонения Scanner: {rejected_text}\n"
            f"Медиана notional BID/ASK: {median_text} USDT\n"
            f"Ratio sweep (порог:BID/ASK): {sweep_text}\n"
            f"Крупнейшие уровни: {escape('; '.join(largest) or 'нет данных')}\n"
            f"Стратегии пары: {escape(', '.join(data.get('planner_strategies') or []) or 'нет данных')}\n"
            f"Trade Planner: {escape(_compact_planner_decision(data.get('planner_decision')))}\n"
            f"Оценка стен · стр. {page + 1}/{total_pages}:"
        )
        body = summary + "\n" + ("\n".join(_planner_wall_line(wall) for wall in selected) or "Нет оценённых стабильных стен.")
    else:
        walls = stable if section == "stable" else candidates
        total_pages = max(1, (len(walls) + WALLS_PAGE_SIZE - 1) // WALLS_PAGE_SIZE)
        page = min(page, total_pages - 1)
        selected = walls[page * WALLS_PAGE_SIZE:(page + 1) * WALLS_PAGE_SIZE]
        if not data.get("book_synchronized"):
            body = (
                "стакан не синхронизирован. Стены по прежнему стакану не показываются.\n"
                f"Причина: {escape(str(data.get('reason', 'ожидание синхронизации')))}"
            )
        elif not selected:
            body = "Список пуст."
        else:
            rows = []
            for wall in selected:
                side = "BID" if wall.get("side") == "buy" else "ASK"
                rows.append(
                    f"{side} <code>{float(wall['price']):.8g}</code> · "
                    f"<b>{float(wall['notional']):.4g} USDT</b> · "
                    f"{float(wall['distance_bps']):.2f} bps · "
                    f"{float(wall['age_seconds']):.1f} с · "
                    f"Δ {float(wall['change_notional']):+.4g} USDT"
                )
            body = "\n".join(rows)

    text = _wall_header(symbol, data, health, section, page) + "\n" + body
    if len(text) > TELEGRAM_TEXT_LIMIT:
        # Pages are bounded to five numeric rows and all free-form diagnostic
        # strings are compacted above. Treat a violated invariant as a bug.
        raise AssertionError(f"Wall diagnostics page exceeds Telegram limit: {len(text)}")
    return text


def stable_walls_keyboard(
    runtime: RuntimeController, symbol: str, section: str, page: int,
) -> InlineKeyboardMarkup:
    data, _health = _wall_snapshot(runtime, symbol)
    stable, candidates = _wall_counts(data)
    total = (
        len(stable if section == "stable" else candidates)
        if section != "diagnostics"
        else len(data.get("planner_wall_decisions") or [])
    )
    page_count = max(1, (total + WALLS_PAGE_SIZE - 1) // WALLS_PAGE_SIZE)
    page = min(max(0, page), page_count - 1)
    pair_row = [InlineKeyboardButton(
        text=("✓ " if pair == symbol else "") + pair,
        callback_data=f"walls:{pair}:{section}:0",
    ) for pair in runtime.engine.pair_settings]
    rows = [pair_row] if pair_row else []
    rows.append([
        InlineKeyboardButton(text=f"STABLE · {len(stable)}", callback_data=f"walls:{symbol}:stable:0"),
        InlineKeyboardButton(text=f"CANDIDATE · {len(candidates)}", callback_data=f"walls:{symbol}:candidate:0"),
    ])
    rows.append([InlineKeyboardButton(text="Диагностика", callback_data=f"walls:{symbol}:diagnostics:0")])
    rows.append([
        InlineKeyboardButton(text="Назад", callback_data=f"walls:{symbol}:{section}:{max(0, page - 1)}"),
        InlineKeyboardButton(text="Вперёд", callback_data=f"walls:{symbol}:{section}:{min(page_count - 1, page + 1)}"),
    ])
    rows.append([
        InlineKeyboardButton(text="Обновить", callback_data=f"walls:{symbol}:{section}:{page}"),
        InlineKeyboardButton(text="На главную", callback_data="dashboard"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _render_stable_walls(callback: CallbackQuery, runtime: RuntimeController,
                               symbol: str, section: str, page: int,
                               answer: str | None = None) -> None:
    if not runtime.engine.pair_settings:
        if callback.message is not None:
            await edit_message_if_changed(callback.message, "<b>Стабильные стены</b>\nТорговые пары не настроены.", kb.main)
        await callback.answer("Пары не настроены")
        return
    symbol = symbol.strip().upper()
    if symbol not in runtime.engine.pair_settings:
        symbol = "BTCUSDT" if "BTCUSDT" in runtime.engine.pair_settings else next(iter(runtime.engine.pair_settings))
    page = max(0, int(page))
    text = stable_walls_text(runtime, symbol, section, page)
    markup = stable_walls_keyboard(runtime, symbol, section, page)
    if callback.message is not None:
        await edit_message_if_changed(callback.message, text, markup)
    await callback.answer(answer or "")


@admin.callback_query(Admin(), F.data == "stable_walls")
async def show_stable_walls(callback: CallbackQuery, runtime: RuntimeController) -> None:
    pairs = runtime.engine.pair_settings
    if not pairs:
        await callback.answer("Пары не настроены", show_alert=True)
        return
    symbol = "BTCUSDT" if "BTCUSDT" in pairs else next(iter(pairs))
    await _render_stable_walls(callback, runtime, symbol, "stable", 0)


@admin.callback_query(Admin(), F.data.startswith("walls:"))
async def navigate_stable_walls(callback: CallbackQuery, runtime: RuntimeController) -> None:
    try:
        _prefix, symbol, section, raw_page = callback.data.split(":", 3)
        page = int(raw_page)
    except (AttributeError, ValueError):
        await callback.answer("Некорректная навигация", show_alert=True)
        return
    answer = None
    if section not in {"stable", "candidate", "diagnostics"}:
        await callback.answer("Неизвестный раздел", show_alert=True)
        return
    if page < 0:
        page = 0
        answer = "Это первая страница"
    await _render_stable_walls(callback, runtime, symbol, section, page, answer)
