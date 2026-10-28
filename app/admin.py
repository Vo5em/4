import os
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
    return (
        "<b>Админ-панель · виртуальная торговля</b>\n\n"
        f"Состояние: {_mode_label(status['mode'])}\n"
        f"Алгоритм: {_algorithm_label(status['algorithm_status'])}\n"
        f"Пары: {pairs}\n"
        f"Рыночные данные: {feeds}\n"
        f"Поиск и анализ стен: {analysis_status}\n"
        f"Последнее обновление данных: {_date_label(last_update)}\n"
        f"Ошибки потока: {feed_error_text}\n"
        f"PostgreSQL: {_database_label(status['database_status'])}; "
        f"последний успех {_date_label(status['database_last_success_at'])}; "
        f"отложено записей: {status['persistence_pending']}\n"
        f"Виртуальный баланс: {status['balance_usdt']:.2f} USDT\n"
        f"Плавающий P/L: {status['unrealized_profit_usdt']:+.2f} USDT · "
        f"оценка баланса: {status['equity_usdt']:.2f} USDT\n"
        f"Ставка на сделку: {status['stake_usdt']:.2f} USDT\n"
        "Общий лимит сделок не задан; новые входы зависят от сигналов и доступного баланса\n"
        f"Профит всего: {status['total_profit_usdt']:.2f} USDT\n"
        f"Профит за 7 дней: {status['weekly_profit_usdt']:.2f} USDT\n"
        f"Доходность: {status['total_return_percent']:+.2f}% · "
        f"прибыльных сделок: {status['win_rate_percent']:.1f}%\n"
        f"Доступно для новых сделок: {status['available_balance_usdt']:.2f} USDT\n"
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
        pairs.append(
            f"• {symbol}: {state}; стена {settings.min_wall_ratio:g}× / "
            f"от {settings.min_wall_notional_usdt:g} USDT; "
            f"дистанция поиска {settings.max_wall_distance_bps:g} б.п.; "
            f"вход {settings.entry_distance_bps:g} б.п.; "
            f"стоп за стеной {settings.stop_distance_bps:g} б.п.; "
            f"устойчивость {max(40.0, settings.min_wall_age_seconds):g} с; "
            f"порог потребления {settings.consumed_ratio:.0%}; "
            f"стратегии {escape(strategies)}"
        )
    return "\n".join(pairs) or "Торговые пары не настроены."


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
    if callback.message is not None:
        await edit_message_if_changed(callback.message, dashboard_text(runtime), reply_markup=kb.main)
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
            f"<b>Пары</b>\n{pair_text}\n\n"
            "Настройки сохраняются в PostgreSQL. Пороги и дистанции "
            "настраиваются отдельно для каждой пары; стратегии пары можно "
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
    if callback.message is not None:
        await edit_message_if_changed(callback.message, dashboard_text(runtime), reply_markup=kb.main)
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
    if callback.message is not None:
        await edit_message_if_changed(callback.message, dashboard_text(runtime), reply_markup=kb.main)
    await callback.answer(message, show_alert=runtime.engine.mode == EngineMode.STOPPED)


@admin.callback_query(Admin(), F.data == "entries_resume")
async def resume_entries(
    callback: CallbackQuery, runtime: RuntimeController, state: FSMContext
) -> None:
    await state.clear()
    try:
        await runtime.resume_new_entries()
        message = "Новые входы разрешены."
    except RuntimeError as error:
        message = str(error)
    except Exception:
        message = "Не удалось сохранить режим работы в базе данных."
    if callback.message is not None:
        await edit_message_if_changed(callback.message, dashboard_text(runtime), reply_markup=kb.main)
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
    if callback.message is not None:
        await edit_message_if_changed(callback.message, dashboard_text(runtime), reply_markup=kb.main)
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
    if callback.message is not None:
        await edit_message_if_changed(callback.message, dashboard_text(runtime), reply_markup=kb.main)
    await callback.answer(message, show_alert="не закрыты" in message)


@admin.callback_query(Admin(), F.data == "dashboard")
async def show_dashboard(
    callback: CallbackQuery, runtime: RuntimeController, state: FSMContext
) -> None:
    await state.clear()
    if callback.message is not None:
        await edit_message_if_changed(callback.message, dashboard_text(runtime), reply_markup=kb.main)
    await callback.answer()
