import os
from html import escape
from dataclasses import replace

from aiogram import F, Router
from aiogram.filters import CommandStart, Filter, StateFilter, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message

import app.keyboard as kb
from app.runtime import RuntimeController
from script.engine import EngineMode
from script.settings import PairSettings

admin = Router()


def _load_admin_user_ids() -> set[int]:
    raw_value = os.getenv("ADMIN_USER_ID", "").strip()
    if not raw_value:
        raise RuntimeError(
            "Set ADMIN_USER_ID to comma-separated Telegram admin user IDs."
        )

    try:
        user_ids = {
            int(value.strip())
            for value in raw_value.split(",")
            if value.strip()
        }
    except ValueError as error:
        raise RuntimeError(
            "ADMIN_USER_ID must contain positive integers separated by commas."
        ) from error

    if not user_ids or any(user_id <= 0 for user_id in user_ids):
        raise RuntimeError(
            "ADMIN_USER_ID must contain positive integers separated by commas."
        )

    return user_ids


ADMIN_USER_IDS = _load_admin_user_ids()


class SettingsInput(StatesGroup):
    waiting_for_stake = State()
    waiting_for_trade_limit = State()
    waiting_for_pair = State()
    waiting_for_new_pair = State()
    waiting_for_strategy = State()


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
    return (
        "<b>Админ-панель · виртуальная торговля</b>\n\n"
        f"Состояние: {_mode_label(status['mode'])}\n"
        f"Пары: {pairs}\n"
        f"Рыночные данные: {feeds}\n"
        f"Виртуальный баланс: {status['balance_usdt']:.2f} USDT\n"
        f"Плавающий P/L: {status['unrealized_profit_usdt']:+.2f} USDT · "
        f"оценка баланса: {status['equity_usdt']:.2f} USDT\n"
        f"Ставка на сделку: {status['stake_usdt']:.2f} USDT\n"
        f"Лимит одновременных сделок: {status['max_open_trades']}\n"
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
        f"Ожидают сохранения в БД: {status['persistence_pending']}"
        f"{strategy_error_line}"
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
            f"устойчивость {settings.min_wall_age_seconds:g} с; "
            f"порог потребления {settings.consumed_ratio:.0%}; "
            f"лимит сделок {settings.max_open_trades}; "
            f"стратегии {escape(strategies)}"
        )
    return "\n".join(pairs) or "Торговые пары не настроены."


@admin.message(Command('id'))
async def dayid(message: Message):
    tg_id = message.from_user.id
    await message.answer(f'{tg_id}')

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
        await callback.message.edit_text(dashboard_text(runtime), reply_markup=kb.main)
    await callback.answer("Ввод отменён")


@admin.callback_query(Admin(), F.data == "settings")
async def show_settings(
    callback: CallbackQuery, runtime: RuntimeController, state: FSMContext
) -> None:
    await state.clear()
    pair_text = pair_settings_text(runtime)
    if callback.message is not None:
        await callback.message.edit_text(
            "<b>Настройки виртуальной торговли</b>\n\n"
            f"Ставка на сделку: {runtime.engine.trading_settings.stake_usdt:.2f} USDT\n"
            f"Общий лимит одновременных сделок: {runtime.engine.trading_settings.max_open_trades}\n"
            f"Начальный баланс: {runtime.engine.trading_settings.initial_balance_usdt:.2f} USDT\n\n"
            f"<b>Пары</b>\n{pair_text}\n\n"
            "Настройки сохраняются в PostgreSQL. Пороги, дистанции и лимит "
            "настраиваются отдельно для каждой пары; стратегии пары можно "
            "включить или отключить отдельной кнопкой.",
            reply_markup=kb.settings,
        )
    await callback.answer()


@admin.callback_query(Admin(), F.data == "settings_trade_limit")
async def request_trade_limit(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(SettingsInput.waiting_for_trade_limit)
    if callback.message is not None:
        await callback.message.answer(
            "Отправьте максимальное число одновременно ожидающих и открытых сделок, "
            "например: <code>3</code>.",
            reply_markup=kb.cancel_input,
        )
    await callback.answer()


@admin.message(Admin(), StateFilter(SettingsInput.waiting_for_trade_limit))
async def update_trade_limit(
    message: Message,
    state: FSMContext,
    runtime: RuntimeController,
) -> None:
    try:
        maximum = int(message.text or "")
        await runtime.update_max_open_trades(maximum)
    except (ValueError, TypeError):
        await message.answer("Введите целое число от 1 и выше.")
        return
    except Exception:
        await message.answer("Не удалось сохранить лимит в базе данных.")
        return
    await state.clear()
    await message.answer(dashboard_text(runtime), reply_markup=kb.main)


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
            "stop_distance_bps, max_open_trades</code>.\n"
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
    await state.set_state(SettingsInput.waiting_for_strategy)
    strategy_ids = sorted(runtime.engine.strategy_registry.strategy_ids)
    available = ", ".join(escape(item) for item in strategy_ids) or "нет"
    if callback.message is not None:
        await callback.message.answer(
            "Отправьте <code>ПАРА strategy_id on|off</code>.\n"
            f"Доступные стратегии: {available}\n"
            "Пример: <code>BTCUSDT stable_wall_v1 off</code>. При отключении "
            "ожидающие входы этой стратегии отменяются, открытые сделки остаются под контролем.",
            reply_markup=kb.cancel_input,
        )
    await callback.answer()


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
    selected = runtime.engine.get_enabled_strategy_ids(symbol)
    if action == "on":
        selected.add(strategy_id)
    else:
        selected.discard(strategy_id)
    try:
        await runtime.update_enabled_strategies(symbol, selected)
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
        "consumed_ratio", "entry_distance_bps", "stop_distance_bps",
    }
    if parameter == "enabled":
        if raw_value.lower() not in {"true", "false", "1", "0", "on", "off"}:
            await message.answer("Для enabled используйте true/false.")
            return
        value = raw_value.lower() in {"true", "1", "on"}
    elif parameter == "max_open_trades":
        try:
            value = int(raw_value)
        except ValueError:
            await message.answer("max_open_trades должен быть целым числом.")
            return
    elif parameter in numeric_fields:
        try:
            value = float(raw_value)
        except ValueError:
            await message.answer("Для этого параметра нужно числовое значение.")
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
        await callback.message.edit_text(
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
        await callback.message.edit_text(dashboard_text(runtime), reply_markup=kb.main)
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
        await callback.message.edit_text(dashboard_text(runtime), reply_markup=kb.main)
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
        await callback.message.edit_text(dashboard_text(runtime), reply_markup=kb.main)
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
        await callback.message.edit_text(dashboard_text(runtime), reply_markup=kb.main)
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
        await callback.message.edit_text(dashboard_text(runtime), reply_markup=kb.main)
    await callback.answer(message, show_alert="не закрыты" in message)


@admin.callback_query(Admin(), F.data == "dashboard")
async def show_dashboard(
    callback: CallbackQuery, runtime: RuntimeController, state: FSMContext
) -> None:
    await state.clear()
    if callback.message is not None:
        await callback.message.edit_text(dashboard_text(runtime), reply_markup=kb.main)
    await callback.answer()
