import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from app import keyboard as kb
from app.admin import (
    SettingsInput,
    _pair_settings_overview_text,
    dashboard_text,
    navigate_pair_settings_section,
    pair_settings_card_keyboard,
    pair_settings_card_text,
    pair_settings_list_keyboard,
    pair_settings_list_text,
    select_settings_pair,
    stable_walls_keyboard,
    update_pair_settings,
)
from app.runtime import RuntimeController
from script.engine import TradingEngine
from script.settings import PairSettings


def run(coroutine):
    try:
        coroutine.send(None)
    except StopIteration as result:
        return result.value
    finally:
        coroutine.close()
    raise AssertionError("test coroutine suspended")


class FakeState:
    def __init__(self):
        self.data = {}
        self.state = None

    async def set_state(self, state):
        self.state = state

    async def update_data(self, **kwargs):
        self.data.update(kwargs)

    async def get_data(self):
        return dict(self.data)

    async def clear(self):
        self.data.clear()
        self.state = None


class FakeMessage:
    def __init__(self):
        self.text = None
        self.reply_markup = None
        self.edit_text = AsyncMock()
        self.answer = AsyncMock()


def make_runtime(count: int) -> RuntimeController:
    settings = {
        f"PAIR{i:03d}USDT": PairSettings(
            f"PAIR{i:03d}USDT", enabled=i % 3 != 0,
        )
        for i in range(count)
    }
    return RuntimeController(TradingEngine(settings))


class SettingsPairScalingTests(unittest.TestCase):
    def test_settings_summary_is_bounded_and_counts_5_50_100_pairs(self):
        for count in (5, 50, 100):
            runtime = make_runtime(count)
            with self.subTest(pair_count=count):
                text = _pair_settings_overview_text(runtime)
                self.assertLess(len(text), 4096)
                self.assertIn(f"Торговые пары: {count}", text)
                self.assertIn(f"включено {sum(i % 3 != 0 for i in range(count))}", text)
                self.assertIn(f"выключено {sum(i % 3 == 0 for i in range(count))}", text)

    def test_dashboard_stays_within_telegram_limit_for_100_pairs(self):
        runtime = make_runtime(100)
        for symbol in runtime.engine.pair_settings:
            runtime.engine.set_feed_status(symbol, "reconnecting", RuntimeError("stream failure " * 30))
            runtime.engine.strategy_errors[symbol] = {"bounce": "diagnostic failure"}
        text = dashboard_text(runtime)
        self.assertLess(len(text), 4096)
        self.assertIn(f"Пары ({sum(item.enabled for item in runtime.engine.pair_settings.values())} включено)", text)
        self.assertIn("и ещё", text)

    def test_pair_picker_has_two_symbols_per_row_and_ten_per_page(self):
        runtime = make_runtime(50)
        first = pair_settings_list_keyboard(runtime, 0)
        second = pair_settings_list_keyboard(runtime, 1)
        first_symbols = [
            button.callback_data for row in first.inline_keyboard for button in row
            if button.callback_data.startswith("settings_pair_select:")
        ]
        second_symbols = [
            button.callback_data for row in second.inline_keyboard for button in row
            if button.callback_data.startswith("settings_pair_select:")
        ]
        self.assertEqual(len(first_symbols), 10)
        self.assertEqual(len(second_symbols), 10)
        for row in first.inline_keyboard:
            pair_buttons = [button for button in row if button.callback_data.startswith("settings_pair_select:")]
            self.assertLessEqual(len(pair_buttons), 2)
        self.assertIn("стр. 2/5", pair_settings_list_text(runtime, 1))

    def test_selected_pair_card_shows_only_that_pair_and_preserves_context(self):
        runtime = make_runtime(50)
        state = FakeState()
        message = FakeMessage()
        callback = SimpleNamespace(
            data="settings_pair_select:PAIR037USDT:3",
            message=message, answer=AsyncMock(),
        )
        run(select_settings_pair(callback, state, runtime))
        text = message.edit_text.await_args.args[0]
        self.assertIn("PAIR037USDT", text)
        self.assertNotIn("PAIR036USDT", text)
        self.assertEqual(state.data["pair_symbol"], "PAIR037USDT")
        self.assertEqual(state.data["pair_list_page"], 3)

        section_callback = SimpleNamespace(
            data="settings_pair_section:PAIR037USDT:levels",
            message=message, answer=AsyncMock(),
        )
        run(navigate_pair_settings_section(section_callback, state, runtime))
        self.assertEqual(state.data["pair_symbol"], "PAIR037USDT")
        self.assertEqual(state.data["pair_section"], "levels")

    def test_individual_cards_show_current_values_and_all_new_settings(self):
        settings = PairSettings(
            "ETHUSDT", historical_level_timeframe="15m",
            historical_level_candle_count=333,
            historical_level_refresh_seconds=120,
            max_wall_level_distance_bps=13,
            bounce_take_profit_bps=7, bounce_stop_loss_bps=8,
            breakout_take_profit_bps=9, breakout_stop_loss_bps=10,
            false_breakout_point_1_offset_bps=5,
            false_breakout_point_2_distance_bps=12,
            false_breakout_observation_timeout_seconds=600,
            false_breakout_take_profit_bps=20,
            false_breakout_stop_loss_bps=15,
        )
        runtime = RuntimeController(TradingEngine({"ETHUSDT": settings}))
        levels = pair_settings_card_text(runtime, "ETHUSDT", "levels")
        strategies = pair_settings_card_text(runtime, "ETHUSDT", "strategies")
        false = pair_settings_card_text(runtime, "ETHUSDT", "false_breakout")
        self.assertIn("historical_level_timeframe</code>: 15m", levels)
        self.assertIn("historical_level_candle_count</code>: 333", levels)
        self.assertIn("max_wall_level_distance_bps</code>: 13", levels)
        self.assertIn("bounce_take_profit_bps</code>: 7", strategies)
        self.assertIn("breakout_stop_loss_bps</code>: 10", strategies)
        self.assertIn("false_breakout_point_1_offset_bps</code>: 5", false)
        self.assertIn("false_breakout_observation_timeout_seconds</code>: 600", false)
        self.assertIn("900 секунд", false)
        self.assertTrue(all(len(text) < 4096 for text in (levels, strategies, false)))

    def test_none_is_visible_and_command_uses_literal_none(self):
        runtime = RuntimeController(TradingEngine({"ETHUSDT": PairSettings("ETHUSDT")}))
        text = pair_settings_card_text(runtime, "ETHUSDT", "false_breakout")
        self.assertIn("Не задано (none)", text)
        self.assertIn("ETHUSDT false_breakout_point_1_offset_bps none", text)
        self.assertIn("ETHUSDT false_breakout_observation_timeout_seconds none", text)

    def test_setting_update_returns_to_same_pair_and_relevant_card(self):
        runtime = make_runtime(50)
        state = FakeState()
        state.data = {"pair_symbol": "PAIR037USDT", "pair_section": "levels", "pair_list_page": 3}
        state.state = SettingsInput.waiting_for_pair
        message = SimpleNamespace(text="PAIR037USDT historical_level_timeframe 15m", answer=AsyncMock())
        run(update_pair_settings(message, state, runtime))
        self.assertEqual(runtime.engine.pair_settings["PAIR037USDT"].historical_level_timeframe, "15m")
        self.assertEqual(state.data["pair_symbol"], "PAIR037USDT")
        self.assertEqual(state.data["pair_section"], "levels")
        self.assertIn("Настройка historical_level_timeframe сохранена", message.answer.await_args.args[0])
        self.assertIn("PAIR037USDT", message.answer.await_args.args[0])


class PairKeyboardScalingTests(unittest.TestCase):
    def test_generic_picker_and_stable_walls_have_two_pair_buttons_per_row(self):
        symbols = [f"PAIR{i:03d}USDT" for i in range(55)]
        picker = kb.paginated_pair_keyboard(
            symbols, 0, lambda symbol: f"select:{symbol}",
            lambda page: f"page:{page}", "settings", columns=2,
        )
        rows = [row for row in picker.inline_keyboard if any(b.callback_data.startswith("select:") for b in row)]
        self.assertTrue(rows)
        self.assertTrue(all(sum(b.callback_data.startswith("select:") for b in row) <= 2 for row in rows))

        runtime = make_runtime(55)
        markup = stable_walls_keyboard(runtime, "PAIR000USDT", "stable", 0, pair_page=1)
        for row in markup.inline_keyboard:
            pair_buttons = [b for b in row if len(b.callback_data.split(":")) == 5 and b.callback_data.startswith("walls:")]
            self.assertLessEqual(len(pair_buttons), 2)
        pair_callbacks = [
            b.callback_data for row in markup.inline_keyboard for b in row
            if b.callback_data.startswith("walls:") and len(b.callback_data.split(":")) == 5
        ]
        self.assertTrue(pair_callbacks)
        self.assertTrue(all(callback.endswith(":1") for callback in pair_callbacks))

    def test_long_pair_names_fit_callbacks_and_rows_remain_bounded(self):
        symbols = ["1000PEPEUSDT", "BTCUSDT", "ETHUSDT"]
        markup = kb.paginated_pair_keyboard(
            symbols, 0, lambda symbol: f"settings_pair_select:{symbol}:0",
            lambda page: f"settings_pair_page:{page}", "settings", columns=2,
        )
        self.assertLessEqual(max(len(button.callback_data) for row in markup.inline_keyboard for button in row), 64)
        self.assertEqual(len(markup.inline_keyboard[0]), 2)


if __name__ == "__main__":
    unittest.main()
