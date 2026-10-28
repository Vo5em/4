from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

main = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="Запустить / возобновить", callback_data="engine_start")],
        [InlineKeyboardButton(text="Пауза входов · отменить ожидание", callback_data="entries_pause")],
        [InlineKeyboardButton(text="Продолжить входы", callback_data="entries_resume")],
        [InlineKeyboardButton(text="Закрыть только открытые сделки", callback_data="close_open_trades")],
        [InlineKeyboardButton(text="Полная остановка · закрыть сделки", callback_data="engine_stop")],
        [InlineKeyboardButton(text="Обновить панель", callback_data="dashboard")],
        [InlineKeyboardButton(text="Настройки", callback_data="settings")],
        [InlineKeyboardButton(text="Статистика пар", callback_data="pair_statistics")],
        [InlineKeyboardButton(text="Подробная статистика", callback_data="moreinfo")],
    ]
)

back = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="На главную", callback_data="dashboard")],
    ]
)

pair_statistics = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="К списку пар", callback_data="pair_statistics")],
        [InlineKeyboardButton(text="На главную", callback_data="dashboard")],
    ]
)

settings = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="Изменить ставку", callback_data="settings_stake")],
        [InlineKeyboardButton(text="Настроить пару", callback_data="settings_pair")],
        [InlineKeyboardButton(text="Стратегии по парам", callback_data="settings_strategy")],
        [InlineKeyboardButton(text="Добавить USDT-пару", callback_data="settings_add_pair")],
        [InlineKeyboardButton(text="На главную", callback_data="dashboard")],
    ]
)

cancel_input = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="Отмена", callback_data="cancel_input")],
    ]
)
