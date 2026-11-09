from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


PAIR_PAGE_SIZE = 10


def paginated_pair_keyboard(
    pairs,
    page: int,
    item_callback,
    page_callback,
    back_callback: str,
    *,
    page_size: int = PAIR_PAGE_SIZE,
    columns: int = 2,
    labels=None,
    back_label: str = "Назад к настройкам",
) -> InlineKeyboardMarkup:
    """Build a bounded pair picker shared by settings and market diagnostics."""
    symbols = list(pairs)
    if page_size < 1 or columns < 1:
        raise ValueError("page_size and columns must be positive")
    page_count = max(1, (len(symbols) + page_size - 1) // page_size)
    page = max(0, min(int(page), page_count - 1))
    page_symbols = symbols[page * page_size:(page + 1) * page_size]
    labels = labels or {}
    rows = []
    for offset in range(0, len(page_symbols), columns):
        rows.append([
            InlineKeyboardButton(
                text=str(labels.get(symbol, symbol)),
                callback_data=item_callback(symbol),
            )
            for symbol in page_symbols[offset:offset + columns]
        ])
    rows.append([
        InlineKeyboardButton(
            text="Назад", callback_data=page_callback(max(0, page - 1)),
        ),
        InlineKeyboardButton(text=f"{page + 1}/{page_count}", callback_data=page_callback(page)),
        InlineKeyboardButton(
            text="Вперёд", callback_data=page_callback(min(page_count - 1, page + 1)),
        ),
    ])
    rows.append([InlineKeyboardButton(text=back_label, callback_data=back_callback)])
    return InlineKeyboardMarkup(inline_keyboard=rows)

main = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="Запустить / возобновить", callback_data="engine_start")],
        [InlineKeyboardButton(text="Пауза входов · отменить ожидание", callback_data="entries_pause")],
        [InlineKeyboardButton(text="Продолжить входы", callback_data="entries_resume")],
        [InlineKeyboardButton(text="Закрыть только открытые сделки", callback_data="close_open_trades")],
        [InlineKeyboardButton(text="Менеджер сделок", callback_data="deals:home")],
        [InlineKeyboardButton(text="Полная остановка · закрыть сделки", callback_data="engine_stop")],
        [InlineKeyboardButton(text="Обновить панель", callback_data="dashboard")],
        [InlineKeyboardButton(text="Настройки", callback_data="settings")],
        [InlineKeyboardButton(text="Статистика пар", callback_data="pair_statistics")],
        [InlineKeyboardButton(text="Стабильные стены", callback_data="stable_walls")],
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
        [InlineKeyboardButton(text="Плечо по парам", callback_data="settings_leverage")],
        [InlineKeyboardButton(text="Настроить TP / SL", callback_data="settings_trade_levels")],
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
