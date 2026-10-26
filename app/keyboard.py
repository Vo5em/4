from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

main = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text = 'настройки', callback_data = 'settings')],
    [InlineKeyboardButton(text = 'подробная статистика', callback_data = 'moreinfo')],
    [InlineKeyboardButton(text = '')]
])