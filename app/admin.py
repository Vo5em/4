from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Filter, CommandStart, Command

import app.keyboard as kb

admin = Router()

class Admin(Filter):
    async def __call__(self, message: Message):
        return message.from_user.id in [1175542555]

@admin.message(Admin(), CommandStart())
async def cmd_start(message: Message):
    inf = await get_info()
    await message.answer("баланс - \n"
                         "ставка - \n"
                         "лимит одновременных сделок - \n"
                         "общий профит - \n"
                         "профит за неделю - \n"
                         "kd all - \n"
                         "kd per week - \n",
                         reply_markup=kb.main)

@admin.callback_query(Admin(), F.data == 'settings')
async def cmd_moreinfo(message: Message):
    await

