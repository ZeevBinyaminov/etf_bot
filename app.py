import logging

from aiogram import executor
import asyncio

# from database import database
from handlers import user
from loader import bot, dp, ADMIN_ID
from database.database import moex_db, update_data_periodically, isins

logging.basicConfig(level=logging.INFO)


async def on_startup(dispatcher):
    await bot.send_message(chat_id=ADMIN_ID, text='Бот запущен!')
    await moex_db.update_moex_data(isins)
    asyncio.create_task(update_data_periodically())


async def on_shutdown(dp):
    await bot.send_message(chat_id=ADMIN_ID, text='Бот выключен!')


if __name__ == '__main__':
    executor.start_polling(dp,
                           on_startup=on_startup,
                           on_shutdown=on_shutdown,
                           skip_updates=True)
