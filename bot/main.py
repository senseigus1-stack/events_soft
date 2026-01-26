import asyncio
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from config import Config
from db import Database_Users
from ml import MLService
from handlers import start, recommend, button_handler, handle_city_selection

async def main():
    # Инициализация
    bot = Bot(token=Config.TELEGRAM_TOKEN)
    dp = Dispatcher()
    
    # Привязыв сервисы к боту
    bot.db = Database_Users()
    bot.ml = MLService()
    
    # Регистрация обработчиков
    dp.message.register(start, Command("start"))
    dp.message.register(handle_city_selection, F.text.in_(["МСК", "СПБ", "МСК и СПБ"])) 
    dp.message.register(recommend, Command("recommend"))
    dp.callback_query.register(button_handler)
    
    print("Бот запущен...")
    await dp.start_polling(bot)  # поменять для продакшена

if __name__ == "__main__":
    asyncio.run(main())