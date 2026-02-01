
import asyncio
import logging
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from config import CONFIG
from db import Database_Users
from ml import MLService
from handlers import start, recommend, button_handler, handle_city_selection, show_referral

# Импортируем планировщик и его экземпляр
from scheduled import setup_scheduler, scheduler
async def main():
    # Инициализация
    bot = Bot(token=CONFIG.TELEGRAM_TOKEN)
    dp = Dispatcher()
    
    bot.db = Database_Users()
    bot.ml = MLService()

    # Запуск планировщика
    setup_scheduler(bot, bot.db)  # ← ВАЖНО: подключаем планировщик
    logging.info("Планировщик запущен")

    # Регистрация обработчиков
    dp.message.register(start, Command("start"))
    dp.message.register(handle_city_selection, F.text.in_(["МСК", "СПБ", "МСК и СПБ"]))
    dp.message.register(show_referral, Command("referral"))
    dp.message.register(recommend, Command("recommend"))
    dp.callback_query.register(button_handler)

    print("Бот запущен. Ожидаем сообщения (polling)...")
    
    # Запускаем polling и планировщик параллельно
    try:
        await dp.start_polling(bot)
    finally:
        # При остановке бота:
        if scheduler.running:
            scheduler.shutdown()
            logging.info("Планировщик остановлен")

if __name__ == "__main__":
    asyncio.run(main())