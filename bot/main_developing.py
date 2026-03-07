
import asyncio
from logging.handlers import RotatingFileHandler
import logging
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, StateFilter
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from config import CONFIG
from db import Database_Users
from ml import MLService
from scheduled import setup_scheduler, scheduler

# Импорты обработчиков (все из приведённого кода)
from new import (
    start,
    handle_city_selection,
    show_main_menu,
    recommend,
    button_handler,
    show_referral,
    handle_show_confirmed_events,
    add_event_command,
    process_city,
    process_title,
    process_description,
    process_datetime,
    process_url,
    confirm_event,
    handle_moderation,
    recommend_main_interest,
    AddEventStates,
    help_command,
    handle_problem_text,
    HelpState
)

# Создаём два обработчика с разными файлами
info_handler = RotatingFileHandler(
    "/app/logs/bot_info.log",
    maxBytes=10*1024*1024,  # 10 МБ на файл
    backupCount=2,  # хранить 2 старых файла (всего ~30 МБ)
    encoding="utf-8"
)

error_handler = RotatingFileHandler(
    "/app/logs/bot_error.log",
    maxBytes=50*1024*1024,  # 50 МБ на файл
    backupCount=10,  # хранить 10 старых файлов (всего ~550 МБ)
    encoding="utf-8"
)

# Настраиваем формат
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
info_handler.setFormatter(formatter)
error_handler.setFormatter(formatter)

# Фильтр для INFO (только INFO)
class InfoFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == logging.INFO

# Фильтр для ERROR (ERROR и выше: ERROR, CRITICAL)
class ErrorFilter(logging.Filter):
    def filter(self, record):
        return record.levelno >= logging.ERROR

# Применяем фильтры
info_handler.addFilter(InfoFilter())
error_handler.addFilter(ErrorFilter())

# Настраиваем логгер
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # собираем все уровни
logger.addHandler(info_handler)
logger.addHandler(error_handler)



async def main():
    # Проверка токена
    if not CONFIG.TELEGRAM_TOKEN:
        raise ValueError("Токен Telegram не указан в CONFIG")

    # Инициализация
    bot = Bot(token=CONFIG.TELEGRAM_TOKEN)
    dp = Dispatcher()

    # Прикрепление зависимостей к боту (без инициализации ML-модели)
    bot.db = Database_Users()
    bot.ml = MLService()  # Создаём экземпляр без загрузки модели

    try:
        # Запуск планировщика
        global scheduler
        scheduler = setup_scheduler(bot, bot.db)
        logger.info("Планировщик запущен")

        # Регистрация обработчиков команд и сообщений
        dp.message.register(start, Command("start"))

        # Обработка выбора города
        dp.message.register(
            handle_city_selection,
            F.text.in_(["Москва", "Санкт‑Петербург", "Оба города"])
        )

        dp.message.register(recommend_main_interest, Command("main"))
        dp.message.register(show_main_menu, Command("menu"))
        dp.message.register(recommend, Command("recommend"))
        dp.message.register(show_referral, Command("referral"))

        # Команда /add (старт)
        dp.message.register(
            add_event_command,
            Command("add")
        )

        # Последовательная обработка добавления события
        dp.message.register(
            process_city,
            (AddEventStates.wait_city)
        )
        dp.message.register(
            process_title,
            (AddEventStates.wait_title)
        )
        dp.message.register(
            process_description,
            (AddEventStates.wait_description)
        )
        dp.message.register(
            process_datetime,
            (AddEventStates.wait_datetime)
        )
        dp.message.register(
            process_url,
            (AddEventStates.wait_url)
        )
        dp.message.register(
            confirm_event,
            (AddEventStates.confirm)
        )

        # Обработчики callback-запросов
        dp.callback_query.register(
            handle_moderation,
            F.data.startswith(("approve_", "reject_"))
        )
        dp.callback_query.register(
            button_handler,
            F.data.startswith(("like_", "dislike_", "confirm_", "next_"))
        )
        dp.callback_query.register(
            handle_show_confirmed_events,
            F.data.startswith("show_confirmed_events_")
        )

        # Команда помощи
        dp.message.register(
            help_command,
            Command("help")
        )

        # Обработчик текста проблемы
        dp.message.register(
            handle_problem_text,
            HelpState.waiting_for_problem
        )

        logger.info("Бот запущен. Ожидаем сообщения (polling)...")

        # ЗАПУСК POLLING В ОТДЕЛЬНОЙ ЗАДАЧЕ
        polling_task = asyncio.create_task(dp.start_polling(bot))

        # Асинхронная инициализация ML-сервиса ПОСЛЕ старта polling
        await bot.ml.initialize()

        # Ждём завершения polling (если нужно)
        await polling_task

    except Exception as e:
        logger.error(f"Критическая ошибка в main(): {e}", exc_info=True)
        raise
    finally:
        # Корректное завершение планировщика
        if scheduler and scheduler.running:
            scheduler.shutdown()
            logger.info("Планировщик остановлен")

        # Закрытие сессий бота
        await bot.session.close()
        logger.info("Бот остановлен.")

if __name__ == "__main__":
    asyncio.run(main())