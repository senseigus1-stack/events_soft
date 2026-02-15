
import asyncio
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
    AddEventStates
)

logger = logging.getLogger(__name__)


async def main():
    # Проверка токена
    if not CONFIG.TELEGRAM_TOKEN:
        raise ValueError("Токен Telegram не указан в CONFIG")

    # Инициализация
    bot = Bot(token=CONFIG.TELEGRAM_TOKEN)
    dp = Dispatcher()

    # Прикрепление зависимостей к боту
    bot.db = Database_Users()
    bot.ml = MLService()

    try:
        # Запуск планировщика
        setup_scheduler(bot, bot.db)
        logger.info("Планировщик запущен")

        dp.message.register(start, Command("start"))

        # 2. Обработка выбора города (только для сообщений с текстом из списка)
        dp.message.register(
            handle_city_selection,
            F.text.in_(["Москва", "Санкт‑Петербург", "Оба города"])
        )
        dp.message.register(recommend_main_interest, Command("main"))
        dp.message.register(show_main_menu, Command("menu"))
        dp.message.register(recommend, Command("recommend"))
        dp.message.register(show_referral, Command("referral"))
        # dp.message.register(my_friends, Command("myfriends"))
        # dp.message.register(friend_events, Command("friendevents"))
# 1. Команда /add (старт)
        dp.message.register(
            add_event_command,
            Command("add")
        )

        # 2. Обработка выбора города (состояние wait_city)
        dp.message.register(
            process_city,
            StateFilter(AddEventStates.wait_city)
        )

        # 3. Обработка названия (состояние wait_title)
        dp.message.register(
            process_title,
            StateFilter(AddEventStates.wait_title)
        )

        # 4. Обработка описания (состояние wait_description)
        dp.message.register(
            process_description,
            StateFilter(AddEventStates.wait_description)
        )

        # 5. Обработка даты/времени (состояние wait_datetime)
        dp.message.register(
            process_datetime,
            StateFilter(AddEventStates.wait_datetime)
        )

        # 6. Обработка URL (состояние wait_url)
        dp.message.register(
            process_url,
            StateFilter(AddEventStates.wait_url)
        )

        # 7. Подтверждение (состояние confirm)
        dp.message.register(
            confirm_event,
            StateFilter(AddEventStates.confirm)
        )

        dp.callback_query.register(
            handle_moderation,
            F.data.startswith(("approve_", "reject_"))
        )
        dp.callback_query.register(button_handler, F.data.startswith(("like_", "dislike_", "confirm_", "next_")))
        dp.callback_query.register(handle_show_confirmed_events, F.data.startswith("show_confirmed_events_"))
        # dp.callback_query.register(handle_select_event_for_invite, F.data.startswith("invite_to_event_"))
        # dp.callback_query.register(handle_invite_event, F.data.startswith("invite_friend_"))
        # dp.callback_query.register(handle_accept_invite, F.data.startswith("accept_invite_"))
        # dp.callback_query.register(handle_decline_invite, F.data.startswith("decline_invite_"))


        logger.info("Бот запущен. Ожидаем сообщения (polling)...")

        # Запуск polling
        await dp.start_polling(bot)

    except Exception as e:
        logger.error(f"Критическая ошибка в main(): {e}", exc_info=True)
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