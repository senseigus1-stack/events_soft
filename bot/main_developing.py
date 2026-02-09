
import asyncio
import logging
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
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
    my_friends,
    friend_events,
    handle_invite_event,
    ask_city,
    handle_decline_invite,
    handle_accept_invite,
    handle_select_event_for_invite,
    handle_show_confirmed_events,
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
        dp.message.register(ask_city, F.text.in_(["Москва", "Санкт‑Петербург", "Оба города"]))
        dp.message.register(show_main_menu, Command("menu"))
        dp.message.register(recommend, Command("recommend"))
        dp.message.register(show_referral, Command("referral"))
        # dp.message.register(my_friends, Command("myfriends"))
        # dp.message.register(friend_events, Command("friendevents"))
        dp.message.register(handle_city_selection)


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