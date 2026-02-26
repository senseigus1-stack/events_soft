import asyncio
import logging
from logging.handlers import RotatingFileHandler
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from config import CONFIG
from db import Database_Users
from ml import MLService
from new import (
    start,
    handle_city_selection,
    show_main_menu,
    recommend,
    button_handler,
    show_referral,
    ask_city,
    handle_show_confirmed_events,
    recommend_main_interest,
    help_command,
    handle_problem_text,
    HelpState,
    add_event_command,
    process_city,
    process_title,
    process_description,
    process_datetime,
    process_url,
    confirm_event,
    handle_moderation,
    AddEventStates
)
import ssl
from aiohttp import web

# Импортируем планировщик
from scheduled import setup_scheduler, scheduler

# Создаём два обработчика с разными файлами
info_handler = RotatingFileHandler(
    "bot_info.log",
    maxBytes=10*1024*1024,  # 10 МБ на файл
    backupCount=2,  # хранить 2 старых файла (всего ~30 МБ)
    encoding="utf-8"
)

error_handler = RotatingFileHandler(
    "/app/logs/bot.log",
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

# Глобальные переменные
app = web.Application()
dp = Dispatcher()
bot = Bot(token=CONFIG.TELEGRAM_TOKEN)

async def on_startup(app: web.Application):
    """Действия при запуске сервера."""
    try:
        # Инициализация сервисов
        bot.db = Database_Users()
        bot.ml = MLService()

        # Запуск планировщика напоминаний
        setup_scheduler(bot, bot.db)
        logger.info("Планировщик напоминаний инициализирован")

        # Установка вебхука
        webhook_url = f"https://{CONFIG.WEBHOOK_HOST}:{CONFIG.WEBHOOK_PORT}{CONFIG.WEBHOOK_PATH}"
        await bot.set_webhook(url=webhook_url)
        logger.info(f"Бот запущен. Вебхук установлен: {webhook_url}")

    except Exception as e:
        logger.error(f"Ошибка при старте: {e}", exc_info=True)
        raise

async def on_shutdown(app: web.Application):
    """Действия при остановке сервера."""
    try:
        await bot.delete_webhook()
        await bot.session.close()

        # Остановка планировщика
        if scheduler.running:
            scheduler.shutdown()
            logger.info("Планировщик остановлен")

        logger.info("Бот остановлен.")

    except Exception as e:
        logger.error(f"Ошибка при остановке: {e}", exc_info=True)

async def handle_webhook(request: web.Request):
    """Обработчик входящих вебхуков от Telegram."""
    if request.content_type != 'application/json':
        return web.Response(status=400)

    try:
        update = types.Update(**await request.json())
        await dp.feed_update(bot, update)
        return web.Response(status=200)
    except Exception as e:
        logger.error(f"[ERROR] Обработка вебхука: {e}", exc_info=True)
        return web.Response(status=500)

async def health_handler(request: web.Request):
    return web.Response(text="OK", status=200)

def setup_routes():
    """Настройка маршрутов сервера."""
    path = CONFIG.WEBHOOK_PATH.lstrip("/")
    app.router.add_post(f"/{path}", handle_webhook)
    app.router.add_get("/health", health_handler)  # Добавляем healthcheck
    logger.info(f"Маршруты настроены: POST /{path}, GET /health")

async def main():
    try:
        # Регистрация хендлеров (без StateFilter — aiogram 3.x стиль)
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

        # Команда /add
        dp.message.register(add_event_command, Command("add"))

        # Обработка состояний (без StateFilter)
        dp.message.register(process_city, AddEventStates.wait_city)
        dp.message.register(process_title, AddEventStates.wait_title)
        dp.message.register(process_description, AddEventStates.wait_description)
        dp.message.register(process_datetime, AddEventStates.wait_datetime)
        dp.message.register(process_url, AddEventStates.wait_url)
        dp.message.register(confirm_event, AddEventStates.confirm)

        # Callback-хендлеры
        dp.callback_query.register(handle_moderation, F.data.startswith(("approve_", "reject_")))
        dp.callback_query.register(button_handler, F.data.startswith(("like_", "dislike_", "confirm_", "next_")))
        dp.callback_query.register(handle_show_confirmed_events, F.data.startswith("show_confirmed_events_"))

        dp.message.register(help_command, Command("help"))

        # Хендлер для текста проблемы
        dp.message.register(handle_problem_text, HelpState.waiting_for_problem)

        # Настройка сервера
        setup_routes()
        app.on_startup.append(on_startup)
        app.on_shutdown.append(on_shutdown)

        runner = web.AppRunner(app)
        await runner.setup()

        if CONFIG.USE_HTTPS:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS)
            ssl_context.load_cert_chain(CONFIG.CERT_PATH, CONFIG.KEY_PATH)
            site = web.TCPSite(runner, CONFIG.WEBHOOK_HOST, CONFIG.WEBHOOK_PORT, ssl_context=ssl_context)
        else:
            site = web.TCPSite(runner, CONFIG.WEBHOOK_HOST, CONFIG.WEBHOOK_PORT)

        await site.start()
        logger.info(
            f"Сервер запущен: http{'s' if CONFIG.USE_HTTPS else ''}://"
            f"{CONFIG.WEBHOOK_HOST}:{CONFIG.WEBHOOK_PORT}"
        )

        # Бесконечный цикл с улучшенной обработкой
        while True:
            try:
                await asyncio.sleep(3600)  # Проверка раз в час
            except asyncio.CancelledError:
                logger.info("Цикл остановлен (asyncio.CancelledError)")
                break
            except Exception as e:
                logger.error(f"Ошибка в основном цикле: {e}", exc_info=True)
                await asyncio.sleep(60)  # Пауза перед повторной попыткой

    except KeyboardInterrupt:
        logger.info("Бот остановлен вручную (Ctrl+C)")
    except Exception as e:
        logger.critical(f"Критическая ошибка: {e}", exc_info=True)
    finally:
        # Гарантированная очистка
        if app.on_shutdown:
            for handler in app.on_shutdown:
                try:
                    await handler(app)
                except Exception as e:
                    logger.error(f"Ошибка при выполнении shutdown-обработчика: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Программа завершена пользователем")
    except Exception as e:
        logger.critical(f"Непредвиденная ошибка: {e}")