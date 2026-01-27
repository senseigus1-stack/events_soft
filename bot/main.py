import asyncio
import logging
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from config import CONFIG
from db import Database_Users
from ml import MLService
from handlers import start, recommend, button_handler, handle_city_selection
import ssl
from aiohttp import web

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def setup_routes():
    """Настройка маршрутов сервера."""
    path = CONFIG.WEBHOOK_PATH.lstrip("/")
    app.router.add_post(f"/{path}", handle_webhook)
    logger.info(f"Маршрут настроен: POST /{path}")

async def main():
    try:
        # Регистрация обработчиков
        dp.message.register(start, Command("start"))
        dp.message.register(handle_city_selection, F.text.in_(["МСК", "СПБ", "МСК и СПБ"]))
        dp.message.register(recommend, Command("recommend"))
        dp.callback_query.register(button_handler)

        # Настройка сервера
        setup_routes()
        app.on_startup.append(on_startup)
        app.on_shutdown.append(on_shutdown)

        # Запуск сервера
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

        # Сервер работает — не нужно ничего ждать
        while True:
            await asyncio.sleep(3600)  # Можно убрать, если не нужно

    except (KeyboardInterrupt, SystemExit):
        logger.info("Остановка сервера по сигналу...")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
    finally:
        await runner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())