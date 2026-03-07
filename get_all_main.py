import logging
import os
from kudago import EventManager
from logging.handlers import RotatingFileHandler
# Создаём два обработчика с разными файлами
info_handler = RotatingFileHandler(
    "/app/logs/cron_info.log",
    maxBytes=10*1024*1024,  # 10 МБ на файл
    backupCount=2,  # хранить 2 старых файла (всего ~30 МБ)
    encoding="utf-8"
)

error_handler = RotatingFileHandler(
    "/app/logs/cron.log",
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

# Добавляем обработчики
logger.addHandler(info_handler)
logger.addHandler(error_handler)



if __name__ == "__main__":

    # Параметры подключения к БД
    DB_DSN = (
        f"dbname={os.getenv('DB_NAME')} "
        f"user={os.getenv('DB_USER')} "
        f"password={os.getenv('DB_PASSWORD')} "
        f"host={os.getenv('DB_HOST')} "
        f"port={os.getenv('DB_PORT')} "
        f"options='-c client_encoding=UTF8'"
    )

    CITIES = ["msk", "spb"]

    try:
        # Получаем путь к clusters.json из .env
        clusters_path = os.getenv('CLUSTERS_PATH')
        if not clusters_path:
            raise ValueError("CLUSTERS_PATH не задан в окружении!")

        # Создаем менеджер
        manager = EventManager(
            db_dsn=DB_DSN,
            api_base_url="https://kudago.com/public-api/v1.4",
            clusters_path=clusters_path  # используем переменную
        )
        manager.sync_places(cities=CITIES, limit=2000)
        manager.sync_events(cities=CITIES, limit=1000)
        upcoming = manager.get_upcoming_events_periods(cities=CITIES)
        print(upcoming)

    except Exception as e:
        logger.error(f"Execution error: {e}")
    finally:
        if 'manager' in locals():
            manager.close()
