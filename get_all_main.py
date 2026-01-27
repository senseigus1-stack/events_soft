import logging
import os
from kudago_api import EventManager

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

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

        manager.sync_events(cities=CITIES, limit=50)
        upcoming = manager.get_upcoming_events_periods(cities=CITIES)
        print(upcoming)

    except Exception as e:
        logging.error(f"Execution error: {e}")
    finally:
        if 'manager' in locals():
            manager.close()
