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

    # Список городов для синхронизации
    CITIES = ["msk", "spb"]

    try:
        # Создаем менеджер
        manager = EventManager(
            db_dsn=DB_DSN,
            api_base_url="https://kudago.com/public-api/v1.4",
            clusters_path ='C:/Users/redmi/events_soft/ai/clusters.json'
        )

        # Синхронизируем мероприятия + Статус от МО
        manager.sync_events(cities=CITIES, limit=50)

        # Получаем все мероприятия
        all_events = manager.get_all_events()



        logging.info(f"Total events in all cities: {len(all_events)}")

        # Выводим результаты
        for event in all_events:
            city = event['city']
            title = event['title']
            start_time = event['start_datetime']
            status = event['status']
            print(f"{city} | {title} | {start_time} | Статус: {status}")

    except Exception as e:
        logging.error(f"Execution error: {e}")
    finally:
        if 'manager' in locals():
            manager.close()