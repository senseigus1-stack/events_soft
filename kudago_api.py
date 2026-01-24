import os
import sys
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Optional, Any, Tuple
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from dotenv import load_dotenv
import time
from ai.main_status import load_clusters_from_file
from ai.schemas import Event_ML
from ai.cluster_service import ClusterService
from sentence_transformers import SentenceTransformer


load_dotenv()


@dataclass
class Event:
    """Event model"""
    title: str
    description: str
    place_name: str
    address: str
    event_url: str
    image_url: str
    start_datetime: Optional[int]
    end_datetime: Optional[int]
    category: str
    status: str
    status_ml: str

    # Новые поля
    id: int                           # from "id" in JSON
    publication_date: Optional[int]     # from "publication_date"
    slug: str                         # from "slug"
    age_restriction: str              # from "age_restriction"
    price: str                       # form "price"
    is_free: bool                    # from "is_free"
    tags: List[str]                  # from "tags" (список строк)
    favorites_count: int             # from "favorites_count"
    comments_count: int              # from "comments_count"
    short_title: str                # from "short_title"
    disable_comments: bool           # from "disable_comments"
    periods: List[Dict[str, int]] = field(default_factory=list)  # [{"start": 123, "end": 456}, ...]


class KudaGoAPI:
    def __init__(self, base_url: str = "https://kudago.com/public-api/v1.4"):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
   #     self.session.headers.update({
    #        "User-Agent": "EventAggregator/1.0"
     #   })

    def get_event_ids(self, city: str, limit: int = 100, max_retries: int = 3) -> List[int]:
        all_ids = []  # only (ID events)
        page = 1
        retry_count = 0

        # INterval: will 30 days
        now = int(datetime.now(timezone.utc).timestamp())
        actual_since = now #- 2628000  # 1 months before
        actual_until = now + 2592000*12   # 30  forward


        while True:
            try:
                params = {
                    "fields": "id",
                    "order_by": "id",
                    "location": city,
                    "page": page,
                    "actual_since": actual_since,
                    "actual_until": actual_until
                }

                response = self.session.get(
                    f"{self.base_url}/events/",
                    params=params,
                    timeout=15
                )

                if response.status_code == 200:
                    data = response.json()
                    events = data.get("results", [])

                    if not events:
                        print(f"✓ Page {page}: haven't events. Stoppped.")
                        break

                    # only ID событий (integer)
                    for event in events:
                        all_ids.append(event["id"])

                    print(f"✓ Page {page}: {len(events)} events. ALL: {len(all_ids)}")

                    # Step for new page
                    next_page = data.get("next")
                    if not next_page:
                        print("Больше страниц нет. Завершаем.")
                        break

                    page += 1
                    retry_count = 0  #count stop


                elif response.status_code == 429:
                    wait_time = 5 * (2 ** retry_count)
                    print(f"!429: слишком много запросов. Пауза {wait_time} сек...")
                    time.sleep(wait_time)
                    retry_count += 1

                else:
                    print(f"!Ошибка {response.status_code} на странице {page}: {response.text}")
                    retry_count += 1

            except Exception as e:
                print(f"!Исключение на странице {page}: {e}")
                retry_count += 1

            # Проверка на превышение попыток
            if retry_count >= max_retries:
                print(f"Превышено количество попыток ({max_retries}) для страницы {page}. Остановка.")
                break

            time.sleep(0.5)  # Пауза между запросами

        return all_ids  # Возвращаем список чисел (ID)
        
    def get_event_details(self, event_id: int) -> Optional[Dict]:
        """Получить подробную информацию о событии"""
        url = f"{self.base_url}/events/{event_id}/"

        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            try:
                data = response.json()
            except json.JSONDecodeError as e:
                logging.error(f"Ошибка JSON для event_id {event_id}: {e}")
                text = response.content.decode('utf-8', errors='replace')
                data = json.loads(text)
            return data
        except requests.RequestException as e:
            logging.error(f"Ошибка API для event_id {event_id}: {e}")
            return None

class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.connection = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(self.dsn)
            logging.info("Подключение к БД установлено")
        except psycopg2.Error as e:
            logging.error(f"Ошибка подключения к БД: {e}")
            raise

    def create_city_table(self, city: str):
        table_name = city.lower().replace("-", "_")
        
        # Основной запрос для таблицы событий города
        query1 = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id BIGINT PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            description TEXT,
            place_name VARCHAR(255),
            address TEXT,
            event_url VARCHAR(500),
            image_url VARCHAR(500),
            start_datetime BIGINT,
            end_datetime BIGINT,
            category VARCHAR(255),
            status VARCHAR(20) DEFAULT 'upcoming',
            status_ml JSONB,
            
            -- Новые поля
            publication_date BIGINT,
            slug VARCHAR(255),
            age_restriction VARCHAR(10),
            price VARCHAR(255),
            is_free BOOLEAN,
            tags TEXT[],
            favorites_count INTEGER,
            comments_count INTEGER,
            short_title VARCHAR(255),
            disable_comments BOOLEAN
            
        );
        """
        
        # Запрос для таблицы дат событий
        query2 = f"""
        CREATE TABLE IF NOT EXISTS event_dates_{table_name} (
            id SERIAL PRIMARY KEY,
            event_id BIGINT NOT NULL REFERENCES {table_name}(id) ON DELETE CASCADE,
            start_timestamp BIGINT NOT NULL,
            end_timestamp BIGINT NOT NULL
        );
        """
        


        query3 = """
                    CREATE TABLE IF NOT EXISTS users (
                        id BIGINT PRIMARY KEY,
                        name VARCHAR(255),
                        city INTEGER,
                        status_ml JSONB DEFAULT '[]',
                        event_history JSONB DEFAULT '[]'
                        );
                    """
    # 1 - msk
    # 2 - spb
    # 3 - msk & spb


        with self.connection.cursor() as cursor:
            cursor.execute(query1)
            cursor.execute(query2)
            cursor.execute(query3)
        
        self.connection.commit()
        
    def save_events(self, city: str, events: List[Event]):
        table_name = city.lower().replace("-", "_")
        query = f"""
        INSERT INTO {table_name} (
            id,
            title,
            description,
            place_name,
            address,
            event_url,
            image_url,
            start_datetime,
            end_datetime,
            category,
            status,
            publication_date,
            slug,
            age_restriction,
            price,
            is_free,
            tags,
            favorites_count,
            comments_count,
            short_title,
            disable_comments,
            status_ml
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s
        )
        ON CONFLICT (id) DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            place_name = EXCLUDED.place_name,
            address = EXCLUDED.address,
            event_url = EXCLUDED.event_url,
            image_url = EXCLUDED.image_url,
            start_datetime = EXCLUDED.start_datetime,
            end_datetime = EXCLUDED.end_datetime,
            category = EXCLUDED.category,
            status = EXCLUDED.status,
            publication_date = EXCLUDED.publication_date,
            slug = EXCLUDED.slug,
            age_restriction = EXCLUDED.age_restriction,
            price = EXCLUDED.price,
            is_free = EXCLUDED.is_free,
            tags = EXCLUDED.tags,
            favorites_count = EXCLUDED.favorites_count,
            comments_count = EXCLUDED.comments_count,
            short_title = EXCLUDED.short_title,
            disable_comments = EXCLUDED.disable_comments,
            status_ml = EXCLUDED.status_ml
        """

        with self.connection.cursor() as cursor:
            for event in events:
                cursor.execute(query, (
                    event.id,
                    event.title,
                    event.description,
                    event.place_name,
                    event.address,
                    event.event_url,
                    event.image_url,
                    event.start_datetime,
                    event.end_datetime,
                    event.category,
                    event.status,
                    event.publication_date,
                    event.slug,
                    event.age_restriction,
                    event.price,
                    event.is_free,
                    event.tags,  # PostgreSQL поддерживает массивы напрямую
                    event.favorites_count,
                    event.comments_count,
                    event.short_title,
                    event.disable_comments,
                    event.status_ml
                ))
        self.connection.commit()

    def get_all_events(self, city:str) -> List[Dict]:
        table_name = city.lower().replace("-", "_")
        all_events = []

        with self.connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT {table_name}
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name NOT LIKE 'pg_%'
                AND table_name NOT IN ('schema_migrations')
            """)
            tables = cursor.fetchall()

        for table in tables:
            table_name = table[0]
            query = f"""
                SELECT
                    '{table_name}' AS city,
                    title,
                    description,
                    place_name,
                    address,
                    event_url,
                    image_url,
                    start_datetime,
                    end_datetime,
                    category,
                    status
                FROM {table_name}
                ORDER BY start_datetime
            """

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                events = cursor.fetchall()
                all_events.extend(events)

        return all_events

    def save_event_periods(self, event_id: int, periods: List[Dict[str, int]], city:str) -> None:
        """Сохраняет все периоды события в таблицу event_dates"""
        table_name = city.lower().replace("-", "_")
        if not periods:
            return

        query = f"""
            INSERT INTO event_dates_{table_name} (event_id, start_timestamp, end_timestamp)
            VALUES (%s, %s, %s)
        """

        with self.connection.cursor() as cursor:
            for period in periods:
                cursor.execute(query, (
                    event_id,
                    period["start"],
                    period["end"]
                ))
        self.connection.commit()




    def get_actual_periods(self, city: str) -> List[Dict]:
        """
        Возвращает список актуальных периодов событий для указанного города
        на ближайший месяц (от текущего момента до +30 дней).

        Args:
            city (str): Название города (используется для формирования имени таблицы).

        Returns:
            List[Dict]: Список словарей с ключами:
                - event_id (int)
                - start_timestamp (int)
                - end_timestamp (int)
        """
        table_name = city.lower().replace("-", "_")
        current_timestamp = int(time.time())
        one_month_later_timestamp = current_timestamp + (30 * 24 * 60 * 60)  # +30 дней в секундах

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT event_id, start_timestamp, end_timestamp
                    FROM event_dates_{table_name}
                    WHERE start_timestamp >= %s
                    AND start_timestamp < %s
                    ORDER BY start_timestamp
                    """,
                    (current_timestamp, one_month_later_timestamp)
                )

                rows = cursor.fetchall()

                result = []
                for row in rows:
                    result.append({
                        "event_id": row[0],
                        "start_timestamp": row[1],
                        "end_timestamp": row[2]
                    })
                return result

        except Exception as e:
            logging.error(f"Ошибка при получении актуальных периодов для города {city}: {e}")
            return []
        
    def close(self):
        if self.connection:
            self.connection.close()



class EventManager:

    def __init__(self, db_dsn: str,
                api_base_url: str = "https://kudago.com/public-api/v1.4",
                clusters_path:str='C:/Users/arsenii/events_soft/ai/clusters.json'
                ):
        
        self.api = KudaGoAPI(api_base_url)
        self.db = Database(db_dsn)
        self.clusters = load_clusters_from_file(clusters_path)
        self.cluster_service = ClusterService()
        self.cluster_service.load_clusters(self.clusters)


    def _parse_datetime(self, value) -> Optional[int]:
        if not value:
            return None

        # Проверка на числовой тип и диапазон
        #if isinstance(value, (int, float)):
        #    if value < 0 or value > 253402300800:  # ~9999 год
        #        logging.warning(f"Timestamp вне допустимого диапазона: {value}")
        #        return None
        #    try:
        #        dt = datetime.fromtimestamp(value, tz=timezone.utc)
        #        if dt.year < 1900 or dt.year > 9999:
        #            return None
        #       return int(value)
        #    except (OSError, OverflowError, ValueError) as e:
        #        logging.warning(f"Невалидный timestamp {value}: {e}")
       #         return None
    

        if isinstance(value, str):
            clean_value = value.replace("Z", "+00:00")
            try:
                dt = datetime.fromisoformat(clean_value)
                return int(dt.timestamp())
            except ValueError as e:
                logging.warning(f"Некорректный формат даты {clean_value}: {e}")
                return None

        logging.warning(f"Неожиданный тип даты: {type(value)} (значение: {value})")
        return None

    def _get_event_status(self, start_dt: Optional[int], end_dt: Optional[int]) -> str:
        """
        Определяет статус мероприятия на основе дат начала и окончания.
        
        Args:
            start_dt: Unix-timestamp начала события (int или None)
            end_dt: Unix-timestamp окончания события (int или None)
        
        Returns:
            Статус события: 'active', 'expired', 'upcoming', 'unknown'
        """
        now = int(datetime.now(timezone.utc).timestamp())  # Текущее время в UTC

        # Если обе даты отсутствуют — статус неизвестен
        if not start_dt and not end_dt:
            return "unknown"

        # Преобразуем timestamp в datetime для сравнения
        start_dt_obj = None
        end_dt_obj = None

        if start_dt is not None:
            try:
                start_dt_obj = datetime.fromtimestamp(start_dt, tz=timezone.utc)
            except (OSError, OverflowError, ValueError) as e:
                logging.warning(f"Невалидный start_dt timestamp {start_dt}: {e}")
                return "unknown"

        if end_dt is not None:
            try:
                end_dt_obj = datetime.fromtimestamp(end_dt, tz=timezone.utc)
            except (OSError, OverflowError, ValueError) as e:
                logging.warning(f"Невалидный end_dt timestamp {end_dt}: {e}")
                return "unknown"

        # Логика определения статуса
        if start_dt_obj and end_dt_obj:
            # Событие активно, если сейчас между началом и концом
            if start_dt_obj <= now <= end_dt_obj:
                return "active"
            # Событие уже завершилось
            elif end_dt_obj < now:
                return "expired"
            # Событие ещё не началось
            else:
                return "upcoming"

        elif start_dt_obj:
            # Есть только дата начала
            if start_dt_obj > now:
                return "upcoming"
            else:
                return "expired"  # началось, но нет даты окончания → считаем завершённым

        elif end_dt_obj:
            # Есть только дата окончания
            if end_dt_obj < now:
                return "expired"
            else:
                return "upcoming"  # ещё не закончилось, но нет даты начала → считаем будущим


        return "unknown"  # Непредвиденный случай

    def _get_status_vector(self, event_ml: dict) -> List[Tuple[str, float]]:

        raw_result = self.cluster_service.get_relevant_clusters(event_ml, self.clusters)
        
        # Преобразуем np.float64 → float для каждого кортежа
        cleaned_result = []
        for cluster_id, score in raw_result:
            cleaned_result.append((cluster_id, float(score)))  # явное приведение
        
        return cleaned_result
    
    def _create_event_from_item(self, item: Dict) -> Event:
        # Извлекаем и преобразуем даты
        start_str = item.get("start")
        end_str = item.get("finish")
        
        start_dt = self._parse_datetime(start_str)
        end_dt = self._parse_datetime(end_str)
        
        # Определяем статус на основе дат
        status = self._get_event_status(start_dt, end_dt)
        
        place = item.get("place") or {}
        images = item.get("images", [])
        categories = item.get("categories", [])

        tags = item.get("tags", [])
        logging.debug(f"Типы элементов в tags: {[type(t) for t in tags]}")   

        cleaned_tags = []
        for tag in tags:
            if isinstance(tag, tuple):
            # Если это кортеж с NumPy-числом
                cleaned_tuple = tuple(
                    str(item) if hasattr(item, 'dtype') else item
                    for item in tag
                )
                cleaned_tags.append(cleaned_tuple)
            else:
                cleaned_tags.append(tag)            

        #Сохраняем status_ml

        event_ml = self.extract_event_fields(item)
        
        # Получаем вектор статусов (список кортежей: [(cluster_id, score), ...])
        status_vector = self._get_status_vector(event_ml)


                # Преобразуем в список словарей для JSONB
        status_ml = []
        for cluster_id, score in status_vector:
            status_ml.append({
                "category": cluster_id,
                "score": float(score),  # гарантируем float
                "description": ""  # описание пока пустое (можно дополнить позже)
            })

        # Конвертируем в JSON-строку для столбца JSONB
        status_ml = json.dumps(status_ml)

        
            #ЭТО ЗАВТРА ПОМЕНЯТЬ
           #ЭТО ЗАВТРА ПОМЕНЯТЬ
          #ЭТО ЗАВТРА ПОМЕНЯТЬ
         #ЭТО ЗАВТРА ПОМЕНЯТЬ
        #ЭТО ЗАВТРА ПОМЕНЯТЬ
       #ЭТО ЗАВТРА ПОМЕНЯТЬ
      #ЭТО ЗАВТРА ПОМЕНЯТЬ
     #ЭТО ЗАВТРА ПОМЕНЯТЬ
    #ЭТО ЗАВТРА ПОМЕНЯТЬ
        
        #каждый раз загружать модель  заново
        # event_ml = self.extract_event_fields(item)
        # status_ml = get_status_vector(event_ml, 'C:/Users/redmi/events_soft/ai/clusters.json')
                    
        return Event(
            title=item.get("title", ""),
            description=item.get("description", ""),
            place_name=place.get("title", "") or "",
            address=place.get("address", "") or "",
            event_url=item.get("site_url", ""),
            image_url=images[0].get("image", "") if images else "",
            start_datetime=start_dt,
            end_datetime=end_dt,
            category=", ".join(categories),
            status=status,
            status_ml=status_ml,

            # Остальные поля (как в оригинальном коде)
            id=item["id"],
            publication_date=item.get("publication_date"),
            slug=item.get("slug", ""),
            age_restriction=item.get("age_restriction", ""),
            price=item.get("price", ""),
            is_free=item.get("is_free", False),
            tags=cleaned_tags,
            favorites_count=item.get("favorites_count", 0),
            comments_count=item.get("comments_count", 0),
            short_title=item.get("short_title", ""),
            disable_comments=item.get("disable_comments", False)
        )
    
    def get_all_events(self) -> List[Dict]:
        """Получить все мероприятия из всех городов через базу данных"""
        return self.db.get_all_events()
    
    
    def get_upcoming_events_periods(self, cities: List[str]) -> Dict[str, List[Dict]]:
        """
        Получает актуальные периоды событий для списка городов на ближайший месяц.

        Args:
            cities (List[str]): Список городов.

        Returns:
            Dict[str, List[Dict]]: Словарь, где ключ — город, значение — список периодов.
        """
        result = {}
        for city in cities:
            periods = self.db.get_actual_periods(city)
            result[city] = periods
        return result
    
    def close(self):
        try:
            if self.db.connection:
                self.db.connection.close()
        except Exception as e:
            logging.error(f"Ошибка при закрытии соединения с БД: {e}")

    def extract_event_fields(self, data: dict) -> dict:
        # Получаем имена полей класса Event
        event_fields = Event_ML.model_fields.keys()
        # Отбираем только те ключи из data, которые есть в полях Event_ML для обучения
        return {k: v for k, v in data.items() if k in event_fields}


    def sync_events(self, cities: List[str], limit: int = 100):
        self.db.connect()

        for city in cities:
            logging.info(f"Обработка города: {city}")
            self.db.create_city_table(city)

            try:
                # 1. Получаем ID событий
                event_ids = self.api.get_event_ids(city, limit)
                if not event_ids:
                    logging.warning(f"Нет событий для города {city}")
                    continue

                full_events = []
                for event_id in event_ids:
                    details = self.api.get_event_details(event_id)
                    if details:
                        full_events.append(details)
                    else:
                        logging.warning(f"Не удалось получить детали для события {event_id}")

                if not full_events:
                    logging.info(f"Нет данных для сохранения по городу {city}")
                    continue

                # 2. Обрабатываем каждое событие
                for item in full_events:
                    #а) Создаём событие (даты и статус извлекаются внутри метода)
                    event = self._create_event_from_item(item)

                    # Сохраняем основное событие
                    self.db.save_events(city, [event])  # save_events принимает список
                    logging.debug(f"Сохранено основное событие: {event.id}")

                    # query_ml = f"""
                    # INSERT INTO {city} (
                    #     status_ml
                    # ) VALUES (
                    #     %s)
                    # """

                    # with self.connection.cursor() as cursor:
                    #         cursor.execute(query_ml, (
                    #         status_ml   
                    #         ))
                    # self.connection.commit()

                    # logging.debug(f"Сохранен Cтатус Мероприятия по Рекомендациям для кластеров: {event.id}")


                    # 3. Сохраняем периоды (если есть)
                    periods = item.get("dates", [])
                    if periods:
                        valid_periods = []
                        for period in periods:
                            start = period.get("start")
                            end = period.get("end")

                            if start is None or end is None:
                                logging.warning(f"Пропущен период без start/end: {period}")
                                continue

                            if start > end:
                                logging.warning(f"Пропущен некорректный период (start >= end): {period}")
                                continue

                            valid_periods.append({
                                "start": start,
                                "end": end
                            })

                        if valid_periods:
                            self.db.save_event_periods(event.id, valid_periods, city)
                    
                            logging.debug(f"Сохранено {len(valid_periods)} периодов для события {event.id}")
                        else:
                            logging.warning(f"Нет валидных периодов для события {event.id}")
                    else:
                        logging.info(f"У события {event.id} нет периодов")

                self.db.get_actual_periods(city)

                logging.info(f"Завершена обработка города {city}. Сохранены данные по {len(full_events)} событиям.")

            except Exception as e:
                logging.error(f"Ошибка при обработке города {city}: {e}", exc_info=True)




# if __name__ == "__main__":

#     logging.basicConfig(
#         level=logging.INFO,
#         format="%(asctime)s [%(levelname)s] %(message)s",
#         datefmt="%Y-%m-%d %H:%M:%S"
#     )

#     # Параметры подключения к БД
#     DB_DSN = (
#         f"dbname={os.getenv('DB_NAME')} "
#         f"user={os.getenv('DB_USER')} "
#         f"password={os.getenv('DB_PASSWORD')} "
#         f"host={os.getenv('DB_HOST')} "
#         f"port={os.getenv('DB_PORT')} "
#         f"options='-c client_encoding=UTF8'"
#     )

#     # Список городов для синхронизации
#     CITIES = ["msk", "spb"]

#     try:
#         # Создаем менеджер
#         manager = EventManager(
#             db_dsn=DB_DSN,
#             api_base_url="https://kudago.com/public-api/v1.4"
#         )

#         # Синхронизируем мероприятия
#         manager.sync_events(cities=CITIES, limit=50)

#         # Получаем все мероприятия
#         all_events = manager.get_all_events()

#         logging.info(f"Всего мероприятий во всех городах: {len(all_events)}")

#         # Выводим результаты
#         for event in all_events:
#             city = event['city']
#             title = event['title']
#             start_time = event['start_datetime']
#             status = event['status']
#             print(f"{city} | {title} | {start_time} | Статус: {status}")

#     except Exception as e:
#         logging.error(f"Ошибка выполнения: {e}")
#     finally:
#         if 'manager' in locals():
#             manager.close()