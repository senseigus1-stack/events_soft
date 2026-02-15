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
class Place:
    """Place model"""
    id: int
    title: str
    address: str
    description: str
    place_url: str
    image_url: str
    lat: Optional[float] = None
    lon: Optional[float] = None
    phone: str = ""
    site_url: str = ""
    timetable: str = ""
    is_free: bool = False
    favorites_count: int = 0
    comments_count: int = 0
    slug: str = ""
    subway: List[str] = field(default_factory=list)  # станции метро
    is_closed: bool = False             # закрыто ли место
    categories: List[str] = field(default_factory=list)
    short_title: str = ""
    tags: List[str] = field(default_factory=list)
    location: str = ""                 # город (например, "spb")
    age_restriction: Optional[str] = None  # возрастное ограничение
    disable_comments: bool = False
    has_parking_lot: bool = False

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
    place_id: Optional[int] = None  # новое поле
    likes: Optional[int]=0
    periods: List[Dict[str, int]] = field(default_factory=list)  # [{"start": 123, "end": 456}, ...]


class KudaGoAPI:
    def __init__(self, base_url: str = "https://kudago.com/public-api/v1.4"):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
   #     self.session.headers.update({
    #        "User-Agent": "EventAggregator/1.0"
     #   })

    def get_place_details(self, place_id: int) -> Optional[Dict]:
        """Получить подробную информацию о месте"""
        url = f"{self.base_url}/places/{place_id}/"

        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            try:
                data = response.json()
            except json.JSONDecodeError as e:
                logging.error(f"Ошибка JSON для place_id {place_id}: {e}")
                text = response.content.decode('utf-8', errors='replace')
                data = json.loads(text)
            return data
        except requests.RequestException as e:
            logging.error(f"Ошибка API для place_id {place_id}: {e}")
            return None
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
        # Очищаем имя таблицы: только буквы, цифры и подчёркивание
        table_name = city.lower().replace("-", "_")
        if not table_name.isidentifier():
            raise ValueError(f"Некорректное имя таблицы: {table_name}")
        # 1. Таблица places (без изменений, корректна)
        query4 = """
        CREATE TABLE IF NOT EXISTS places (
            id BIGINT PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            address TEXT,
            description TEXT,
            short_title VARCHAR(100),
            slug VARCHAR(255),
            place_url VARCHAR(500),
            site_url VARCHAR(500),
            image_url VARCHAR(500),
            lat DOUBLE PRECISION,
            lon DOUBLE PRECISION,
            phone VARCHAR(50),
            timetable TEXT,
            is_free BOOLEAN DEFAULT FALSE,
            is_closed BOOLEAN DEFAULT FALSE,
            disable_comments BOOLEAN DEFAULT FALSE,
            has_parking_lot BOOLEAN DEFAULT FALSE,
            favorites_count INTEGER DEFAULT 0,
            comments_count INTEGER DEFAULT 0,
            subway TEXT[],
            categories TEXT[],
            tags TEXT[],
            location VARCHAR(50),
            age_restriction VARCHAR(10),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """

        # 2. Таблица событий города (исправлено: FOREIGN KEY, удалены лишние кавычки)
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
            publication_date BIGINT,
            slug VARCHAR(255),
            age_restriction VARCHAR(10),
            price VARCHAR(255),
            is_free BOOLEAN,
            tags TEXT[],
            favorites_count INTEGER,
            comments_count INTEGER,
            short_title VARCHAR(255),
            disable_comments BOOLEAN,
            place_id BIGINT,
            likes BIGINT DEFAULT 0,
            added_by BIGINT, 
            CONSTRAINT fk_place
                FOREIGN KEY (place_id)
                REFERENCES places (id)
                ON DELETE SET NULL
        );
        """

        # 3. Таблица дат событий (исправлено: FOREIGN KEY)
        query2 = f"""
        CREATE TABLE IF NOT EXISTS event_dates_{table_name} (
            id SERIAL PRIMARY KEY,
            event_id BIGINT NOT NULL,
            start_timestamp BIGINT NOT NULL,
            end_timestamp BIGINT NOT NULL,
            FOREIGN KEY (event_id) REFERENCES {table_name}(id) ON DELETE CASCADE
        );
        """

        # 4. Таблица пользователей (исправлено: лишние кавычки и форматирование)
        query3 = """
        CREATE TABLE IF NOT EXISTS users (
            id BIGINT PRIMARY KEY,
            name VARCHAR(255),
            city INTEGER,
            status_ml JSONB DEFAULT '[]',
            event_history JSONB DEFAULT '[]',
            referral_code VARCHAR(50) UNIQUE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """

        # 5. Таблица referrals (исправлено: FOREIGN KEY, синтаксис)
        query5 = """
        CREATE TABLE IF NOT EXISTS referrals (
            id SERIAL PRIMARY KEY,
            referrer_id BIGINT NOT NULL,
            referred_id BIGINT NOT NULL,
            referral_code VARCHAR(50) NOT NULL,
            is_friend BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            CONSTRAINT fk_referrer
                FOREIGN KEY (referrer_id)
                REFERENCES users (id)
                ON DELETE CASCADE,
            CONSTRAINT fk_referred
                FOREIGN KEY (referred_id)
                REFERENCES users (id)
                ON DELETE CASCADE,
            CONSTRAINT unique_referral
                UNIQUE (referrer_id, referred_id)
        );
        """

        # 6. Таблица user_confirmed_events (исправлено: FOREIGN KEY, PRIMARY KEY)
        query6 = f"""
        CREATE TABLE IF NOT EXISTS user_confirmed_events (
            user_id BIGINT NOT NULL,
            event_id BIGINT NOT NULL,
            confirmed_at TIMESTAMP WITH TIME ZONE NOT NULL,
            reminder_sent BOOLEAN DEFAULT FALSE,
            PRIMARY KEY (user_id, event_id),
            FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
        );
        """

        query7 = f""" 
        CREATE TABLE IF NOT EXISTS user_event_actions (
            user_id BIGINT NOT NULL,
            event_id BIGINT NOT NULL,
            action VARCHAR(20) NOT NULL,  -- 'like', 'dislike', 'confirmed'
            timestamp TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (user_id, event_id, action)
        );
        CREATE INDEX IF NOT EXISTS idx_user_event ON user_event_actions(user_id, event_id);
            """

            # сделать инвалидное кеширование с JSONB отсюда

        query8 = f"""
        CREATE TABLE IF NOT EXISTS friends (
            user_id BIGINT NOT NULL,
            friend_id BIGINT NOT NULL,
            added_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (user_id, friend_id),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (friend_id) REFERENCES users(id) ON DELETE CASCADE
            );
            """
        query9 = f"""
        CREATE TABLE IF NOT EXISTS invitations (
            id SERIAL PRIMARY KEY,
            event_id INTEGER NOT NULL,
            sender_id BIGINT NOT NULL,      -- кто отправил
            receiver_id BIGINT NOT NULL,   -- кому отправили
            token VARCHAR(16) UNIQUE NOT NULL,
            status VARCHAR(20) NOT NULL,    -- sent, delivered, viewed, accepted, declined, failed
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE
        );

        -- Индексы для скорости поиска
        CREATE INDEX IF NOT EXISTS idx_invitations_token ON invitations(token);
        CREATE INDEX IF NOT EXISTS idx_invitations_receiver ON invitations(receiver_id);
        CREATE INDEX IF NOT EXISTS idx_invitations_event ON invitations(event_id);
        """
        with self.connection.cursor() as cursor:
            # 1. Создаём таблицу places
            cursor.execute(query4)

            # 2. Создаём таблицу событий города
            cursor.execute(query1)

            # 3. Создаём таблицу дат событий
            cursor.execute(query2)

            # 4. Создаём таблицу users
            cursor.execute(query3)
            cursor.execute(query5)
            cursor.execute(query6)
            cursor.execute(query7)
            cursor.execute(query8)
            cursor.execute(query9)
        self.connection.commit()
        logging.info(f"Таблица {table_name} создана успешно")

    def save_places(self, places: List[Place]):
        """
        Сохраняет список мест в таблицу `places`.
        Если место с таким ID уже есть — обновляет поля.
        """
        self.connect()

        # SQL-запрос с ON CONFLICT для обновления существующих записей
        query = """
            INSERT INTO places (
                id, title, address, description, short_title, slug,
                place_url, site_url, image_url,
                lat, lon,
                phone, timetable,
                is_free, is_closed, disable_comments, has_parking_lot,
                favorites_count, comments_count,
                subway, categories, tags,
                location, age_restriction,
                created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                NOW(), NOW()
            ) 
            ON CONFLICT DO NOTHING;
        """

        with self.connection.cursor() as cursor:
            for place in places:
                # Преобразуем списки в формат, понятный PostgreSQL (массивы)
                subway = place.subway if place.subway else None
                categories = place.categories if place.categories else None
                tags = place.tags if place.tags else None


                cursor.execute(query, (
                    place.id,
                    place.title,
                    place.address,
                    place.description,
                    place.short_title,
                    place.slug,
                    place.place_url,
                    place.site_url,
                    place.image_url,
                    place.lat,
                    place.lon,
                    place.phone,
                    place.timetable,
                    place.is_free,
                    place.is_closed,
                    place.disable_comments,
                    place.has_parking_lot,
                    place.favorites_count,
                    place.comments_count,
                    subway,           # передаётся как массив PostgreSQL
                    categories,       # передаётся как массив PostgreSQL
                    tags,             # передаётся как массив PostgreSQL
                    place.location,
                    place.age_restriction
                ))

        self.connection.commit()
        logging.info(f"Сохранено {len(places)} мест в БД.")

    def save_events(self, city: str, events: List[Event]):
        table_name = city.lower().replace("-", "_")
        
        # SQL-запрос (без изменений, но лучше вынести константой)
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
            status_ml,
            place_id
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s
        )
        ON CONFLICT (id) DO NOTHING
        """

        with self.connection.cursor() as cursor:
            for event in events:
                # 1. Обрабатываем place_id: проверяем наличие в таблице places
                place_id = event.place_id
                if place_id is not None:
                    cursor.execute("SELECT 1 FROM places WHERE id = %s", (place_id,))
                    if not cursor.fetchone():
                        place_id = None  # заменяем на NULL, если места нет

                # 2. Выполняем INSERT с обработанным place_id
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
                    event.status_ml,
                    place_id  # используем обработанное значение!
                ))
        
        self.connection.commit()  # commit после всех операций

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
                clusters_path:str=os.getenv('CLUSTERS_PATH')
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
    
    def _create_place_from_item(self, item: Dict) -> Place:
        """Создаёт объект Place из JSON-ответа API"""
        # Обработка координат
        coords = item.get("coords", {})
        lat = coords.get("lat")
        lon = coords.get("lon")


        # Обработка метро
        subway_str = item.get("subway", "")
        subway = [station.strip() for station in subway_str.split(",")] if subway_str else []

        # Обработка категорий
        categories = item.get("categories", [])

        # Обработка тегов
        tags = item.get("tags", [])


        return Place(
            id=item["id"],
            title=item.get("title", ""),
            address=item.get("address", ""),
            description=item.get("description", ""),
            place_url=item.get("site_url", ""),
            image_url=item.get("images", [{}])[0].get("image", "") if item.get("images") else "",
            lat=lat,
            lon=lon,
            phone=item.get("phone", ""),
            site_url=item.get("foreign_url", "") or item.get("site_url", ""),
            timetable=item.get("timetable", ""),
            is_free=item.get("is_free", False),
            favorites_count=item.get("favorites_count", 0),
            comments_count=item.get("comments_count", 0),
            slug=item.get("slug", ""),
            subway=subway,
            is_closed=item.get("is_closed", False),
            categories=categories,
            short_title=item.get("short_title", ""),
            tags=tags,
            location=item.get("location", ""),
            age_restriction=item.get("age_restriction"),
            disable_comments=item.get("disable_comments", False),
            has_parking_lot=item.get("has_parking_lot", False)
        )
    
    def _create_event_from_item(self, item: Dict) -> Event:
        # Извлекаем и преобразуем даты
        start_str = item.get("start")
        end_str = item.get("finish")

        start_dt = self._parse_datetime(start_str)
        end_dt = self._parse_datetime(end_str)

        # Определяем статус на основе дат
        status = self._get_event_status(start_dt, end_dt)

        
        place_id = None
        place_data = item.get("place")

        # Проверяем, что place_data — словарь и содержит ключ "id"
        if isinstance(place_data, dict) and "id" in place_data:
            place_id = place_data["id"]
        else:
            print(f"Ошибка: поле 'place' отсутствует или некорректно: {place_data}")

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
        images = item.get("images", [])
        categories = item.get("categories", [])

        return Event(
            title=item.get("title", ""),
            description=item.get("description", ""),
            place_name=item.get("title", "") or "",
            address=item.get("address", "") or "",
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
            disable_comments=item.get("disable_comments", False),
            place_id=place_id

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


    def sync_places(self, cities: List[str], limit: int=2000):
        """
        Синхронизирует места (places) для указанных городов:
        - получает ID мест через API;
        - загружает детали по каждому месту;
        - сохраняет в БД (с обновлением при конфликте по ID).


        Args:
            cities (List[str]): Список городов (например, ["spb", "msk"]).
            limit (int): Лимит мест на город (по умолчанию 100).
        """
        self.db.connect()


        for city in cities:

            logging.info(f"Places Обработка города: {city}")
            self.db.create_city_table(city)

            try:
                # 1. Получаем ID мест для города
                place_ids = self._get_place_ids(city, limit)
                if not place_ids:
                    logging.warning(f"Нет мест для города {city}")
                    continue

                logging.info(f"Получено {len(place_ids)} ID мест для {city}")

                # 2. Загружаем детали по каждому месту
                full_places = []
                for place_id in place_ids:
                    details = self.api.get_place_details(place_id)
                    if details:
                        full_places.append(details)
                    else:
                        logging.warning(f"Не удалось получить детали для места {place_id}")

                if not full_places:
                    logging.info(f"Нет данных для сохранения по городу {city}")
                    continue

                # 3. Преобразуем в объекты Place
                places = []
                for item in full_places:
                    try:
                        place = self._create_place_from_item(item)
                        places.append(place)
                    except Exception as e:
                        logging.error(f"Ошибка при создании Place для id={item.get('id')}: {e}")


                if not places:
                    logging.warning(f"Не создано ни одного объекта Place для города {city}")
                    continue

                # 4. Сохраняем в БД
                self.db.save_places(places)
                logging.info(f"Сохранено {len(places)} мест для города {city}")


            except Exception as e:
                logging.error(f"Ошибка при синхронизации мест для города {city}: {e}", exc_info=True)


    def _get_place_ids(self, city: str, limit: int) -> List[int]:
        """
        Получает список ID мест для указанного города через API.

        Args:
            city (str): Город (например, "spb").
            limit (int): Лимит результатов.

        Returns:
            List[int]: Список ID мест.
        """
        all_ids = []
        page = 1
        retry_count = 0
        max_retries = 3

        while True:
            try:
                params = {
                    "fields": "id",
                    "order_by": "id",
                    "location": city,
                    "page": page,
                    "limit": limit  # Ограничиваем количество на страницу
                }

                response = self.api.session.get(
                    f"{self.api.base_url}/places/",
                    params=params,
                    timeout=15
                )

                if response.status_code == 200:
                    data = response.json()
                    places = data.get("results", [])

                    if not places:
                        logging.info(f"Страница {page}: нет мест. Завершаем.")
                        break

                    # Добавляем ID мест
                    for place in places:
                        all_ids.append(place["id"])

                    logging.info(f"Страница {page}: {len(places)} мест. Всего: {len(all_ids)}")


                    # Проверяем наличие следующей страницы
                    next_page = data.get("next")
                    if not next_page:
                        logging.info("Больше страниц нет. Завершаем.")
                        break

                    page += 1
                    retry_count = 0  # Сброс счётчика попыток


                elif response.status_code == 429:
                    wait_time = 5 * (2 ** retry_count)
                    logging.warning(f"429: слишком много запросов. Пауза {wait_time} сек...")
                    time.sleep(wait_time)
                    retry_count += 1

                else:
                    logging.error(f"Ошибка {response.status_code} на странице {page}: {response.text}")
                    retry_count += 1

            except Exception as e:
                logging.error(f"Исключение на странице {page}: {e}")
                retry_count += 1

            # Проверка на превышение попыток
            if retry_count >= max_retries:
                logging.error(f"Превышено количество попыток ({max_retries}) для страницы {page}. Завершаем.")
                break

            time.sleep(0.5)  # Пауза между запросами

        return all_ids

