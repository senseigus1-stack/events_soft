import psycopg2
import json
from config import Config
from typing import List
import logging
from datetime import datetime
from datetime import timedelta
import time
import pytz  # Для явного указания часового пояса
# Настраиваем логгер
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler("status_updates.log", encoding="utf-8"),
        logging.StreamHandler()  # вывод в консоль
    ]
)
logger = logging.getLogger(__name__)


class Database_Users:
    def __init__(self):
        self.conn = psycopg2.connect(Config.DB_DSN)

    # def create_table(self):

    #     if not self.conn:
    #         self.conn = psycopg2.connect(Config.DB_DSN)

    #     try:
    #         with self.conn.cursor() as cur:
    #             cur.execute("""
    #                 CREATE TABLE IF NOT EXISTS users (
    #                     id BIGINT PRIMARY KEY,
    #                     city INTEGER,
    #                     status_ml JSONB DEFAULT '[]',
    #                     event_history JSONB DEFAULT '[]'
    #                     );
    #                 """)
    # # 1 - msk
    # # 2 - spb
    # # 3 - msk & spb


    #         self.conn.commit()
    #         print("Table created successfully (if not already present).")

    #     except psycopg2.Error as e:
    #         print(f"Database error: {e}")
    #         if self.conn:
    #             self.conn.rollback()

    #     except Exception as e:
    #         print(f"!Unexpected error: {e}")
    #         if self.conn:
    #             self.conn.rollback()



    def get_user(self, user_id: int):
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT id, city, status_ml, event_history FROM users WHERE id = %s",
                (user_id,)
            )
            row = cur.fetchone()

        if row is None:
            return None

        return {
            "id": row[0],
            "city": row[1],
            "status_ml": row[2] if row[2] is not None else [],  # Возвращает строку или None → []
            "event_history": row[3] if row[3] is not None else []  # То же самое
        }

    def update_user_status_ml(self, user_id: int, status_ml: List) -> bool:
        
        # # Проверка типа
        # if not isinstance(status_ml, list):
        #     print(f"Ошибка: status_ml должен быть списком, получено {type(status_ml)}")
        #     return False

        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET status_ml = %s WHERE id = %s",
                    (json.dumps(status_ml), user_id)
                )
                # Проверяем, затронута ли хотя бы одна строка
                if cur.rowcount == 0:
                    print(f"Пользователь с ID {user_id} не найден")
                    return False

            self.conn.commit()
            return True

        except Exception as e:
            print(f"Ошибка при обновлении status_ml для user_id={user_id}: {e}")
            self.conn.rollback()  # Откат транзакции при ошибке
            return False

    def add_event_to_history(self, user_id: int, event_id: int, rating: str):
        user = self.get_user(user_id)
        history = user["event_history"]
        history.append({"event_id": event_id, "rating": rating})
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET event_history = %s WHERE id = %s",
                (json.dumps(history[-Config.MAX_HISTORY:]), user_id)
            )
        self.conn.commit()


    # def get_recommended_events(self, table_name: str, limit: int = 10) -> list:
    #     with self.conn.cursor() as cur:
    #         cur.execute(f"""
    #             SELECT id, title, description, start_datetime, event_url, status_ml
    #             FROM {table_name}
    #             ORDER BY RANDOM()
    #             LIMIT %s
    #         """, (limit,))
    #         rows = cur.fetchall()
    #         return [
    #             {
    #                 "id": r[0], "title": r[1], "description": r[2],
    #                 "start_datetime": r[3], "event_url": r[4],
    #                 "status_ml": r[5]
    #             }
    #             for r in rows
    #         ]
 

    def get_recommended_events(
        self,
        table_name: str,
        limit: int = 40,
        months_ahead: float = 1.5,
        use_local_time: bool = False
    ) -> list:
        
        # 1. Определяем текущее время в UTC (рекомендуется)
        if use_local_time:
            # Если нужен локальный часовой пояс (например, МСК)
            local_tz = pytz.timezone('Europe/Moscow')
            now = datetime.now(local_tz)
        else:
            now = datetime.utcnow().replace(tzinfo=pytz.utc)  # UTC с явным tzinfo

        # 2. Рассчитываем границу
        total_days = int(months_ahead * 30)
        future_limit = now + timedelta(days=total_days)
        
        # Преобразуем в UNIX-timestamp (целое число)
        now_ts = int(now.timestamp())
        future_limit_ts = int(future_limit.timestamp())

        # 3. SQL-запрос: выбираем ближайшую будущую дату для каждого события
        with self.conn.cursor() as cur:
            cur.execute(f"""
                SELECT
                    t.id,
                    t.title,
                    t.description,
                    min_dates.start_datetime,
                    t.event_url,
                    t.status_ml
                FROM {table_name} t
                JOIN (
                    SELECT
                        event_id,
                        MIN(start_timestamp) AS start_datetime
                    FROM event_dates_{table_name}
                    WHERE start_timestamp >= %s    -- Только будущие даты
                    AND start_timestamp <= %s  -- В пределах 1.5 месяцев
                    GROUP BY event_id
                ) min_dates ON t.id = min_dates.event_id
                ORDER BY RANDOM()
                LIMIT %s
            """, (now_ts, future_limit_ts, limit))

            rows = cur.fetchall()

        # 4. Формируем ответ
        return [
            {
                "id": r[0],
                "title": r[1],
                "description": r[2],
                "start_datetime": int(r[3]) if r[3] is not None else None,
                "event_url": r[4],
                "status_ml": r[5]
            }
            for r in rows
        ]