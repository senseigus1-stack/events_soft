
import psycopg2
import json
from config import Config
from typing import List, Optional
import logging
from datetime import datetime, timezone
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

    def add_event_to_history(self, user_id: int, event_id: int, rating: str) -> bool:
        """
        Добавляет событие в историю пользователя с ограничением по размеру.
        Возвращает True при успехе, False при ошибке.
        """
        # Валидация входных данных
        if not isinstance(user_id, int) or not isinstance(event_id, int):
            logging.error(f"Некорректные ID: user_id={user_id}, event_id={event_id}")
            return False

        if rating not in ["like", "dislike", "confirmed"]:
            logging.error(f"Недопустимый рейтинг: {rating}")
            return False

        try:
            # Получаем текущую историю (отдельная транзакция)
            user = self.get_user(user_id)
            if not user:
                logging.error(f"Пользователь не найден: {user_id}")
                return False

            history = user.get("event_history", [])
            
            # Удаляем дубликаты (если событие уже есть в истории)
            history = [
                item for item in history 
                if item["event_id"] != event_id
            ]
            
            # Добавляем новое событие
            new_entry = {
                "event_id": event_id,
                "rating": rating,
                "timestamp": int(datetime.now().timestamp())  # Важно: время добавления
            }
            history.append(new_entry)

            # Ограничиваем размер истории
            MAX_HISTORY = 100  # Лучше вынести в конфиг
            history = history[-MAX_HISTORY:]

            # Обновляем БД
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE users 
                    SET event_history = %s
                    WHERE id = %s
                    AND event_history IS DISTINCT FROM %s  -- Оптимизация: не обновлять, если данные не изменились
                    """,
                    (json.dumps(history, ensure_ascii=False), user_id, json.dumps(history))
                )
                
                # Проверяем, была ли запись обновлена
                if cur.rowcount == 0:
                    logging.warning(f"История не обновлена (возможно, дубликат): user_id={user_id}, event_id={event_id}")
                    return False

            self.conn.commit()
            return True

        except Exception as e:
            logging.exception(f"Ошибка при добавлении в историю: user_id={user_id}, event_id={event_id}, ошибка={e}")
            # Не коммитим транзакцию при ошибке
            return False


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
        limit: int = 50,
        months_ahead: float = 1.5,
        use_local_time: bool = False,
        exclude_event_ids: set = None  # Новый параметр!
    ) -> list:
        
        # 1. Определяем текущее время
        if use_local_time:
            local_tz = pytz.timezone('Europe/Moscow')
            now = datetime.now(local_tz)
        else:
            now = datetime.utcnow().replace(tzinfo=pytz.utc)

        # 2. Рассчитываем временной интервал
        total_days = int(months_ahead * 30)
        future_limit = now + timedelta(days=total_days)
        now_ts = int(now.timestamp())
        future_limit_ts = int(future_limit.timestamp())

        # 3. Формируем условие для исключения событий
        exclude_ids_tuple = tuple(exclude_event_ids) if exclude_event_ids else ()
        exclude_clause = ""
        if exclude_ids_tuple:
            exclude_clause = f" AND t.id NOT IN ({', '.join(['%s'] * len(exclude_ids_tuple))})"

        # 4. SQL-запрос с JOIN к таблице places и исключением событий
        with self.conn.cursor() as cur:
            query = f"""
                SELECT
                    t.id,
                    t.title,
                    t.description,
                    min_dates.start_datetime,
                    t.event_url,
                    t.status_ml,
                    t.favorites_count,
                    p.address,
                    p.title AS place_title
                FROM {table_name} t
                JOIN (
                    SELECT
                        event_id,
                        MIN(start_timestamp) AS start_datetime
                    FROM event_dates_{table_name}
                    WHERE start_timestamp >= %s
                    AND start_timestamp <= %s
                    GROUP BY event_id
                ) min_dates ON t.id = min_dates.event_id
                LEFT JOIN places p ON t.place_id = p.id
                WHERE 1=1  -- Базовая условная конструкция для удобства добавления условий
                {exclude_clause}
                ORDER BY t.favorites_count DESC, min_dates.start_datetime ASC
                LIMIT %s
            """
            
            # Формируем параметры для запроса
            params = [now_ts, future_limit_ts]
            if exclude_ids_tuple:
                params.extend(exclude_ids_tuple)
            params.append(limit)

            cur.execute(query, params)
            rows = cur.fetchall()

        # 5. Формируем результат
        return [
            {
                "id": r[0],
                "title": r[1],
                "description": r[2],
                "start_datetime": int(r[3]) if r[3] is not None else None,
                "event_url": r[4],
                "status_ml": r[5],
                "likes": r[6] if r[6] is not None else 0,
                "address": r[7] if r[7] is not None else "",
                "place_title": r[8] if r[8] is not None else ""
            }
            for r in rows
        ]


    # --- Реферальная система ---

    def save_referral_code(self, user_id: int, code: str) -> bool:
        """Сохраняет реферальный код пользователя, если его ещё нет."""
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE users 
                    SET referral_code = %s
                    WHERE id = %s AND referral_code IS NULL
                    """,
                    (code, user_id)
                )
                # Если строка обновлена (т.е. код был установлен)
                if cur.rowcount > 0:
                    self.conn.commit()
                    logger.info(f"Реферальный код {code} сохранён для пользователя {user_id}")
                    return True
                else:
                    logger.info(f"У пользователя {user_id} уже есть реферальный код")
                    return False
        except Exception as e:
            logger.error(f"Ошибка при сохранении реферального кода для {user_id}: {e}")
            self.conn.rollback()
            return False

    def get_user_by_referral_code(self, code: str) -> Optional[int]:
        """Возвращает ID пользователя по реферальном коду."""
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM users WHERE referral_code = %s",
                    (code,)
                )
                row = cur.fetchone()
                return row[0] if row else None
        except Exception as e:
            logger.error(f"Ошибка при поиске пользователя по коду {code}: {e}")
            return None

    def is_already_referred(self, user_id: int, referrer_id: int) -> bool:
        """Проверяет, был ли пользователь уже приглашён этим реферером."""
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1 FROM referrals
                    WHERE referred_id = %s AND referrer_id = %s
                    """,
                    (user_id, referrer_id)
                )
                return cur.fetchone() is not None
        except Exception as e:
            logger.error(f"Ошибка при проверке реферального статуса {user_id}: {e}")
            return False

    def add_referral(self, user_id: int, referrer_id: int, code: str) -> bool:
        """Добавляет запись о реферале в БД."""
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO referrals (referrer_id, referred_id, referral_code)
                    VALUES (%s, %s, %s)
                    """,
                    (referrer_id, user_id, code)
                )
            self.conn.commit()
            logger.info(f"Реферальный переход: {referrer_id} → {user_id} (код {code})")
            return True
        except Exception as e:
            logger.error(f"Ошибка при добавлении реферала {user_id}: {e}")
            self.conn.rollback()

    def get_upcoming_confirmed(self, days_ahead: int = 1) -> list:

        try:
            # Расчёт временного интервала
            now = datetime.now(timezone.utc)
            target_start = now + timedelta(days=days_ahead - 0.1)
            target_end = now + timedelta(days=days_ahead + 0.1)

            start_ts = int(target_start.timestamp())
            end_ts = int(target_end.timestamp())

            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        uce.user_id,
                        e.event_id,
                        e.title,
                        e.start_datetime,
                        e.event_url,
                        e.city
                    FROM user_confirmed_events uce
                    JOIN (
                        -- Объединяем события из msk и spb
                        SELECT 
                            id AS event_id,
                            title,
                            start_datetime,
                            event_url,
                            'msk' AS city
                        FROM msk
                        UNION ALL
                        SELECT
                            id AS event_id,
                            title,
                            start_datetime,
                            event_url,
                            'spb' AS city
                        FROM spb
                    ) e ON uce.event_id = e.event_id
                    WHERE e.start_datetime BETWEEN %s AND %s
                    AND uce.reminder_sent = FALSE
                    ORDER BY e.start_datetime
                    """, (start_ts, end_ts))

                rows = cur.fetchall()
                return [
                    {
                        "user_id": r[0],
                        "event_id": r[1],
                        "title": r[2],
                        "start_datetime": r[3],
                        "event_url": r[4],
                        "city": r[5]
                    }
                    for r in rows
                ]
        except Exception as e:
            logger.error(f"[DB] Ошибка при получении подтверждённых мероприятий: {e}")
            return []
        

    def confirm_event(self, user_id: int, event_id: int) -> bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO user_confirmed_events (user_id, event_id, confirmed_at, reminder_sent)
                    VALUES (%s, %s, %s, FALSE)
                    ON CONFLICT (user_id, event_id) DO NOTHING
                    """,
                    (user_id, event_id, datetime.now(timezone.utc))
                )
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"[DB] Ошибка подтверждения мероприятия {event_id} для {user_id}: {e}")
            return False
        
    def mark_reminder_sent(self, user_id: int, event_id: int) -> bool:
        """Помечает, что напоминание для пользователя и мероприятия уже отправлено."""
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE user_confirmed_events
                    SET reminder_sent = TRUE
                    WHERE user_id = %s AND event_id = %s
                    """,
                    (user_id, event_id)
                )
                self.conn.commit()
                return cur.rowcount > 0
        except Exception as e:
            logger.error(f"[DB] Ошибка отметки отправленного напоминания: {e}")
            return False
        
    
    def get_event_by_id(self, event_id: int, table_name: str) -> Optional[dict]:
        """Возвращает данные мероприятия по ID из указанной таблицы."""
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"""
                    SELECT id, title, description, start_datetime, event_url
                    FROM {table_name}
                    WHERE id = %s
                    """, (event_id,))
                row = cur.fetchone()
                if row:
                    return {
                        "id": row[0],
                        "title": row[1],
                        "description": row[2],
                        "start_datetime": row[3],
                        "event_url": row[4]
                    }
                return None
        except Exception as e:
            logger.error(f"[DB] Ошибка получения мероприятия {event_id}: {e}")
            return None
        
    def increment_event_likes(self, event_id: int, table_name: str) -> bool:
        """
        Увеличивает счётчик likes у события на 1.
        Возвращает True, если обновление прошло успешно.
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {table_name} SET likes = likes + 1 WHERE id = %s",
                    (event_id,)
                )
                if cur.rowcount == 0:
                    logger.warning(f"Событие {event_id} не найдено в таблице {table_name}")
                    return False
            self.conn.commit()
            logger.info(f"Событие {event_id}: лайк добавлен (+1)")
            return True
        except Exception as e:
            logger.error(f"Ошибка при увеличении likes для event_id={event_id}: {e}")
            self.conn.rollback()
            return False
