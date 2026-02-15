import random
import psycopg2
import json
from config import Config
from typing import List, Optional, Dict, Any
import logging 
from datetime import datetime, timezone
from datetime import timedelta
import time
import pytz  # Для явного указания часового пояса
from ml import MLService

# Настраиваем логгер
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler("status_updates.log", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)


class Database_Users:
    def __init__(self):
        self.conn = psycopg2.connect(Config.DB_DSN)
        self.ml_service = MLService()

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
    
    def update_user_status_ml(
        self, 
        user_id: int,
        status_ml: Optional[List[Dict[str, Any]]]
    ) -> bool:
        """
        Обновляет status_ml пользователя в БД.
        
        Args:
            user_id: ID пользователя
            status_ml: Список словарей вида [{"category": "cat1", "score": 0.5}, ...]
        
        Returns:
            True если обновление успешно, False в случае ошибки
        """
        # # 1. Валидация входных данных
        # if not isinstance(status_ml, list):
        #     logger.error(f"status_ml должен быть списком, получено {type(status_ml)} для user_id={user_id}")
        #     return False

        if len(status_ml) == 0:
            logger.warning(f"Пустой status_ml передан для user_id={user_id}. Операция продолжена.")

        # # Проверяем структуру каждого элемента (опционально, можно убрать если уверены в данных)
        # for item in status_ml:
        #     if not isinstance(item, dict):
        #         logger.error(f"Элемент status_ml не является словарём: {item} для user_id={user_id}")
        #         return False
        #     if "category" not in item or "score" not in item:
        #         logger.error(f"Отсутствует required поле в элементе status_ml: {item} для user_id={user_id}")
        #         return False
        #     if not isinstance(item["score"], (int, float)):
        #         logger.error(f"Поле score должно быть числом: {item['score']} для user_id={user_id}")
        #         return False

        try:
            with self.conn.cursor() as cur:
                # 2. Преобразуем в JSON
                # json_data = json.dumps(status_ml, ensure_ascii=False)
                
                cur.execute(
                    "UPDATE users SET status_ml = %s WHERE id = %s",
                    (status_ml, user_id)
                )
                
                # 3. Проверяем результат
                if cur.rowcount == 0:
                    logger.warning(f"Пользователь с ID {user_id} не найден в БД")
                    return False
                
            # 4. Commit только если всё прошло успешно
            self.conn.commit()
            logger.info(f"status_ml успешно обновлён для user_id={user_id}, записано {len(status_ml)} категорий")
            return True


        except (TypeError, ValueError) as e:
            logger.error(f"Ошибка сериализации JSON для user_id={user_id}: {e}")
            self.conn.rollback()
            return False

        except Exception as e:
            logger.exception(f"Неожиданная ошибка при обновлении status_ml для user_id={user_id}: {e}")
            self.conn.rollback()
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
        exclude_event_ids: set = None
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

        # 3. Подготовка условия исключения событий
        exclude_ids_tuple = tuple(exclude_event_ids) if exclude_event_ids else ()
        exclude_clause = ""
        if exclude_ids_tuple:
            exclude_clause = f" AND t.id NOT IN ({', '.join(['%s'] * len(exclude_ids_tuple))})"

        # 4. Единый запрос с приоритетной сортировкой
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
            WHERE TRUE {exclude_clause}
            ORDER BY
                CASE
                    WHEN 'добавленное' = ANY(t.tags) THEN 1  -- высший приоритет
                    WHEN 'интересное' = ANY(t.tags) THEN 2  -- средний приоритет
                    ELSE 3  -- низкий приоритет
                END,
                t.favorites_count DESC,      -- далее по числу лайков
                min_dates.start_datetime ASC -- затем по времени начала
            LIMIT %s
        """

        # 5. Выполняем запрос
        with self.conn.cursor() as cur:
            params = [now_ts, future_limit_ts]
            if exclude_ids_tuple:
                params.extend(exclude_ids_tuple)
            params.append(limit)
            cur.execute(query, params)
            rows = cur.fetchall()
            logger.info(f"Retrieved {len(rows)} recommended events")

        # 6. Формируем итоговый список
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
        """Добавляет запись о реферале и устанавливает дружбу."""
        try:
            with self.conn.cursor() as cur:
                # Добавляем запись о реферале
                cur.execute(
                    """
                    INSERT INTO referrals (referrer_id, referred_id, referral_code, is_friend)
                    VALUES (%s, %s, %s, TRUE)
                    ON CONFLICT (referrer_id, referred_id) DO NOTHING
                    """,
                    (referrer_id, user_id, code)
                )
                
                # Добавляем дружбу в обе стороны
                cur.execute(
                    """
                    INSERT INTO friends (user_id, friend_id)
                    VALUES (%s, %s), (%s, %s)
                    ON CONFLICT (user_id, friend_id) DO NOTHING
                    """,
                    (referrer_id, user_id, user_id, referrer_id)
                )
            
            self.conn.commit()
            logger.info(f"Реферальный переход: {referrer_id} → {user_id} (код {code}). Дружба установлена.")
            return True
        except Exception as e:
            logger.error(f"Ошибка при добавлении реферала {user_id}: {e}")
            self.conn.rollback()
            return False

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
        """
        Возвращает данные мероприятия по ID из указанной таблицы.
        
        :param event_id: ID мероприятия
        :param table_name: Название таблицы (должно быть проверено заранее)
        :return: Словарь с данными мероприятия или None
        """
        # Список разрешённых таблиц (защита от SQL‑инъекций)
        ALLOWED_TABLES = {"msk", "spb"}
        
        if table_name not in ALLOWED_TABLES:
            logger.error(f"[DB] Запрещённая таблица: {table_name}")
            return None

        try:
            with self.conn.cursor() as cur:
                # Используем параметризованный запрос только для значений, а не для имён таблиц
                query = f"""
                    SELECT 
                        id,
                        title,
                        description,
                        start_datetime,
                        event_url
                    FROM {table_name}
                    WHERE id = %s
                """
                cur.execute(query, (event_id,))
                row = cur.fetchone()

                if not row:
                    return None

                # Явное сопоставление колонок (защита от изменения порядка)
                return {
                    "id": row[0],
                    "title": row[1] or "",  # Если NULL → пустая строка
                    "description": row[2] or "",
                    "start_datetime": row[3],
                    "event_url": row[4] or ""
                }
        except Exception as e:
            logger.error(f"[DB] Ошибка получения мероприятия {event_id} из таблицы {table_name}: {e}")
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



        #---Логика, связанная с друзьями




    def get_friends(self, user_id: int) -> List[Dict]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT f.friend_id, u.name
                FROM friends f
                JOIN users u ON f.friend_id = u.id
                WHERE f.user_id = %s
                ORDER BY u.name
                """,
                (user_id,)
            )
            rows = cur.fetchall()
        
        # Отладка: проверим, что rows — это список кортежей
        print("Raw rows from DB:", rows)  # например: [(123, 'Иван'), (456, None)]
        
        result = [
            {"id": row[0], "name": row[1] or f"Друг {row[0]}"}
            for row in rows
        ]
        
        # Отладка: проверим итоговый результат
        print("Result list:", result)
        
        return result
    
    def remove_friend(self, user_id: int, friend_id: int) -> bool:
        """Удаляет друга из списка."""
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM friends WHERE user_id = %s AND friend_id = %s",
                    (user_id, friend_id)
                )
            self.conn.commit()
            logger.info(f"Друг {friend_id} удалён для пользователя {user_id}")
            return True
        except Exception as e:
            logger.error(f"Ошибка при удалении друга {friend_id} для {user_id}: {e}")
            self.conn.rollback()
            return False

    def get_friends(self, user_id: int) -> List[int]:
        """Возвращает список ID друзей пользователя."""
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT friend_id FROM friends WHERE user_id = %s ORDER BY added_at DESC",
                    (user_id,)
                )
                rows = cur.fetchall()
            return [row[0] for row in rows]
        except Exception as e:
            logger.error(f"Ошибка при получении друзей для {user_id}: {e}")
            return []

    def are_friends(self, user_id: int, friend_id: int) -> bool:
        """Проверяет, являются ли пользователи друзьями."""
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM friends WHERE user_id = %s AND friend_id = %s",
                    (user_id, friend_id)
                )
                return cur.fetchone() is not None
        except Exception as e:
            logger.error(f"Ошибка при проверке дружбы между {user_id} и {friend_id}: {e}")
            return False
        
        
    def get_confirmed_events_for_user(self, user_id: int) -> List[Dict]:
        """
        Возвращает список подтверждённых мероприятий для указанного пользователя.
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        e.event_id,
                        e.title,
                        e.start_datetime,
                        e.event_url,
                        t.city
                    FROM user_confirmed_events uce
                    JOIN (
                        SELECT id, title, start_datetime, event_url, 'msk' AS city FROM msk
                        UNION ALL
                        SELECT id, title, start_datetime, event_url, 'spb' AS city FROM spb
                    ) e ON uce.event_id = e.event_id
                    WHERE uce.user_id = %s
                    AND uce.confirmed_at IS NOT NULL
                    ORDER BY e.start_datetime
                    """, (user_id,))

                rows = cur.fetchall()
                return [
                    {
                        "event_id": r[0],
                        "title": r[1],
                        "start_datetime": r[2],
                        "event_url": r[3],
                        "city": r[4]
                    }
                    for r in rows
                ]
        except Exception as e:
            logger.error(f"[DB] Ошибка при получении подтверждённых событий для {user_id}: {e}")
            return []
        
    def save_invitation(self, event_id: int, sender_id: int, receiver_id: int, token: str, status: str):
        """Сохраняет приглашение в БД."""
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO invitations (event_id, sender_id, receiver_id, token, status, created_at) "
                "VALUES (%s, %s, %s, %s, %s, NOW())",
                (event_id, sender_id, receiver_id, token, status)
            )
        self.conn.commit()

    def get_invitation_by_token(self, token: str) -> dict | None:
        """Получает приглашение по токену."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM invitations WHERE token = %s",
                (token,)
            )
            row = cur.fetchone()
            return dict(row) if row else None


    def update_invitation_status(self, token: str, status: str):
        """Обновляет статус приглашения."""
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE invitations SET status = %s, updated_at = NOW() WHERE token = %s",
                (status, token)
            )
        self.conn.commit()


    def get_all_users_except(self, excluded_user_id: int) -> list[dict]:
        """
        Возвращает список всех пользователей, кроме указанного по ID.
        
        :param excluded_user_id: ID пользователя, которого нужно исключить
        :return: список словарей с данными пользователей
        """
        try:
            query = """
                SELECT id, name, city, created_at
                FROM users
                WHERE id != %s
                ORDER BY name
            """
            with self.conn.cursor() as cur:
                cur.execute(query, (excluded_user_id,))
                # Получаем имена колонок
                columns = [col[0] for col in cur.description]
                # Преобразуем каждую строку (кортеж) в словарь
                result = [dict(zip(columns, row)) for row in cur.fetchall()]
            return result
        except Exception as e:
            logger.error(f"[ERROR] get_all_users_except: {e}")
            return []
    def get_confirmed_future_events(self, user_id: int) -> List[Dict]:
        """
        Возвращает подтверждённые и ещё не прошедшие мероприятия пользователя.
        
        :param user_id: ID пользователя
        :return: список событий с полями: event_id, title, start_datetime, event_url, city
        """
        try:
            # Текущее время в UTC (как в БД)
            now_ts = int(datetime.now(timezone.utc).timestamp())

            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        e.event_id,
                        e.title,
                        e.start_datetime,
                        e.event_url,
                        t.city
                    FROM user_confirmed_events uce
                    JOIN (
                        SELECT id, title, start_datetime, event_url, 'msk' AS city FROM msk
                        UNION ALL
                        SELECT id, title, start_datetime, event_url, 'spb' AS city FROM spb
                    ) e ON uce.event_id = e.event_id
                    WHERE uce.user_id = %s
                    AND uce.confirmed_at IS NOT NULL
                    AND e.start_datetime > %s  -- Только будущие события
                    ORDER BY e.start_datetime
                    """, (user_id, now_ts))

                rows = cur.fetchall()
                return [
                    {
                        "event_id": r[0],
                        "title": r[1],
                        "start_datetime": r[2],
                        "event_url": r[3],
                        "city": r[4]
                    }
                    for r in rows
                ]
        except Exception as e:
            logger.error(f"[DB] Ошибка при получении будущих подтверждённых событий для {user_id}: {e}")
            return []
        
    def get_place_by_event_id(self, event_id: int, table_name: str) -> Optional[dict]:
        """
        Возвращает данные места (название, адрес, сайт) по event_id из указанной таблицы событий.

        :param event_id: ID мероприятия
        :param table_name: Название таблицы событий ('msk' или 'spb')
        :return: Словарь с полями 'title', 'address', 'site_url' или None
        """
        # Список разрешённых таблиц (защита от SQL‑инъекций)
        ALLOWED_TABLES = {"msk", "spb"}

        if table_name not in ALLOWED_TABLES:
            logger.error(f"[DB] Запрещённая таблица: {table_name}")
            return None

        try:
            with self.conn.cursor() as cur:
                # Шаг 1: получаем place_id из таблицы событий по event_id
                query_event = f"""
                    SELECT place_id
                    FROM {table_name}
                    WHERE id = %s
                """
                cur.execute(query_event, (event_id,))
                event_row = cur.fetchone()

                if not event_row or not event_row[0]:
                    return None  # Нет place_id или event_id не найден

                place_id = event_row[0]

                # Шаг 2: получаем данные места из таблицы places
                query_place = """
                    SELECT
                        title,
                        address,
                        site_url
                    FROM places
                    WHERE id = %s
                """
                cur.execute(query_place, (place_id,))
                place_row = cur.fetchone()

                if not place_row:
                    return None  # Место не найдено по place_id

                # Формируем результат
                return {
                    "title": place_row[0] or "",
                    "address": place_row[1] or "",
                    "site_url": place_row[2] or ""
                }

        except Exception as e:
            logger.error(f"[DB] Ошибка получения места для event_id={event_id} из таблицы {table_name}: {e}")
            return None
        
    


    def add_event(
        self,
        table_name: str,
        title: str,
        description: str,
        start_datetime: int,
        event_url: str,
        added_by: int,
        status_ml: List[Dict]=None

   # Ожидаем список диктов
    ) -> bool:

        ALLOWED_TABLES = {"msk", "spb"}

        # 1. Проверка таблицы
        if table_name not in ALLOWED_TABLES:
            logger.error(f"[DB] Запрещённая таблица: {table_name}")
            return False


        status_ml =[
                {
                    "score": 0.9,
                    "category": "Nostalgia‑поколение",
                    "description": ""
                },
                {
                    "score": 0.9,
                    "category": "Романтики‑эстеты",
                    "description": ""
                },
                {
                    "score": 0.9,
                    "category": "Поклонники стендапа",
                    "description": ""
                },
                {
                    "score": 0.9,
                    "category": "Ценители оперного и вокального искусства",
                    "description": ""
                },
                {
                    "score": 0.9,
                    "category": "Ночные искатели приключений",
                    "description": ""
                },
                {
                    "score": 0.9,
                    "category": "Джаз‑адепты",
                    "description": ""
                },
                {
                    "score": 0.9,
                    "category": "Рок‑энтузиасты",
                    "description": ""
                },
                {
                    "score": 0.9,
                    "category": "Любители гастрономического театра",
                    "description": ""
                },
                {
                    "score": 0.9,
                    "category": "Фанаты мюзиклов и Бродвея",
                    "description": ""
                },
                {
                    "score": 0.9,
                    "category": "Ностальгирующие романтики",
                    "description": ""
                }
            
            ]

        max_attempts = 100
        candidate_id = None

        # 5. Генерация уникального ID
        for attempt in range(max_attempts):
            candidate_id = random.randint(1_000_000, 9_999_999)
            try:
                with self.conn.cursor() as cur:
                    cur.execute(
                        f"SELECT 1 FROM {table_name} WHERE id = %s",
                        (candidate_id,)
                    )
                    if not cur.fetchone():
                        break
            except Exception as e:
                logger.error(f"[DB] Ошибка при проверке ID {candidate_id}: {e}")
                return False
        else:
            logger.error(f"[DB] Не удалось сгенерировать уникальный ID после {max_attempts} попыток")
            return False

        # 6. Вставка в БД
        try:
            with self.conn.cursor() as cur:
                # Основной запрос (с полем status_ml)
                query_main = f"""
                    INSERT INTO {table_name} (
                        id, title, description, event_url, added_by, tags, status_ml
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s
                    )
                """
                tags_list = ['добавленное']
                cur.execute(
                    query_main,
                    (
                        candidate_id,
                        title,
                        description,
                        event_url,
                        added_by,
                        tags_list,
                        json.dumps(status_ml)  # Преобразуем список в JSONB
                    )
                )

                # Вставка в таблицу дат
                query_dates = f"""
                    INSERT INTO event_dates_{table_name} (
                        event_id, start_timestamp, end_timestamp
                    ) VALUES (
                        %s, %s, %s
                    )
                """
                cur.execute(query_dates, (candidate_id, start_datetime, start_datetime))

            self.conn.commit()
            logger.info(f"Мероприятие '{title}' добавлено с ID={candidate_id}")


            # 7. ML-обработка (если нужно)
            try:
                new_event = {
                    "id": candidate_id,
                    "title": title,
                    "description": description,
                    "status_ml": status_ml
                }
                self.ml_service.encode_text(f"{title} {description}")
                logger.info(f"ML-обработка события ID={candidate_id} завершена")
            except Exception as ml_e:
                logger.error(f"[ML] Ошибка при обработке события ID={candidate_id}: {ml_e}")

            return True

        except Exception as e:
            self.conn.rollback()
            logger.error(f"[DB] Ошибка при добавлении мероприятия: {e}")
            return False



    def get_recommended_interest(
        self,
        table_name: str,
        limit: int = 12,
        months_ahead: float = 1.5,
        use_local_time: bool = False,
        exclude_event_ids: set = None
    ) -> list:
        """
        Возвращает мероприятия: сначала с тегом 'добавленное', затем с тегом 'интересное'.
        Сортировка внутри групп: по likes (DESC), затем по времени начала (ASC).
        Общий лимит — limit.
        """

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

        # 3. Подготовка условия исключения событий
        exclude_ids_tuple = tuple(exclude_event_ids) if exclude_event_ids else ()
        exclude_clause = ""
        if exclude_ids_tuple:
            exclude_clause = f" AND t.id NOT IN ({', '.join(['%s'] * len(exclude_ids_tuple))})"

        # 4. Запрос для событий с тегом 'добавленное'
        query_added = f"""
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
            WHERE 'добавленное' = ANY(t.tags) {exclude_clause}
            ORDER BY
                t.favorites_count DESC,
                min_dates.start_datetime ASC
            LIMIT %s
        """

        # 5. Запрос для событий с тегом 'интересное'
        query_interesting = f"""
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
            WHERE 'интересное' = ANY(t.tags) {exclude_clause}
            ORDER BY
                t.favorites_count DESC,
                min_dates.start_datetime ASC
            LIMIT %s
        """

        all_rows = []

        # 6. Выполняем запрос для 'добавленное'
        with self.conn.cursor() as cur:
            params_added = [now_ts, future_limit_ts]
            if exclude_ids_tuple:
                params_added.extend(exclude_ids_tuple)
            params_added.append(limit)  # Лимит на первую группу (можно скорректировать)

            cur.execute(query_added, params_added)
            rows_added = cur.fetchall()
            all_rows.extend(rows_added)

        # Если уже набрали limit, обрезаем
        if len(all_rows) >= limit:
            all_rows = all_rows[:limit]
        else:
            # 7. Выполняем запрос для 'интересное' (с учётом уже выбранных)
            remaining = limit - len(all_rows)
            if remaining > 0:
                with self.conn.cursor() as cur:
                    params_interesting = [now_ts, future_limit_ts]
                    if exclude_ids_tuple:
                        params_interesting.extend(exclude_ids_tuple)
                    params_interesting.append(remaining)

                    cur.execute(query_interesting, params_interesting)
                    rows_interesting = cur.fetchall()
                    all_rows.extend(rows_interesting)

        # 8. Формируем итоговый список
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
            for r in all_rows
        ]