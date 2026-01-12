import psycopg2
import logging
from typing import List, Dict, Optional
from dataclasses import dataclass, field

@dataclass
class User:
    """User model"""
    tg_id: int
    name: str
    address: str
    status_ml: str
    tags: List[str]              

class Database_Users:
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

    def create_users_table(self):
        
        # Основной запрос для таблицы событий города
        query = f"""
        CREATE TABLE IF NOT EXISTS users (
            tg_id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            address TEXT,
            status_ml JSONB,
            tags TEXT[],
        );
        """
        
        with self.connection.cursor() as cursor:
            cursor.execute(query)
        
        self.connection.commit()
        
    def save_users(self, users: List[User]):
        query = f"""
        INSERT INTO users (

            tg_id BIGINT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            address TEXT,
            status_ml JSONB,
            tags TEXT[],

        ) VALUES (
            %s, %s, %s, %s, %s
        )
        """

        with self.connection.cursor() as cursor:
            for user in users:
                cursor.execute(query, (
                    user.tg_id,
                    user.name,
                    user.address,
                    user.status_ml,
                    user.tags
                ))
        self.connection.commit()

    def get_events_description(self, ) -> List[Dict]:
        query = """
            SELECT description
            FROM msk
            UNION ALL
            SELECT description
            FROM spb
        """
        # Если city строго соответствует названию таблицы, можно упростить:
        # query = f"SELECT description FROM {city};"
        
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
        
        return [{"description": row[0]} for row in results]

    def close(self):
        if self.connection:
            self.connection.close()





