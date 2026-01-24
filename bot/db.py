import psycopg2
import json
from config import Config


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
            cur.execute("SELECT id, city, status_ml, event_history FROM users WHERE id = %s", (user_id,))
            row = cur.fetchone()
            if row:
                return {
                    "id": row[0],
                    "city": row[0],
                    "status_ml": json.loads(row[1]) if row[1] else [],
                    "event_history": json.loads(row[2]) if row[2] else []
                }
        return None

    def update_user_status_ml(self, user_id: int, status_ml: list):
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET status_ml = %s WHERE id = %s",
                (json.dumps(status_ml), user_id)
            )
        self.conn.commit()

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


    def get_recommended_events(self, table_name: str, limit: int = 10) -> list:
        with self.conn.cursor() as cur:
            cur.execute(f"""
                SELECT id, title, description, start_datetime, event_url, status_ml
                FROM {table_name}
                ORDER BY RANDOM()
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
            return [
                {
                    "id": r[0], "title": r[1], "description": r[2],
                    "start_datetime": r[3], "event_url": r[4],
                    "status_ml": json.loads(r[5])
                }
                for r in rows
            ]