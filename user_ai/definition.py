import os
import random
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from typing import List, Dict, Tuple
import json
from database import Database_Users
import logging
from pydantic import BaseModel


logger = logging.getLogger(__name__)


class Cluster(BaseModel):
    название: str
    возраст: str
    интересы: List[str]
    предпочтения: List[str]
    мотивация: List[str]

# --- КОНФИГУРАЦИЯ ---
def load_clusters_from_file(filepath: str) -> List[Cluster]:
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        return [Cluster(**item) for item in data]
    except FileNotFoundError:
        logger.error(f"Файл {filepath} не найден")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка декодирования JSON в файле {filepath}: {e}")
        raise
    except Exception as e:
        logger.error(f"Ошибка при загрузке кластеров из файла {filepath}: {e}")
        raise
# Модели и векторизация
MODEL_NAME = "all-MiniLM-L6-v2"
vectorizer = SentenceTransformer(MODEL_NAME)

# Кластеры интересов (ваши данные)
CLUSTERS = load_clusters_from_file('ai\clusters.json')

    # Параметры подключения к БД
DB_DSN = (
        f"dbname={os.getenv('DB_NAME')} "
        f"user={os.getenv('DB_USER')} "
        f"password={os.getenv('DB_PASSWORD')} "
        f"host={os.getenv('DB_HOST')} "
        f"port={os.getenv('DB_PORT')} "
        f"options='-c client_encoding=UTF8'"
    )

# Укажите DSN (строку подключения к PostgreSQL)
dsn = DB_DSN

# Создаём экземпляр класса
db = Database_Users(dsn)

db.connect()
# Список мероприятий (для демонстрации)
EVENTS = db.get_events_description()



# --- ВЕКТОРИЗАЦИЯ ---

def vectorize_text(text: str) -> np.ndarray:
    """Векторизует текст с помощью SentenceTransformer."""
    return vectorizer.encode([text])[0]

def vectorize_cluster(cluster: Dict) -> np.ndarray:
    """Создаёт вектор кластера на основе его интересов, предпочтений и мотивации."""
    text = " ".join(
        cluster["интересы"] + 
        cluster["предпочтения"] +
        cluster["мотивация"]
    )
    return vectorize_text(text)

def cosine_sim(vec1: np.ndarray, vec2: np.ndarray) -> float:
    """Вычисляет косинусное сходство между двумя векторами."""
    return cosine_similarity([vec1], [vec2])[0][0]

# --- ОСНОВНАЯ ЛОГИКА ---

def get_user_preferences() -> List[Tuple[str, bool]]:
    """
    Выдаёт пользователю 10–15 случайных мероприятий и собирает ответы.
    Возвращает список пар: (название_мероприятия, понравилось_ли).
    """
    selected_events = random.sample(EVENTS, random.randint(10, 15))
    preferences = []

    print("Привет! Ответь «да» или «нет» на каждое мероприятие:")
    print('-!' * 50)

    for event in selected_events:
        answer = input(f"{event}: ").strip().lower()
        liked = answer == "да"
        preferences.append((event, liked))
        print("✓" if liked else "✗")

    return preferences

def calculate_cluster_score(preferences: List[Tuple[str, bool]], cluster_vector: np.ndarray) -> float:
    """
    Считает «рейтинг» кластера на основе предпочтений пользователя.
    Для каждого понравившегося мероприятия: +сходство с кластером.
    Для каждого не понравившегося: -сходство с кластером.
    """
    total_score = 0.0

    for event, liked in preferences:
        event_vector = vectorize_text(event)
        similarity = cosine_sim(event_vector, cluster_vector)

        if liked:
            total_score += similarity
        else:
            total_score -= similarity

    return total_score

def determine_user_cluster(preferences: List[Tuple[str, bool]]) -> Dict:
    """
    Определяет, к какому кластеру пользователь наиболее склонен.
    """
    cluster_scores = {}

    for cluster in CLUSTERS:
        cluster_vector = vectorize_cluster(cluster)
        score = calculate_cluster_score(preferences, cluster_vector)
        cluster_scores[cluster["название"]] = score

    # Находим кластер с максимальным счётом
    best_cluster_name = max(cluster_scores, key=cluster_scores.get)
    best_cluster = next(c for c in CLUSTERS if c["название"] == best_cluster_name)


    print("\n" + "=" * 50)
    print("РЕЗУЛЬТАТ:")
    print(f"Вы относитесь к кластеру: {best_cluster['название']}")
    print(f"Возрастная группа: {best_cluster['возраст']}")
    print(f"Ключевые интересы: {', '.join(best_cluster['интересы'][:3])}")
    print(f"Мотивация: {', '.join(best_cluster['мотивация'])}")
    print("="*50)

    return best_cluster

# --- ЗАПУСК ---

if __name__ == "__main__":
    # 1. Получаем предпочтения пользователя
    user_preferences = get_user_preferences()

    # 2. Определяем кластер
    result_cluster = determine_user_cluster(user_preferences)