
from .vectorizer import Vectorizer
from .cache import RedisCache
from .schemas import Cluster, Event_ML
from typing import List, Tuple
import numpy as np
import logging
from .config import Config

logger = logging.getLogger(__name__)


class ClusterService:

    def __init__(self):
        self.vectorizer = Vectorizer()
        self.cache = RedisCache()
        self.cluster_vectors = {}  # {название_кластера: vector}

    def load_clusters(self, clusters: List[Cluster]):
        """Vectorizes clusters once and caches them in Redis."""
        for cluster in clusters:
            try:
                text = " ".join(cluster.интересы + cluster.предпочтения + cluster.мотивация)
                vector = self.vectorizer.encode([text])[0]
                self.cluster_vectors[cluster.название] = vector
                self.cache.set_vector(f"cluster_vector:{cluster.название}", vector)
            except Exception as e:
                logger.error(f"Error vectorization clusters's {cluster.название}: {e}")

    def _get_cluster_vector(self, cluster_name: str) -> np.ndarray:
        cached = self.cache.get_vector(f"cluster_vector:{cluster_name}")
        if cached is not None:
            return cached
        if cluster_name in self.cluster_vectors:
            return self.cluster_vectors[cluster_name]
        raise ValueError(f"Cluster {cluster_name} didn't download")

    def _get_event_vector(self, event: dict) -> np.ndarray:
        """
        Gets the event vector from the cache or calculates it.
        :param event: a dictionary with event data (must contain 'id', 'title', 'description', 'tags')
        """
        event_id = event.get('id')
        if not event_id:
            raise ValueError("The event does not contain the required 'id' field")

        cached = self.cache.get_vector(f"event_vector:{event_id}")
        if cached is not None:
            return cached

        try:
            title = event.get('title', '')
            description = event.get('description', '')
            tags = event.get('tags', [])
            text = f"{title} {description} {' '.join(tags)}"
            vector = self.vectorizer.encode([text])[0]
            self.cache.set_vector(f"event_vector:{event_id}", vector)
            return vector
        except Exception as e:
            logger.error(f"Event vectorization error {event_id}: {e}")
            raise

    def get_relevant_clusters(
        self,
        event: dict,
        clusters: List[Cluster]
    ) -> List[Tuple[str, float]]:
        """
        Returns a list (cluster, similarity) sorted in descending order.
        If no clusters pass filters, returns the top-1 by similarity.
        """
        try:
            event_vec = self._get_event_vector(event)

            scores = []
            for cluster in clusters:
                try:
                    cluster_vec = self._get_cluster_vector(cluster.название)
                    sim = self.vectorizer.cosine_sim(event_vec, cluster_vec)
                    scores.append((cluster.название, sim))
                except Exception as e:
                    logger.warning(f"Skipping cluster {cluster.название} due to an error: {e}")

            # Фильтрация по возрасту
            age_restriction = event.get('age_restriction')
            if age_restriction:
                scores = [
                    (name, sim) for name, sim in scores
                    if not self._age_conflict(name, age_restriction, clusters)
                ]

            # Сортировка по сходству (убывание)
            sorted_scores = sorted(scores, key=lambda x: x[1], reverse=True)

            # Если после фильтрации остались кластеры — применяем порог и ограничиваем TOP_K
            if sorted_scores:
                filtered_scores = [
                    (name, sim) for name, sim in sorted_scores
                    if sim > Config.SIMILARITY_THRESHOLD
                ]
                # Если есть кластеры выше порога — возвращаем их (до TOP_K)
                if filtered_scores:
                    return filtered_scores[:Config.TOP_K]
                else:
                    # Если все кластеры ниже порога — возвращаем топ‑1 (даже если сходство низкое)
                    return [sorted_scores[0]]
            else:
                # Если все кластеры отфильтрованы по возрасту — возвращаем топ‑1 по сходству без учёта возраста
                unfiltered_sorted = sorted(scores, key=lambda x: x[1], reverse=True)
                return [unfiltered_sorted[0]] if unfiltered_sorted else []

        except Exception as e:
            event_id = event.get('id', 'unknown')
            logger.error(f"Error when selecting clusters for event_id={event_id}: {e}")
            return []
        

    def _age_conflict(
        self,
        cluster_name: str,
        age_restriction: str,
        clusters: List[Cluster]
    ) -> bool:
        """
        Checks if the cluster's age range conflicts with the event's age restriction.
        Returns True if there is a conflict (event is not suitable for the cluster).
        """
        if not age_restriction or not cluster_name:
            return False

        # Находим кластер по имени
        cluster = next(
            (c for c in clusters if c.название == cluster_name),
            None
        )
        if not cluster or not cluster.возраст:
            return False

        # Парсим ограничение возраста события (например, "18+")
        try:
            if age_restriction.endswith("+"):
                min_age_event = int(age_restriction[:-1])
            else:
                return False  # Если формат не "X+", считаем, что ограничений нет
        except ValueError:
            logger.warning(f"Invalid age_restriction format: {age_restriction}")
            return False

        # Парсим возрастной диапазон кластера (например, "30–50 лет (с детьми)")
        try:
            # Удаляем всё после цифр (включая пробелы и текст)
            cleaned = cluster.возраст.replace("лет", "")
            # Ищем числа через регулярное выражение
            import re
            numbers = re.findall(r'\d+', cleaned)
            if len(numbers) < 2:
                logger.warning(f"Not enough numbers in age range: {cluster.возраст}")
                return False
            min_age_cluster = int(numbers[0])
            max_age_cluster = int(numbers[1])
        except (ValueError, IndexError) as e:
            logger.error(f"Error parsing cluster age range {cluster.возраст}: {e}")
            return False

        # Конфликт: минимальный возраст события > максимального возраста кластера
        return min_age_event > max_age_cluster