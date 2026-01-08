from typing import List, Optional
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from .config import Config
from typing import List, Optional, Union
import logging

logger = logging.getLogger(__name__)

class Vectorizer:
    def __init__(self):
        try:
            self.model = SentenceTransformer(Config.MODEL_NAME)
            self.dimension = self.model.get_sentence_embedding_dimension()
        except Exception as e:
            raise RuntimeError(f"Не удалось загрузить модель {Config.MODEL_NAME}: {e}")

    def encode(
        self, 
        texts: List[str], 
        batch_size: Optional[int] = None, 
        show_progress_bar: bool = False
    ) -> np.ndarray:
        try:
            batch_size = batch_size or Config.BATCH_SIZE
            return self.model.encode(
                texts,
                batch_size=batch_size,
                show_progress_bar=show_progress_bar,
                convert_to_numpy=True
            )
        except Exception as e:       
            logger.error(f"Ошибка при векторизации текста: {e}")
            raise

    def cosine_sim(
        self, 
        vec1: np.ndarray, 
        vec2: np.ndarray
    ) -> float:
        try:
            if vec1.shape != vec2.shape:
                raise ValueError("Векторы должны быть одинаковой размерности")
            return cosine_similarity([vec1], [vec2])[0][0]
        except Exception as e:
            logger.error(f"Ошибка при вычислении косинусного сходства: {e}")
            raise

    def batch_similarity(
        self, 
        vectors1: np.ndarray, 
        vectors2: np.ndarray
    ) -> np.ndarray:
        """
        Массовое вычисление косинусного сходства для набора векторов
        """
        try:
            if vectors1.shape[1] != vectors2.shape[1]:
                raise ValueError("Векторы должны быть одинаковой размерности")
            return cosine_similarity(vectors1, vectors2)
        except Exception as e:
            logger.error(f"Ошибка при массовом вычислении сходства: {e}")
            raise

    def normalize(self, vector: np.ndarray) -> np.ndarray:
        """
        Нормализация вектора
        """
        return vector / np.linalg.norm(vector)

    def get_embedding_dimension(self) -> int:
        return self.dimension

    def validate_vectors(
        self, 
        vec1: np.ndarray, 
        vec2: np.ndarray
    ) -> bool:
        """
        Проверка совместимости векторов для сравнения
        """
        return vec1.shape == vec2.shape and vec1.ndim == vec2.ndim

# Дополнительные возможности для улучшения:

# 1. Поддержка разных метрик сходства
class SimilarityMetrics:
    @staticmethod
    def euclidean_distance(vec1: np.ndarray, vec2: np.ndarray) -> float:
        return np.linalg.norm(vec1 - vec2)

    @staticmethod
    def manhattan_distance(vec1: np.ndarray, vec2: np.ndarray) -> float:
        return np.sum(np.abs(vec1 - vec2))

# 2. Кэширование результатов векторизации
class VectorCache:
    def __init__(self):
        self.cache = {}

    def get(self, text: str) -> Optional[np.ndarray]:
        return self.cache.get(text)

    def set(self, text: str, vector: np.ndarray):
        self.cache[text] = vector

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import List

class AsyncVectorizer:
    def __init__(self, vectorizer: Vectorizer):
        self.vectorizer = vectorizer
        self.executor = ThreadPoolExecutor(max_workers=Config.MAX_WORKERS)

    async def async_encode(
        self, 
        texts: List[str], 
        batch_size: Optional[int] = None,
        show_progress_bar: bool = False
    ) -> np.ndarray:
        """
        Асинхронная векторизация текста
        """
        try:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                self.executor, 
                self.vectorizer.encode, 
                texts, 
                batch_size, 
                show_progress_bar
            )
        except Exception as e:
            logger.error(f"Ошибка при асинхронной векторизации: {e}")
            raise

    async def async_batch_encode(
        self, 
        text_batches: List[List[str]]
    ) -> List[np.ndarray]:
        """
        Массовая асинхронная векторизация
        """
        tasks = []
        for batch in text_batches:
            task = asyncio.create_task(
                self.async_encode(batch)
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Обработка ошибок
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Ошибка при обработке батча {i}: {result}")
                results[i] = None
        
        return results

    async def close(self):
        """
        Корректное завершение работы
        """
        self.executor.shutdown(wait=True)

# Дополнительные улучшения:

# 1. Ограничение на размер батча
def chunked(lst: List[str], chunk_size: int):
    """
    Разбивает список на чанки заданного размера
    """
    logger.debug(f"Разбиение списка длиной {len(lst)} на чанки размером {chunk_size}")
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

    

# 2. Управление ресурсами
class VectorizerManager:
    def __init__(self):
        self.vectorizer = Vectorizer()
        self.async_vectorizer = AsyncVectorizer(self.vectorizer)

    async def process_texts(
        self, 
        texts: List[str], 
        batch_size: int = Config.BATCH_SIZE
    ) -> np.ndarray:
        text_batches = list(chunked(texts, batch_size))
        results = await self.async_vectorizer.async_batch_encode(text_batches)
        return np.vstack([res for res in results if res is not None])

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.async_vectorizer.close()
