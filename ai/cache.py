import redis
import json
import logging
from .config import Config
import numpy as np
from typing import Optional
from redis.exceptions import RedisError
from typing import List

# Настраиваем логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RedisCache:
    def __init__(self):
        try:
            self.client = redis.Redis(
                host=Config.REDIS_HOST,
                port=Config.REDIS_PORT,
                db=0,
                socket_connect_timeout=5,
                decode_responses=True  # Автоматическое декодирование ответов
            )
            self.client.ping()  # Проверка подключения
        except RedisError as e:
            logger.error(f"Не удалось подключиться к Redis: {e}")
            raise

    def set_vector(
        self, 
        key: str, 
        vector: np.ndarray, 
        ttl: Optional[int] = None
    ) -> bool:
        """
        Сохраняет вектор в кэш
        :param ttl: время жизни в секундах (по умолчанию из конфигурации)
        """
        try:
            ttl = ttl or Config.CACHE_TTL
            serialized_vector = json.dumps(vector.tolist())
            self.client.setex(key, ttl, serialized_vector)
            return True
        except (TypeError, redis.DataError) as e:
            logger.warning(f"Ошибка сериализации вектора для ключа {key}: {e}")
            return False
        except RedisError as e:
            logger.warning(f"Ошибка сохранения в кэш для ключа {key}: {e}")
            return False

    def get_vector(self, key: str) -> Optional[np.ndarray]:
        try:
            data = self.client.get(key)
            if data:
                try:
                    return np.array(json.loads(data))
                except (json.JSONDecodeError, ValueError) as e:
                    logger.warning(f"Ошибка десериализации вектора для ключа {key}: {e}")
            return None
        except RedisError as e:
            logger.warning(f"Ошибка получения данных из кэша для ключа {key}: {e}")
            return None

    def clear_event_cache(self, event_id: int) -> bool:
        try:
            key = f"event_vector:{event_id}"
            deleted = self.client.delete(key)
            return deleted > 0
        except RedisError as e:
            logger.warning(f"Ошибка очистки кэша для event_id={event_id}: {e}")
            return False

    def clear_all(self) -> bool:
        """
        Очищает весь кэш
        """
        try:
            self.client.flushdb()
            return True
        except RedisError as e:
            logger.error(f"Ошибка очистки всего кэша: {e}")
            return False

    def exists(self, key: str) -> bool:
        """
        Проверяет существование ключа в кэше
        """
        try:
            return self.client.exists(key) > 0
        except RedisError as e:
            logger.warning(f"Ошибка проверки существования ключа {key}: {e}")
            return False

    def get_multiple(self, keys: List[str]) -> List[Optional[np.ndarray]]:
        """
        Получает несколько векторов по списку ключей
        """
        try:
            results = self.client.mget(keys)
            vectors = []
            for data in results:
                if data:
                    try:
                        vectors.append(np.array(json.loads(data)))
                    except (json.JSONDecodeError, ValueError):
                        vectors.append(None)
                else:
                    vectors.append(None)
            return vectors
        except RedisError as e:
            logger.warning(f"Ошибка массового получения данных: {e}")
            return [None] * len(keys)

    def __del__(self):
        try:
            self.client.close()
        except Exception as e:
            logger.warning(f"Ошибка при закрытии соединения с Redis: {e}")