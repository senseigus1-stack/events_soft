import os
from dataclasses import dataclass
from typing import Optional
from decouple import config, Config, RepositoryEnv
import os

print("[DEBUG] Содержимое .env:")
if os.path.exists('.env'):
    with open('.env', 'r', encoding='utf-8') as f:
        for line in f:
            print(f"!  {line.rstrip()}")
else:
    print("  .env не найден!")

def get_env_source():
    """Определяет источник переменных окружения: .env или os.environ."""
    # Ищем .env в текущей директории и родительских
    current_dir = os.getcwd()
    env_path = os.path.join(current_dir, '.env')
    
    if os.path.isfile(env_path):
        print(f"[INFO] Используем .env: {env_path}")
        return Config(RepositoryEnv(env_path))
    else:
        print("[INFO] .env не найден. Используем переменные окружения (os.environ).")
        return config

# Определяем источник конфигурации
env = get_env_source()

@dataclass
class Config:
    """
    Конфигурационный класс для приложения.
    Загружает настройки из .env или переменных окружения.
    """

    # Обязательные параметры
    TELEGRAM_TOKEN: str = env("TELEGRAM_TOKEN", default="").strip()
    DB_DSN: str = env("POSTGRES_URI", default="").strip()
    ADMIN_IDS: int = env("ADMIN_IDS")
    # Опциональные параметры с дефолтами
    REDIS_HOST: str = env("REDIS_HOST", default="localhost").strip()
    REDIS_PORT: int = env("REDIS_PORT", default=6379, cast=int)
    СLUSTERS_PATH: str = env(
        "CLUSTERS_PATH", default="clusters.json"
    ).strip()
    MODEL_NAME: str = env("MODEL_NAME", default="all-MiniLM-L6-v2").strip()
    BATCH_SIZE: int = env("BATCH_SIZE", default=16, cast=int)

    MAX_HISTORY: int = 50
    RNN_SEQ_LEN: int = 15
    RECOMMEND_COUNT: int = 12

    CACHE_TTL: int = env("CACHE_TTL", default=604800, cast=int)
    TOP_K: int = env("TOP_K", default=10, cast=int)
    SIMILARITY_THRESHOLD: float = env("SIMILARITY_THRESHOLD", default=0.4, cast=float)

    # Параметры для вебхука
    WEBHOOK_HOST: Optional[str] = env("WEBHOOK_HOST", default=None)
    WEBHOOK_PORT: int = env("WEBHOOK_PORT", default=8443, cast=int)
    WEBHOOK_PATH: str = env("WEBHOOK_PATH", default="/webhook-telegram").strip()
    USE_HTTPS: bool = env("USE_HTTPS", default=False, cast=bool)
    CERT_PATH: Optional[str] = env("CERT_PATH", default=None)
    KEY_PATH: Optional[str] = env("KEY_PATH", default=None)


    def __post_init__(self):
        """Проверка обязательных полей после инициализации."""
        if not self.TELEGRAM_TOKEN:
            raise ValueError(
                "TELEGRAM_TOKEN не найден!\n"
                "1. Проверьте наличие .env в текущей директории.\n"
                "2. Убедитесь, что в .env есть строка: TELEGRAM_TOKEN=ваш_токен\n"
                "3. Или задайте переменную окружения: set TELEGRAM_TOKEN=ваш_токен"
            )
        if not self.DB_DSN:
            raise ValueError(
                "POSTGRES_URI не найден!\n"
                "1. Проверьте .env на наличие: POSTGRES_URI=ваш_uri\n"
                "2. Или задайте переменную окружения: set POSTGRES_URI=ваш_uri"
            )

        # Проверка WEBHOOK_HOST, если USE_HTTPS=True
        if self.USE_HTTPS:
            if not self.WEBHOOK_HOST:
                raise ValueError("WEBHOOK_HOST обязателен при USE_HTTPS=True.")
            if not self.CERT_PATH or not self.KEY_PATH:
                raise ValueError("CERT_PATH и KEY_PATH обязательны при USE_HTTPS=True.")


    @classmethod
    def from_env(cls) -> Config:
        """Создаёт экземпляр Config из переменных окружения или .env."""
        return cls()

# Глобальный экземпляр конфигурации
CONFIG = Config.from_env()