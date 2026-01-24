import os
from dataclasses import dataclass
from decouple import config


@dataclass
class Config:
    TELEGRAM_TOKEN: str = config("TELEGRAM_TOKEN").strip()
    DB_DSN: str = config("POSTGRES_URI").strip()
    REDIS_HOST: str = config("REDIS_HOST", default="localhost").strip()
    REDIS_PORT: int = config("REDIS_PORT", default=6379, cast=int)
    MODEL_NAME: str = config("MODEL_NAME", default="all-MiniLM-L6-v2").strip()
    BATCH_SIZE: int = config("BATCH_SIZE", default=16, cast=int)
    MAX_HISTORY: int = 50
    RNN_SEQ_LEN: int = 10
    RECOMMEND_COUNT: int = 5
    CACHE_TTL: int = config("CACHE_TTL", default=604800, cast=int)
    TOP_K: int = config("TOP_K", default=10, cast=int)
    SIMILARITY_THRESHOLD: float = config("SIMILARITY_THRESHOLD", default=0.4, cast=float)