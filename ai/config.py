from decouple import config
import os

class Config:
    """CFG for AI tech, cache redis, vectors"""
    # Redis
    REDIS_HOST = config("REDIS_HOST", default="localhost")
    REDIS_PORT = config("REDIS_PORT", default=6379, cast=int)

    # PostgreSQL
    POSTGRES_URI = config("POSTGRES_URI")

    # Модель
    MODEL_NAME = config("MODEL_NAME", default="paraphrase-multilingual-MiniLM-L12-v2")

    # Кеширование
    CACHE_TTL = config("CACHE_TTL", default=604800, cast=int)  # 7 дней

    # Параметры обработки
    BATCH_SIZE = config("BATCH_SIZE", default=32, cast=int)
    TOP_K = config("TOP_K", default=10, cast=int)
    SIMILARITY_THRESHOLD = config("SIMILARITY_THRESHOLD", default=0.6, cast=float)