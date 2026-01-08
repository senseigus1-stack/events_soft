import sys
from .cluster_service import ClusterService
from .schemas import Cluster, Event_ML
import json
from typing import List, Optional, Tuple
from .config import Config
import logging

logger = logging.getLogger(__name__)

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

def get_status_vector(event: Event_ML, path: str) -> List[Tuple[str, float]]:
    """
    Получает релевантные кластеры для события
    
    :param event: Событие для анализа
    :param path: Путь к файлу с кластерами
    :return: Список кортежей (название кластера, степень релевантности)
    """
    try:
        clusters = load_clusters_from_file(path)
        
        if not clusters:
            logger.warning("Список кластеров пуст")
            return []
            
        cluster_service = ClusterService()
        cluster_service.load_clusters(clusters)
        
        relevant_clusters = cluster_service.get_relevant_clusters(event, clusters)
        
        if not relevant_clusters:
            logger.warning("Релевантные кластеры не найдены")
        
        logger.info(f"Найденные кластеры: {relevant_clusters}")
        return relevant_clusters
    
    except Exception as e:
        logger.error(f"Ошибка при получении статус-вектора: {e}")
        raise


