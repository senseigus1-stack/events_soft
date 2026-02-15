import torch
import torch.nn as nn
import numpy as np
from sentence_transformers import SentenceTransformer
from redis import Redis
import json
from config import Config

import logging
from datetime import datetime

# Настраиваем логгер
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler("status_updates.log", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)

class RNNModel(nn.Module):
    def __init__(self, input_size, hidden_size=64, num_layers=2):
        super().__init__()
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_size, input_size)


    def forward(self, x):
        out, _ = self.lstm(x)
        return self.fc(out[:, -1, :])

class MLService:
    def __init__(self):
        self.model = SentenceTransformer(Config.MODEL_NAME)
        self.redis = Redis(host=Config.REDIS_HOST, port=Config.REDIS_PORT)
        self.rnn = RNNModel(input_size=384)
        self.optimizer = torch.optim.Adam(self.rnn.parameters(), lr=0.001)
        self.criterion = nn.MSELoss()


    def _cache_key(self, text: str) -> str:
        return f'vec:{hash(text) % 1000000}'

    def encode_text(self, text: str) -> np.ndarray:
        key = self._cache_key(text)
        cached = self.redis.get(key)
        if cached:
            return np.frombuffer(cached, dtype=np.float32)
        vector = self.model.encode([text], batch_size=Config.BATCH_SIZE)[0]
        self.redis.set(key, vector.tobytes())
        return vector

    def get_event_vector(self, event: dict) -> np.ndarray:
        text = f"{event['title']} {event['description']}"
        return self.encode_text(text)


    def train_rnn(self, user_history: list, events: list):
        vectors = []
        for item in user_history[-Config.RNN_SEQ_LEN:]:
            event = next((e for e in events if e["id"] == item["event_id"]), None)
            if event:
                vec = self.get_event_vector(event)
                weight = 1.0 if item["rating"] == "like" else -0.3
                vectors.append(vec * weight)

        if len(vectors) < 2:
            return
        X = np.array(vectors[:-1]).reshape(1, -1, 384)
        y = np.array(vectors[-1]).reshape(1, -1)
        X_tensor = torch.tensor(X, dtype=torch.float32)
        y_tensor = torch.tensor(y, dtype=torch.float32)
        self.optimizer.zero_grad()
        output = self.rnn(X_tensor)
        loss = self.criterion(output, y_tensor)
        loss.backward()
        self.optimizer.step()

    def recommend(self, user_history: list, candidates: list) -> list:
        if len(user_history) < Config.RNN_SEQ_LEN // 2:
            return self._recommend_by_status_ml(user_history, candidates)
        last_vecs = []
        for item in user_history[-Config.RNN_SEQ_LEN:]:
            event = next((e for e in candidates if e["id"] == item["event_id"]), None)
            if event:
                last_vecs.append(self.get_event_vector(event))
        if len(last_vecs) < 2:
            return self._recommend_by_status_ml(user_history, candidates)
        X = np.array(last_vecs[:-1]).reshape(1, -1, 384)
        X_tensor = torch.tensor(X, dtype=torch.float32)
        pred_vec = self.rnn(X_tensor).detach().numpy().flatten()
        scores = []
        for ev in candidates:
            ev_vec = self.get_event_vector(ev)
                # Косинусное сходство
            cos_sim = np.dot(pred_vec, ev_vec) / (np.linalg.norm(pred_vec) * np.linalg.norm(ev_vec))
            scores.append((ev, cos_sim))
            
            # Сортируем по убыванию сходства
        sorted_scores = sorted(scores, key=lambda x: x[1], reverse=True)
        return [item[0] for item in sorted_scores[:Config.RECOMMEND_COUNT]]


    def _recommend_by_status_ml(self, user_history: list, candidates: list) -> list:
        """Базовая рекомендация через сравнение status_ml"""
        if not user_history:
            return candidates[:Config.RECOMMEND_COUNT]
        
        # Собираем все кластеры из истории с весами
        user_clusters = {}
        for item in user_history:
            if item["rating"] == "like":
                event = next((e for e in candidates if e["id"] == item["event_id"]), None)
                if event:
                    for cluster in event["status_ml"]:
                        cat = cluster["category"]
                        score = cluster["score"]
                        if cat in user_clusters:
                            user_clusters[cat] += score * 0.3
                        else:
                            user_clusters[cat] = score * 0.3
        
        # Оцениваем кандидаты
        scores = []
        for ev in candidates:
            total_score = 0
            for cluster in ev["status_ml"]:
                if cluster["category"] in user_clusters:
                    total_score += cluster["score"] * user_clusters[cluster["category"]]
            scores.append((ev, total_score))
        
        sorted_scores = sorted(scores, key=lambda x: x[1], reverse=True)
        return [item[0] for item in sorted_scores[:Config.RECOMMEND_COUNT]]
    
    def update_user_status_ml(self, user_status: list, event_status: list, weight: float) -> list:
        # Логируем исходное состояние
        logger.info(
            "Обновление статуса пользователя. "
            f"Исходный статус: {user_status}, "
            f"Событие: {event_status}, вес: {weight}"
        )

        updated_status = user_status.copy()

        for event_cluster in event_status:
            event_cat = event_cluster["category"]
            event_score = event_cluster["score"]

            user_cluster = next(
                (c for c in updated_status if c["category"] == event_cat),
                None
            )

            if user_cluster:
                user_cluster["score"] += event_score * weight
                user_cluster["score"] = max(0.0, min(1.0, user_cluster["score"]))
            else:
                updated_status.append({
                    "category": event_cat,
                    "score": event_score * weight
                })

        # Логируем результат
        logger.info(
            f"Обновлённый статус: {updated_status}"
        )
        return updated_status