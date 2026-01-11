import numpy as np
import torch
import torch.nn as nn
from sklearn.preprocessing import LabelEncoder
import pandas as pd
from typing import List, Tuple, Dict
from collections import defaultdict
import json


# --- 1. Загрузка данных ---
# Теги из CSV (предполагаем, что столбец "tag")
tags_df = pd.read_csv('C:/Users/redmi/events_soft/user_ai/tags_unique.csv')
ALL_TAGS = sorted(tags_df['tag'].astype(str).tolist())  # Убедимся, что это строки


# Сегменты из JSON
with open('C:/Users/redmi/events_soft/ai/clusters.json', 'r', encoding='utf-8') as f:
    SEGMENTS = json.load(f)

# --- 2. Векторизация тегов ---
tag_encoder = LabelEncoder()
tag_encoder.fit(ALL_TAGS)

def vectorize_tags(tags: List[str], max_len: int = 20) -> torch.Tensor:
    num_tags = len(ALL_TAGS)
    seq = np.zeros((max_len, num_tags))
    
    for i, tag in enumerate(tags[:max_len]):
        if str(tag) in ALL_TAGS:  # Явное приведение к строке
            try:
                idx = tag_encoder.transform([str(tag)])[0]
                seq[i, idx] = 1.0
            except ValueError:  # Тега нет в encoder
                continue
    return torch.FloatTensor(seq).unsqueeze(0)  # (1, max_len, num_tags)

# --- 3. RNN-модель ---
class TagRNN(nn.Module):
    def __init__(self, input_size: int, hidden_size: int, num_layers: int, num_classes: int, dropout: float = 0.2):
        super(TagRNN, self).__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        
        self.rnn = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True, dropout=dropout)
        self.fc = nn.Linear(hidden_size, num_classes)
        self.dropout = nn.Dropout(dropout)
    
    def forward(self, x):
        h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        out, (hn, cn) = self.rnn(x, (h0, c0))
        out = self.dropout(out[:, -1, :])
        out = self.fc(out)
        return out

# --- 4. Класс UserTagger (добавлен!) ---
class UserTagger:
    def __init__(self, segments: List[Dict], all_tags: List[str]):
        self.segments = segments
        self.all_tags = set(all_tags)
        self.tag_to_segments = defaultdict(list)
        for segment in segments:
            for interest in segment["интересы"]:
                self.tag_to_segments[interest.lower()].append(segment)
            for preference in segment["предпочтения"]:
                self.tag_to_segments[preference.lower()].append(segment)
            for motivation in segment["мотивация"]:
                self.tag_to_segments[motivation.lower()].append(segment)

    def extract_tags(self, user_data: Dict) -> List[str]:
        tags = []
        for key, values in user_data.items():
            if isinstance(values, list):
                for value in values:
                    cleaned = str(value).strip().lower()
                    if cleaned in self.all_tags:
                        tags.append(cleaned)
        return tags

    def assign_segments(self, tags: List[str]) -> List[Tuple[str, int]]:
        segment_scores = defaultdict(int)
        for tag in tags:
            for segment in self.tag_to_segments.get(tag.lower(), []):
                segment_scores[segment["название"]] += 1
        return sorted(segment_scores.items(), key=lambda x: -x[1])

# Инициализация UserTagger
tagger = UserTagger(SEGMENTS, ALL_TAGS)

# --- 5. Подготовка данных ---
segment_names = [s["название"] for s in SEGMENTS]
segment_encoder = LabelEncoder()
segment_encoder.fit(segment_names)

def prepare_sample(user_data: Dict, device: torch.device) -> Tuple[torch.Tensor, torch.LongTensor]:
    tags = tagger.extract_tags(user_data)
    x = vectorize_tags(tags).to(device)
    
    segments = tagger.assign_segments(tags)
    if segments:
        top_segment = segments[0][0]
        try:
            y = torch.LongTensor([segment_encoder.transform([top_segment])[0]]).to(device)
        except ValueError:  # Сегмент не найден
            y = torch.LongTensor([-1]).to(device)
    else:
        y = torch.LongTensor([-1]).to(device)
    return x, y

# --- 6. Обучение ---
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

input_size = len(ALL_TAGS)
hidden_size = 64
num_layers = 2
num_classes = len(segment_names)
learning_rate = 0.001
epochs = 50

model = TagRNN(input_size, hidden_size, num_layers, num_classes).to(device)
criterion = nn.CrossEntropyLoss()
optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate)

# Пример тренировочных данных (замените на свои!)
train_data = [
    {"интересы": ["граффити", "каллиграфия"], "посещенные_мероприятия": ["фестиваль граффити"]},
    {"интересы": ["джаз и блюз", "свинг"], "посещенные_мероприятия": ["концерт в джаз‑клубе"]}
]

# Обучение
model.train()
for epoch in range(epochs):
    total_loss = 0
    for user_data in train_data:
        x, y = prepare_sample(user_data, device)
        if y.item() == -1:
            continue
        
        optimizer.zero_grad()
        outputs = model(x)
        loss = criterion(outputs, y)
        loss.backward()
        optimizer.step()
        total_loss += loss.item()
    
    print(f"!Epoch [{epoch+1}/{epochs}], Loss: {total_loss/len(train_data):.4f}")

def predict_segment(user_data: Dict, model: nn.Module, k: int = 3, device: torch.device = device) -> List[Tuple[str, float]]:
    x, _ = prepare_sample(user_data, device)
    model.eval()
    with torch.no_grad():
        outputs = model(x)
        probs = torch.softmax(outputs, dim=1)
        topk = torch.topk(probs, k)


    predicted = []
    for idx, prob in zip(topk.indices[0], topk.values[0]):
        idx = idx.item()
        if idx < len(segment_encoder.classes_):  # Защита от выхода за пределы
            segment_name = segment_encoder.inverse_transform([idx])[0]
            predicted.append((segment_name, prob.item()))
        else:
            # Если индекс не найден, пропускаем
            continue

    return predicted