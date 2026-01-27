FROM python:3.11.12-alpine

# Метаданные
LABEL maintainer="clegpscb@gmail.com"

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем требования и устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь код проекта
COPY . .

# Открываем порт (если нужен доступ извне)
EXPOSE 8443

# Запускаемый скрипт (можно переопределить в docker-compose)
CMD ["python", "bot/main.py"]
