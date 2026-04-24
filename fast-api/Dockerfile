FROM python:3.11-slim

# Устанавливаем минимальные зависимости для работы скриптов и скачивания браузеров
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем и устанавливаем зависимости Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Устанавливаем браузер и ВСЕ системные зависимости для него одной командой
RUN playwright install chromium
RUN playwright install-deps chromium

# Копируем остальной код
COPY . .
RUN mkdir -p data

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]