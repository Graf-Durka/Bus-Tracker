FROM python:3.11-slim

# Устанавливаем минимальные зависимости для работы скриптов и скачивания браузеров
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем ТОЛЬКО requirements.txt для установки зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Устанавливаем браузер и ВСЕ системные зависимости для него одной командой
RUN playwright install chromium
RUN playwright install-deps chromium

# Копирование кода (COPY . .) удалено, так как код будет прокидываться через volume

EXPOSE 8000

# Добавлен флаг --reload, чтобы uvicorn сам перезапускался при изменении кода
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]