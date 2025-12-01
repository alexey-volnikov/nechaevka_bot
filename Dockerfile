FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Устанавливаем порт по умолчанию
ENV PORT=8000

# Открываем порт для Flask
EXPOSE 8000

CMD ["python", "app.py"]
