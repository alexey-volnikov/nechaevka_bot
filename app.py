import json  # Сериализация объектов в JSON для шаблона
import logging  # Настройка логирования событий приложения
import os  # Работа с переменными окружения
import sqlite3  # Работа с базой SQLite для логов
import threading  # Запуск фонового потока лонгпулла
from pathlib import Path  # Удобная работа с путями и иерархией директорий
from urllib.parse import urlparse  # Разбор URL для выбора имени файла
from dataclasses import dataclass, field  # Упрощенное объявление классов состояния
from datetime import datetime, timedelta  # Фиксация времени событий и диапазонов
from typing import Dict, List, Optional  # Подсказки типов для словарей и списков

from logging.handlers import RotatingFileHandler  # Обработчик логов с ротацией файлов

from dotenv import load_dotenv  # Загрузка переменных окружения из .env
from flask import Flask, jsonify, render_template, request, send_from_directory  # Веб-сервер, рендер, разбор запросов и отдача файлов
import requests  # Загрузка файлов вложений по URL
import vk_api  # Клиент VK API
from vk_api.bot_longpoll import VkBotEventType, VkBotLongPoll  # Лонгпулл сообщества для чтения событий

load_dotenv()  # Инициализируем загрузку переменных окружения при старте скрипта

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")  # Формат логов
logger = logging.getLogger(__name__)  # Получаем логгер для текущего модуля
service_event_logger = None  # Плейсхолдер для логгера сервисных событий в базе

SERVICE_STATUS_EXPLANATIONS = {  # Справочник кодов статусов с пояснениями
    200: "Успех: запрос обработан корректно",  # Человекочитаемое описание коду 200
    201: "Создано: добавлен новый ресурс",  # Пояснение для кода 201
    204: "Нет контента: тело ответа пустое",  # Пояснение для кода 204
    400: "Некорректный запрос: нужно поправить параметры",  # Описание для кода 400
    401: "Требуется авторизация или токен",  # Описание для кода 401
    403: "Доступ запрещён правами",  # Описание для кода 403
    404: "Не найдено: проверьте URL или ID",  # Описание для кода 404
    500: "Ошибка сервера: смотреть стек",  # Описание для кода 500
}  # Справочник кодов и русских пояснений для сервисных логов

def safe_int_env(value: Optional[str], fallback: int) -> int:  # Функция безопасного приведения переменных окружения к int
    try:  # Пробуем выполнить приведение типов
        return int(value) if value is not None else fallback  # Возвращаем число или запасное значение
    except Exception:  # Если приведение не удалось
        return fallback  # Возвращаем запасной вариант


DEFAULT_TIMELINE_MINUTES = safe_int_env(os.getenv("TIMELINE_DEFAULT_MINUTES"), 60)  # Диапазон минут по умолчанию для графика
ATTACHMENTS_ROOT = Path(os.getenv("ATTACHMENTS_DIR") or os.path.join(os.getcwd(), "data", "attachments")).resolve()  # Базовая папка для вложений, доступная через веб
ATTACHMENTS_ROOT.mkdir(parents=True, exist_ok=True)  # Создаем директорию вложений, если её нет


class ServiceContextFilter(logging.Filter):  # Фильтр для добавления обязательных полей
    """Гарантирует наличие полей статуса в каждой записи сервисного логгера."""

    def filter(self, record: logging.LogRecord) -> bool:  # Вызывается для каждой записи перед обработкой
        record.status_code = getattr(record, "status_code", 0)  # Подставляем код ответа по умолчанию
        record.status_description = getattr(record, "status_description", "Сервисное сообщение")  # Добавляем пояснение
        return True  # Запись пропускаем дальше


def build_service_logger() -> logging.Logger:  # Конструирует сервисный логгер с ротацией
    """Создаёт отдельный логгер для сервисных событий с ротацией файла."""

    service_logger = logging.getLogger("service_logger")  # Получаем именованный логгер для сервиса
    service_logger.setLevel(logging.INFO)  # Устанавливаем уровень INFO для сохранения ключевых событий
    service_logger.propagate = False  # Отключаем проброс в родительские логгеры
    if service_logger.handlers:  # Проверяем, есть ли уже обработчики (например, при перезагрузке Flask)
        return service_logger  # Возвращаем готовый логгер, чтобы не дублировать записи
    logs_dir = os.path.join(os.getcwd(), "data")  # Папка для хранения файла логов
    os.makedirs(logs_dir, exist_ok=True)  # Создаем директорию при необходимости
    log_path = os.path.join(logs_dir, "service.log")  # Путь к файлу сервисных логов
    handler = RotatingFileHandler(log_path, maxBytes=512000, backupCount=3, encoding="utf-8")  # Обработчик с ротацией
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(status_code)s (%(status_description)s): %(message)s")  # Формат с кодом и пояснением
    handler.setFormatter(formatter)  # Назначаем форматтер обработчику
    handler.addFilter(ServiceContextFilter())  # Добавляем фильтр для обязательных полей
    service_logger.addHandler(handler)  # Подключаем обработчик к логгеру
    return service_logger  # Возвращаем готовый логгер


def log_service_event(status_code: int, message: str) -> None:  # Упрощенный вызов для записи сервисных событий
    """Пишет важное сервисное событие с кодом и русским пояснением."""

    description = SERVICE_STATUS_EXPLANATIONS.get(status_code, "Сервисное сообщение")  # Находим пояснение по коду
    service_logger.info(message, extra={"status_code": status_code, "status_description": description})  # Логируем событие в файл
    if service_event_logger is not None:  # Проверяем, инициализирован ли логгер базы
        service_event_logger.log_event(status_code, description, message)  # Дублируем событие в базу с локальным временем


service_logger = build_service_logger()  # Создаем отдельный сервисный логгер
logging.getLogger("werkzeug").setLevel(logging.WARNING)  # Поднимаем уровень werkzeug, чтобы скрыть GET/200 шум


@dataclass
class BotState:
    """Состояние бота и накопленные метрики."""

    total_events: int = 0  # Количество всех событий
    new_messages: int = 0  # Количество входящих сообщений
    invites: int = 0  # Количество действий с участниками чата
    errors: int = 0  # Количество ошибок лонгпулла
    last_messages: List[Dict] = field(default_factory=list)  # История последних сообщений
    events_timeline: List[Dict] = field(default_factory=list)  # История точек для графика

    def mark_event(self, payload: Dict, event_kind: str, keep: int = 10) -> None:
        """Фиксируем событие, обновляем счетчики и истории."""

        self.total_events += 1  # Увеличиваем общий счетчик событий
        if event_kind == "message":  # Если пришло новое сообщение
            self.new_messages += 1  # Увеличиваем счетчик сообщений
            self.last_messages.append(payload)  # Сохраняем содержимое сообщения
            if len(self.last_messages) > keep:  # Проверяем длину истории сообщений
                self.last_messages.pop(0)  # Удаляем самое старое сообщение при переполнении
        elif event_kind == "invite":  # Если событие связано с участниками
            self.invites += 1  # Увеличиваем счетчик приглашений/удалений
        current_time = datetime.now().astimezone()  # Фиксируем локальное время с таймзоной
        timestamp = current_time.isoformat()  # Сохраняем ISO-строку с информацией о зоне
        self.events_timeline.append(  # Добавляем точку для графика
            {
                "time": timestamp,  # Время точки
                "events": self.total_events,  # Общее количество событий
                "messages": self.new_messages,  # Количество сообщений
                "invites": self.invites,  # Количество событий с участниками
            }
        )
        if len(self.events_timeline) > 50:  # Обрезаем историю графика до 50 точек
            self.events_timeline.pop(0)  # Удаляем самую старую точку


class EventLogger:
    """Простой логгер событий в SQLite."""

    def __init__(self, db_path: str):
        self.db_path = db_path  # Путь до файла базы
        db_dir = os.path.dirname(self.db_path)  # Вычисляем директорию файла базы
        if db_dir:  # Если путь включает директорию
            os.makedirs(db_dir, exist_ok=True)  # Создаем директорию при необходимости
        self._connection = sqlite3.connect(self.db_path, check_same_thread=False)  # Открываем соединение с разрешением мультипоточности
        self._connection.row_factory = sqlite3.Row  # Включаем доступ к полям по имени
        self._lock = threading.Lock()  # Создаем блокировку для потокобезопасных операций
        self._ensure_schema()  # Инициализируем таблицу при старте

    def _ensure_schema(self) -> None:
        with self._lock:  # Закрываем блокировку
            cursor = self._connection.cursor()  # Берем курсор
            schema_sql = """
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    peer_id INTEGER,
                    peer_title TEXT,
                    peer_avatar TEXT,
                    from_id INTEGER,
                    from_name TEXT,
                    from_avatar TEXT,
                    message_id INTEGER,
                    reply_to INTEGER,
                    reply_message_id INTEGER,
                    reply_message_text TEXT,
                    reply_message_attachments TEXT,
                    reply_message_from_id INTEGER,
                    reply_message_from_name TEXT,
                    reply_message_from_avatar TEXT,
                    is_bot INTEGER DEFAULT 0,
                    text TEXT,
                    attachments TEXT,
                    payload TEXT
                )
            """  # SQL-скрипт создания таблицы без комментариев внутри текста
            cursor.execute(schema_sql)  # Создаем таблицу при отсутствии
            cursor.execute("PRAGMA table_info(events)")  # Читаем описание колонок для миграции
            columns = {row[1] for row in cursor.fetchall()}  # Собираем имена колонок в множество
            if "is_bot" not in columns:  # Если колонки для флага бота нет
                cursor.execute("ALTER TABLE events ADD COLUMN is_bot INTEGER DEFAULT 0")  # Добавляем колонку миграцией
            if "peer_title" not in columns:  # Если нет колонки для названия чата
                cursor.execute("ALTER TABLE events ADD COLUMN peer_title TEXT")  # Добавляем поле для названия чата
            if "from_name" not in columns:  # Если нет колонки для имени автора
                cursor.execute("ALTER TABLE events ADD COLUMN from_name TEXT")  # Добавляем поле для имени отправителя
            if "peer_avatar" not in columns:  # Если нет колонки для аватара чата
                cursor.execute("ALTER TABLE events ADD COLUMN peer_avatar TEXT")  # Добавляем поле для аватара чата
            if "from_avatar" not in columns:  # Если нет колонки для аватара отправителя
                cursor.execute("ALTER TABLE events ADD COLUMN from_avatar TEXT")  # Добавляем поле для аватара отправителя
            if "reply_message_id" not in columns:  # Если нет колонки ID исходного сообщения
                cursor.execute("ALTER TABLE events ADD COLUMN reply_message_id INTEGER")  # Добавляем колонку для ID ответа
            if "reply_message_text" not in columns:  # Если нет колонки текста исходного сообщения
                cursor.execute("ALTER TABLE events ADD COLUMN reply_message_text TEXT")  # Добавляем колонку для текста ответа
            if "reply_message_attachments" not in columns:  # Если нет колонки вложений исходного сообщения
                cursor.execute("ALTER TABLE events ADD COLUMN reply_message_attachments TEXT")  # Добавляем колонку для вложений ответа
            if "reply_message_from_id" not in columns:  # Если нет колонки автора исходного сообщения
                cursor.execute("ALTER TABLE events ADD COLUMN reply_message_from_id INTEGER")  # Добавляем колонку ID автора исходного сообщения
            if "reply_message_from_name" not in columns:  # Если нет колонки имени автора исходного сообщения
                cursor.execute("ALTER TABLE events ADD COLUMN reply_message_from_name TEXT")  # Добавляем колонку имени автора исходного сообщения
            if "reply_message_from_avatar" not in columns:  # Если нет колонки аватара автора исходного сообщения
                cursor.execute("ALTER TABLE events ADD COLUMN reply_message_from_avatar TEXT")  # Добавляем колонку аватара автора исходного сообщения
            self._connection.commit()  # Сохраняем изменения
            cursor.execute(  # Запрашиваем строки с reply_message для нормализации
                "SELECT id, payload, reply_message_id, reply_message_text, reply_message_attachments, reply_message_from_id FROM events WHERE event_type = 'message'"
            )
            rows = cursor.fetchall()  # Читаем строки для миграции
            for row in rows:  # Перебираем строки с потенциальным ответом
                try:  # Пробуем распарсить payload
                    payload = json.loads(row["payload"] or "{}") if isinstance(row, sqlite3.Row) else {}  # Достаем payload в виде словаря
                except Exception:  # Если JSON некорректный
                    continue  # Пропускаем запись
                reply_block = payload.get("reply_message") if isinstance(payload, dict) else None  # Получаем вложенный блок ответа
                if not isinstance(reply_block, dict):  # Если ответа нет или формат неверный
                    continue  # Пропускаем запись
                has_id = row["reply_message_id"] is not None  # Проверяем, заполнен ли ID ответа
                has_text = row["reply_message_text"] is not None  # Проверяем, заполнен ли текст ответа
                has_attachments = row["reply_message_attachments"] is not None  # Проверяем, заполнены ли вложения ответа
                if has_id and has_text and has_attachments:  # Если все поля уже заполнены
                    continue  # Пропускаем миграцию для этой строки
                reply_id = reply_block.get("id")  # Получаем ID исходного сообщения
                reply_text = reply_block.get("text")  # Получаем текст исходного сообщения
                reply_attachments = reply_block.get("attachments", []) if isinstance(reply_block.get("attachments"), list) else []  # Получаем вложения исходного сообщения
                reply_from_id = reply_block.get("from_id")  # Получаем автора исходного сообщения
                reply_from_name = reply_block.get("from_name")  # Получаем имя автора исходного сообщения
                reply_from_avatar = reply_block.get("from_avatar")  # Получаем аватар автора исходного сообщения
                cursor.execute(  # Обновляем строку новыми полями ответа
                    """
                    UPDATE events
                    SET reply_message_id = ?, reply_message_text = ?, reply_message_attachments = ?, reply_message_from_id = ?, reply_message_from_name = ?, reply_message_from_avatar = ?
                    WHERE id = ?
                    """,
                    (
                        reply_id,  # ID исходного сообщения
                        reply_text,  # Текст исходного сообщения
                        json.dumps(reply_attachments, ensure_ascii=False),  # Вложения исходного сообщения в JSON
                        reply_from_id,  # Автор исходного сообщения
                        reply_from_name,  # Имя автора исходного сообщения
                        reply_from_avatar,  # Аватар автора исходного сообщения
                        row["id"],  # ID строки для обновления
                    ),
                )
            self._connection.commit()  # Фиксируем результаты миграции

    def describe_storage(self) -> Dict[str, object]:
        return {
            "path": self.db_path,  # Путь до файла базы
            "exists": os.path.exists(self.db_path),  # Флаг существования файла
            "size_bytes": os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0,  # Размер файла в байтах
        }  # Словарь с описанием хранилища

    def log_event(
        self,
        event_type: str,
        payload: Dict,
        peer_title: Optional[str] = None,
        from_name: Optional[str] = None,
        peer_avatar: Optional[str] = None,
        from_avatar: Optional[str] = None,
    ) -> None:
        created_at = datetime.now().astimezone().isoformat()  # Фиксируем локальное время вставки с таймзоной
        peer_id = payload.get("peer_id")  # Берем ID чата
        from_id = payload.get("from_id")  # Берем автора
        message_id = payload.get("id")  # Берем ID сообщения
        reply_block = payload.get("reply_message") if isinstance(payload.get("reply_message"), dict) else None  # Получаем блок исходного сообщения
        reply_to = reply_block.get("from_id") if isinstance(reply_block, dict) else None  # Берем ID адресата ответа для обратной совместимости
        reply_message_id = reply_block.get("id") if isinstance(reply_block, dict) else None  # Берем ID исходного сообщения
        reply_message_text = reply_block.get("text") if isinstance(reply_block, dict) else None  # Берем текст исходного сообщения
        reply_message_attachments = (
            reply_block.get("attachments", []) if isinstance(reply_block, dict) and isinstance(reply_block.get("attachments"), list) else []
        )  # Берем вложения исходного сообщения
        reply_message_from_id = reply_block.get("from_id") if isinstance(reply_block, dict) else None  # Берем автора исходного сообщения
        reply_message_from_name = reply_block.get("from_name") if isinstance(reply_block, dict) else None  # Берем имя автора исходного сообщения
        reply_message_from_avatar = reply_block.get("from_avatar") if isinstance(reply_block, dict) else None  # Берем аватар автора исходного сообщения
        text = payload.get("text")  # Берем текст
        attachments = payload.get("attachments", [])  # Берем вложения
        is_bot = 1 if isinstance(from_id, int) and from_id < 0 else 0  # Фиксируем, что автор — бот или сообщество
        with self._lock:  # Начинаем потокобезопасную запись
            cursor = self._connection.cursor()  # Получаем курсор
            cursor.execute(  # Выполняем вставку строки
                """
                INSERT INTO events (created_at, event_type, peer_id, peer_title, peer_avatar, from_id, from_name, from_avatar, message_id, reply_to, reply_message_id, reply_message_text, reply_message_attachments, reply_message_from_id, reply_message_from_name, reply_message_from_avatar, is_bot, text, attachments, payload)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    created_at,  # Время вставки
                    event_type,  # Тип события
                    peer_id,  # Чат
                    peer_title,  # Название чата
                    peer_avatar,  # Аватар чата
                    from_id,  # Автор
                    from_name,  # Имя автора
                    from_avatar,  # Аватар автора
                    message_id,  # ID сообщения
                    reply_to,  # Кому отвечали
                    reply_message_id,  # ID исходного сообщения
                    reply_message_text,  # Текст исходного сообщения
                    json.dumps(reply_message_attachments, ensure_ascii=False),  # Вложения исходного сообщения
                    reply_message_from_id,  # ID автора исходного сообщения
                    reply_message_from_name,  # Имя автора исходного сообщения
                    reply_message_from_avatar,  # Аватар автора исходного сообщения
                    is_bot,  # Флаг автора-бота
                    text,  # Текст
                    json.dumps(attachments, ensure_ascii=False),  # Сериализуем вложения
                    json.dumps(payload, ensure_ascii=False),  # Сохраняем сырой payload
                ),
            )
            self._connection.commit()  # Сохраняем изменения

    def clear_messages(self) -> None:
        with self._lock:  # Начинаем потокобезопасную операцию
            cursor = self._connection.cursor()  # Получаем курсор
            cursor.execute("DELETE FROM events")  # Удаляем все строки таблицы событий
            self._connection.commit()  # Фиксируем изменения после удаления

    def delete_message(self, record_id: int) -> bool:
        with self._lock:  # Начинаем потокобезопасную операцию
            cursor = self._connection.cursor()  # Получаем курсор
            cursor.execute(  # Выполняем удаление только для событий типа message по ID записи
                "DELETE FROM events WHERE id = ? AND event_type = ?",
                (int(record_id), "message"),
            )
            deleted = cursor.rowcount > 0  # Фиксируем, была ли удалена хотя бы одна строка
            self._connection.commit()  # Фиксируем изменения после удаления
        return deleted  # Возвращаем результат удаления

    def fetch_messages(self, peer_id: Optional[int] = None, limit: int = 50) -> List[Dict]:
        with self._lock:  # Начинаем безопасное чтение
            cursor = self._connection.cursor()  # Берем курсор
            if peer_id is None:  # Если фильтр не задан
                cursor.execute(  # Запрос без условия по чату
                    "SELECT * FROM events WHERE event_type = ? ORDER BY id DESC LIMIT ?", ("message", limit)
                )
            else:  # Если задан конкретный чат
                cursor.execute(  # Запрос с фильтром peer_id
                    "SELECT * FROM events WHERE event_type = ? AND peer_id = ? ORDER BY id DESC LIMIT ?",
                    ("message", int(peer_id), limit),
                )
            rows = cursor.fetchall()  # Читаем все строки
        return [dict(row) for row in rows]  # Преобразуем в словари

    def list_peers(self) -> List[Dict[str, object]]:
        with self._lock:  # Начинаем безопасное чтение
            cursor = self._connection.cursor()  # Берем курсор
            cursor.execute("SELECT DISTINCT peer_id, peer_title, peer_avatar FROM events WHERE peer_id IS NOT NULL ORDER BY peer_id")  # Запрос уникальных чатов с названиями и аватарами
            rows = cursor.fetchall()  # Читаем строки
        return [  # Возвращаем список словарей с ID и названием
            {"id": row["peer_id"], "title": row["peer_title"], "avatar": row["peer_avatar"]}  # Словарь с ID, названием и аватаром
            for row in rows  # Перебираем строки результата
            if row["peer_id"] is not None  # Фильтруем пустые значения
        ]

    def count_messages_by_peer(self) -> Dict[int, int]:
        with self._lock:  # Начинаем потокобезопасное чтение
            cursor = self._connection.cursor()  # Берем курсор для запроса
            cursor.execute(  # Выполняем агрегатный запрос по количеству сообщений
                "SELECT peer_id, COUNT(*) AS cnt FROM events WHERE event_type = ? AND peer_id IS NOT NULL GROUP BY peer_id",
                ("message",),
            )
            rows = cursor.fetchall()  # Читаем результаты
        return {int(row["peer_id"]): int(row["cnt"]) for row in rows if row["peer_id"] is not None}  # Возвращаем словарь peer_id->количество

    def count_messages(self, range_minutes: Optional[int] = None) -> int:
        now = datetime.now().astimezone()  # Берем текущее локальное время
        params: List[object] = ["message"]  # Готовим параметры запроса
        base_query = "SELECT COUNT(*) AS cnt FROM events WHERE event_type = ?"  # Базовый запрос подсчета сообщений
        if isinstance(range_minutes, int) and range_minutes > 0:  # Проверяем, задан ли диапазон минут
            since = (now - timedelta(minutes=range_minutes)).isoformat()  # Вычисляем начальную точку диапазона
            base_query += " AND created_at >= ?"  # Добавляем условие по времени
            params.append(since)  # Добавляем значение в параметры
        with self._lock:  # Начинаем потокобезопасное чтение
            cursor = self._connection.cursor()  # Получаем курсор для запроса
            cursor.execute(base_query, params)  # Выполняем запрос с параметрами
            row = cursor.fetchone()  # Читаем единственную строку результата
        return int(row["cnt"] if row else 0)  # Возвращаем количество или 0

    def fetch_timeline(self, range_minutes: int = 60, max_points: int = 120) -> List[Dict[str, object]]:
        safe_range = range_minutes if isinstance(range_minutes, int) and range_minutes > 0 else 60  # Нормализуем диапазон минут
        safe_points = max(1, max_points)  # Страхуем количество точек на графике
        bucket_minutes = max(1, (safe_range + safe_points - 1) // safe_points)  # Рассчитываем размер интервала в минутах
        now = datetime.now().astimezone()  # Берем текущее время с таймзоной
        since = (now - timedelta(minutes=safe_range)).isoformat()  # Вычисляем нижнюю границу по времени
        with self._lock:  # Начинаем потокобезопасное чтение
            cursor = self._connection.cursor()  # Получаем курсор
            cursor.execute(  # Запрашиваем все сообщения начиная с нижней границы
                "SELECT created_at FROM events WHERE event_type = ? AND created_at >= ? ORDER BY created_at",
                ("message", since),
            )
            rows = cursor.fetchall()  # Читаем все строки
        buckets: Dict[str, Dict[str, object]] = {}  # Готовим словарь для сгруппированных точек
        for row in rows:  # Перебираем строки результата
            created_raw = row.get("created_at") if isinstance(row, dict) else row[0] if row else None  # Достаем поле времени
            try:  # Пытаемся преобразовать в datetime
                created_at = datetime.fromisoformat(created_raw) if created_raw else None  # Парсим ISO-строку
            except Exception:  # Если формат некорректный
                continue  # Пропускаем запись
            if not created_at:  # Проверяем, что дата есть
                continue  # Пропускаем пустые
            bucket_start = created_at.replace(second=0, microsecond=0)  # Округляем до начала минуты
            offset = created_at.minute % bucket_minutes  # Вычисляем смещение внутри корзины
            if offset:  # Если есть смещение
                bucket_start -= timedelta(minutes=offset)  # Сдвигаем к началу корзины
            bucket_key = bucket_start.isoformat()  # Формируем ключ корзины
            bucket = buckets.setdefault(  # Получаем или создаем корзину
                bucket_key,
                {"time": bucket_key, "events": 0, "messages": 0, "invites": 0},  # Начальные значения
            )
            bucket["events"] += 1  # Увеличиваем количество событий
            bucket["messages"] += 1  # Увеличиваем количество сообщений
        return sorted(buckets.values(), key=lambda x: x["time"])  # Возвращаем точки, отсортированные по времени


class ServiceEventLogger:  # Логгер сервисных событий с отдельной таблицей
    """Хранит сервисные оповещения с типом и пояснением."""

    def __init__(self, db_path: str):
        self.db_path = db_path  # Путь до файла базы
        db_dir = os.path.dirname(self.db_path)  # Директория файла базы
        if db_dir:  # Если путь включает директорию
            os.makedirs(db_dir, exist_ok=True)  # Создаем директорию при необходимости
        self._connection = sqlite3.connect(self.db_path, check_same_thread=False)  # Открываем соединение с разрешением мультипоточности
        self._connection.row_factory = sqlite3.Row  # Включаем доступ к колонкам по имени
        self._lock = threading.Lock()  # Создаем блокировку для потокобезопасных операций
        self._ensure_schema()  # Создаем схему при инициализации

    def _ensure_schema(self) -> None:
        with self._lock:  # Начинаем защищенный доступ
            cursor = self._connection.cursor()  # Берем курсор
            cursor.execute(  # Создаем таблицу сервисных событий при отсутствии
                """
                CREATE TABLE IF NOT EXISTS service_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    status_code INTEGER NOT NULL,
                    description TEXT,
                    message TEXT
                )
                """
            )
            self._connection.commit()  # Сохраняем изменения схемы

    def _classify_event(self, status_code: int) -> str:
        if status_code >= 500:  # Ошибка сервера
            return "error"  # Возвращаем тип ошибки
        if status_code >= 400:  # Клиентское предупреждение
            return "warning"  # Возвращаем тип предупреждения
        return "info"  # По умолчанию информационный тип

    def log_event(self, status_code: int, description: str, message: str) -> None:
        created_at = datetime.now().astimezone().isoformat()  # Фиксируем локальное время с таймзоной
        event_type = self._classify_event(status_code)  # Определяем тип события по коду
        with self._lock:  # Начинаем защищенную запись
            cursor = self._connection.cursor()  # Берем курсор
            cursor.execute(  # Вставляем новую строку в таблицу
                """
                INSERT INTO service_events (created_at, event_type, status_code, description, message)
                VALUES (?, ?, ?, ?, ?)
                """,
                (created_at, event_type, status_code, description, message),
            )
            self._connection.commit()  # Сохраняем изменения

    def fetch_events(self, event_type: Optional[str] = None, limit: int = 50, offset: int = 0) -> List[Dict]:
        with self._lock:  # Начинаем защищенное чтение
            cursor = self._connection.cursor()  # Берем курсор
            base_query = "SELECT * FROM service_events"  # Базовый запрос
            params: List[object] = []  # Список параметров
            if event_type == "important":  # Если нужно вернуть важные события
                base_query += " WHERE event_type IN ('warning', 'error')"  # Добавляем фильтр по типу
            elif event_type:  # Если задан конкретный тип
                base_query += " WHERE event_type = ?"  # Добавляем условие
                params.append(event_type)  # Добавляем значение условия
            base_query += " ORDER BY id DESC LIMIT ? OFFSET ?"  # Добавляем сортировку и пагинацию
            params.extend([limit, offset])  # Добавляем лимит и смещение
            cursor.execute(base_query, params)  # Выполняем запрос
            rows = cursor.fetchall()  # Получаем результаты
            return [dict(row) for row in rows]  # Возвращаем список словарей

    def count_events(self, event_type: Optional[str] = None) -> int:
        with self._lock:  # Начинаем защищенный доступ
            cursor = self._connection.cursor()  # Берем курсор
            base_query = "SELECT COUNT(*) FROM service_events"  # Базовый запрос подсчета
            params: List[object] = []  # Параметры запроса
            if event_type == "important":  # Фильтр важных событий
                base_query += " WHERE event_type IN ('warning', 'error')"  # Ограничиваем типы
            elif event_type:  # Фильтр конкретного типа
                base_query += " WHERE event_type = ?"  # Добавляем условие
                params.append(event_type)  # Добавляем значение параметра
            cursor.execute(base_query, params)  # Выполняем запрос
            result = cursor.fetchone()  # Получаем строку с количеством
            return int(result[0]) if result else 0  # Возвращаем число

    def clear_events(self) -> None:
        with self._lock:  # Начинаем защищенную операцию
            cursor = self._connection.cursor()  # Берем курсор
            cursor.execute("DELETE FROM service_events")  # Удаляем все строки
            self._connection.commit()  # Сохраняем изменения


class BotMonitor:
    """Фоновый монитор лонгпулла без отправки сообщений."""

    def __init__(self, token: str, group_id: int, state: BotState, event_logger: EventLogger):
        self.state = state  # Запоминаем объект состояния для обновлений
        self.token = token  # Токен сообщества
        self.group_id = group_id  # ID сообщества
        self.session = vk_api.VkApi(token=self.token)  # Сессия VK API для запросов
        self._stop_event = threading.Event()  # Флаг корректной остановки потока
        self.event_logger = event_logger  # Объект записи логов
        self.user_cache: Dict[int, Dict[str, Optional[str]]] = {}  # Кэш профилей пользователей (имя и аватар)
        self.group_cache: Dict[int, Dict[str, Optional[str]]] = {}  # Кэш профилей сообществ (имя и аватар)
        self.peer_cache: Dict[int, Dict[str, Optional[str]]] = {}  # Кэш профилей чатов по peer_id
        self.attachments_dir = ATTACHMENTS_ROOT  # Используем общую директорию для вложений
        self.attachments_dir.mkdir(parents=True, exist_ok=True)  # Создаем директории для вложений при инициализации

    def _hydrate_message_details(self, message: Dict) -> Dict:  # Подгружает полную версию сообщения по ID через API
        hydrated = dict(message) if isinstance(message, dict) else {}  # Копируем исходное сообщение в рабочий словарь
        msg_id = hydrated.get("id")  # Извлекаем ID сообщения
        conv_id = hydrated.get("conversation_message_id")  # Извлекаем ID сообщения в переписке для бота
        peer_id = hydrated.get("peer_id")  # Получаем peer_id, чтобы можно было сделать запрос по переписке
        if not isinstance(msg_id, int):  # Проверяем, что ID корректный
            return hydrated  # Возвращаем исходное сообщение без изменений
        try:  # Пробуем запросить полные данные сообщения по глобальному ID
            response = self.session.method(
                "messages.getById",  # Имя метода VK API
                {"message_ids": msg_id, "group_id": self.group_id},  # Параметры запроса с указанием группы
            )  # Завершили вызов API
            items = response.get("items", []) if isinstance(response, dict) else []  # Получаем список сообщений из ответа
            if items:  # Проверяем, что данные пришли
                detailed = items[0] if isinstance(items[0], dict) else {}  # Берем первый элемент как детальный словарь
                for key in ("attachments", "copy_history", "reply_message"):  # Перебираем интересующие поля
                    if detailed.get(key) is not None:  # Если поле присутствует в детальном ответе
                        hydrated[key] = detailed.get(key)  # Обновляем сообщение данными из API
        except Exception as exc:  # Ловим любые ошибки запроса
            logger.debug("Не удалось догрузить полное сообщение %s: %s", msg_id, exc)  # Пишем отладочный лог при неудаче
        attachments_len = len(hydrated.get("attachments", []) or [])  # Считаем количество вложений после первой догрузки
        try:  # Пробуем запросить данные по conversation_message_id, если вложений подозрительно мало
            if isinstance(conv_id, int) and isinstance(peer_id, int) and attachments_len <= 1:  # Проверяем наличие данных и малое число вложений
                response = self.session.method(  # Делаем запрос по conversation_message_id
                    "messages.getByConversationMessageId",  # Имя метода для переписки
                    {
                        "peer_id": peer_id,  # Указываем чат
                        "conversation_message_ids": conv_id,  # Передаем ID сообщения в чате
                        "group_id": self.group_id,  # Добавляем ID группы для прав доступа
                        "extended": 1,  # Запрашиваем расширенный ответ для профилей
                    },  # Конец словаря параметров
                )  # Завершаем вызов API
                items = response.get("items", []) if isinstance(response, dict) else []  # Извлекаем список сообщений
                if items:  # Проверяем, что ответ не пустой
                    detailed = items[0] if isinstance(items[0], dict) else {}  # Берем первый элемент
                    for key in ("attachments", "copy_history", "reply_message"):  # Обновляем интересующие поля
                        if detailed.get(key) is not None:  # Если поле есть в ответе
                            hydrated[key] = detailed.get(key)  # Подменяем данные на расширенные
        except Exception as exc:  # Ловим ошибки второго запроса
            logger.debug(
                "Не удалось догрузить сообщение %s через conversation_message_id %s: %s", msg_id, conv_id, exc
            )  # Пишем отладочный лог
        return hydrated  # Возвращаем дополненное сообщение

    def _sanitize_filename(self, name: str, fallback: str) -> str:
        cleaned = "".join(ch for ch in name if ch.isalnum() or ch in ("-", "_", "."))  # Оставляем буквы, цифры и безопасные символы
        return cleaned or fallback  # Возвращаем очищенное имя или запасной вариант

    def _extract_photo_url(self, photo_block: Dict) -> Optional[str]:
        sizes = photo_block.get("sizes", []) if isinstance(photo_block, dict) else []  # Получаем список размеров фото
        if not sizes:  # Проверяем наличие размеров
            return None  # Возвращаем пустое значение, если нет размеров
        best_size = max(sizes, key=lambda item: item.get("width", 0) * item.get("height", 0))  # Выбираем самое большое изображение
        return best_size.get("url")  # Возвращаем URL выбранного размера

    def _extract_audio_url(self, audio_block: Dict) -> Optional[str]:
        if not isinstance(audio_block, dict):  # Проверяем формат блока аудио
            return None  # Возвращаем пустое значение при некорректном формате
        return audio_block.get("link_mp3") or audio_block.get("link_ogg")  # Возвращаем ссылку на MP3 или OGG

    def _extract_doc_url(self, doc_block: Dict) -> Optional[str]:
        if not isinstance(doc_block, dict):  # Проверяем формат блока документа
            return None  # Возвращаем пустое значение при ошибке
        return doc_block.get("url")  # Возвращаем прямую ссылку на документ

    def _resolve_video_url(self, video_block: Dict) -> Optional[str]:
        if not isinstance(video_block, dict):  # Проверяем формат блока видео
            return None  # Возвращаем пустое значение при ошибке
        owner_id = video_block.get("owner_id")  # Получаем owner_id видео
        video_id = video_block.get("id")  # Получаем id видео
        access_key = video_block.get("access_key")  # Получаем access_key видео
        if owner_id is None or video_id is None:  # Проверяем наличие обязательных полей
            return None  # Возвращаем пустое значение, если данных нет
        videos_param = f"{owner_id}_{video_id}" + (f"_{access_key}" if access_key else "")  # Формируем параметр videos для API
        try:  # Пробуем запросить VK API
            response = self.session.method("video.get", {"videos": videos_param})  # Запрашиваем детали видео
            items = response.get("items", []) if isinstance(response, dict) else []  # Получаем список видео из ответа
            if not items:  # Проверяем наличие данных
                return None  # Возвращаем пустое значение при пустом ответе
            files_block = items[0].get("files", {}) if isinstance(items[0], dict) else {}  # Получаем блок файлов видео
            if not isinstance(files_block, dict):  # Проверяем формат блока файлов
                return None  # Возвращаем пустое значение при ошибке
            candidates = [files_block.get(key) for key in sorted(files_block.keys()) if key.startswith("mp4") or key == "mp4"]  # Собираем ссылки mp4
            candidates = [url for url in candidates if isinstance(url, str)]  # Оставляем только строки URL
            return candidates[-1] if candidates else None  # Возвращаем самую последнюю (обычно наибольшее качество)
        except Exception as exc:  # Обрабатываем ошибки VK API
            logger.debug("Не удалось запросить файл видео: %s", exc)  # Пишем отладку при неудаче
            return None  # Возвращаем пустое значение

    def _pick_attachment_url(self, attachment: Dict) -> Optional[str]:
        if not isinstance(attachment, dict):  # Проверяем формат вложения
            return None  # Возвращаем пустое значение
        att_type = attachment.get("type")  # Получаем тип вложения
        content = attachment.get(att_type, {}) if isinstance(att_type, str) else {}  # Получаем вложенный блок по типу
        if att_type == "photo":  # Если вложение фото
            direct_url = self._extract_photo_url(content)  # Пытаемся взять ссылку из размеров
            return direct_url or content.get("url")  # Возвращаем найденный URL или запасной из поля url
        if att_type == "audio_message":  # Если аудиосообщение
            return self._extract_audio_url(content)  # Возвращаем ссылку на аудио
        if att_type == "doc":  # Если документ
            return self._extract_doc_url(content)  # Возвращаем ссылку на документ
        if isinstance(content, dict) and content.get("url"):  # Fallback на URL в корне для неизвестных типов
            return content.get("url")  # Возвращаем URL как есть
        if att_type == "video":  # Если видео
            return self._resolve_video_url(content)  # Пытаемся получить прямую ссылку видео
        return None  # Возвращаем пустое значение по умолчанию

    def _attachment_signature(self, attachment: Dict) -> Optional[str]:
        if not isinstance(attachment, dict):  # Проверяем, что вложение — словарь
            return None  # Возвращаем пустое значение при неверном формате
        att_type = attachment.get("type")  # Получаем тип вложения
        nested = attachment.get(att_type) if isinstance(att_type, str) else None  # Получаем вложенный блок по типу
        nested_obj = nested if isinstance(nested, dict) else {}  # Нормализуем вложенный блок к словарю
        owner_id = nested_obj.get("owner_id")  # Читаем owner_id при наличии
        item_id = nested_obj.get("id")  # Читаем id вложения при наличии
        access_key = nested_obj.get("access_key")  # Читаем access_key при наличии
        if owner_id is not None and item_id is not None:  # Если присутствуют идентификаторы VK
            return f"{att_type}:{owner_id}_{item_id}_{access_key or ''}"  # Формируем сигнатуру по типу и ID
        url = self._pick_attachment_url(attachment) or attachment.get("url")  # Пробуем взять ссылку вложения
        if url:  # Если ссылка найдена
            return f"{att_type or 'file'}:{url}"  # Формируем сигнатуру по типу и ссылке
        try:  # Пытаемся сформировать сигнатуру из JSON
            return json.dumps(attachment, sort_keys=True, ensure_ascii=False)  # Возвращаем сериализованную сигнатуру
        except Exception:  # Ловим ошибки сериализации
            return None  # Возвращаем пустое значение при ошибке

    def _deduplicate_attachments(self, attachments: List[Dict]) -> List[Dict]:
        unique: List[Dict] = []  # Готовим список уникальных вложений
        if not isinstance(attachments, list):  # Проверяем корректность формата
            return unique  # Возвращаем пустой список при ошибке
        seen: set = set()  # Множество сигнатур для фильтрации дублей
        for attachment in attachments:  # Перебираем все вложения
            if not isinstance(attachment, dict):  # Проверяем тип элемента
                continue  # Пропускаем некорректные элементы
            signature = self._attachment_signature(attachment)  # Вычисляем сигнатуру вложения
            if signature and signature in seen:  # Проверяем, есть ли уже такая сигнатура
                continue  # Пропускаем дубликат
            if signature:  # Если сигнатура рассчитана
                seen.add(signature)  # Добавляем её в множество
            unique.append(attachment)  # Кладем вложение в итоговый список
        return unique  # Возвращаем список без дублей

    def _build_local_path(self, peer_id: Optional[int], message_id: Optional[int], url: str, attachment_type: str) -> Path:
        parsed = urlparse(url)  # Парсим URL для выделения имени файла
        filename = Path(parsed.path).name  # Пытаемся взять имя файла из пути
        base_name = self._sanitize_filename(filename, f"file_{attachment_type}")  # Очищаем имя файла
        target_folder = self.attachments_dir / str(peer_id or "unknown_peer") / str(message_id or "unknown_message")  # Формируем вложенную директорию
        target_folder.mkdir(parents=True, exist_ok=True)  # Создаем вложенные директории
        return target_folder / base_name  # Возвращаем полный путь до файла

    def _download_file(self, url: str, target_path: Path) -> Optional[Path]:
        try:  # Пробуем скачать файл
            response = requests.get(url, timeout=30, stream=True)  # Выполняем HTTP-запрос с таймаутом
            response.raise_for_status()  # Бросаем исключение при ошибке статуса
            with target_path.open("wb") as file_handle:  # Открываем файл для записи
                for chunk in response.iter_content(chunk_size=8192):  # Читаем ответ блоками
                    if not chunk:  # Пропускаем пустые блоки
                        continue  # Переходим к следующему блоку
                    file_handle.write(chunk)  # Записываем блок в файл
            return target_path  # Возвращаем путь к сохраненному файлу
        except Exception as exc:  # Обрабатываем ошибки скачивания
            logger.warning("Не удалось сохранить вложение %s: %s", url, exc)  # Пишем предупреждение в лог
            return None  # Возвращаем пустое значение при ошибке

    def _normalize_attachment(self, attachment: Dict, peer_id: Optional[int], message_id: Optional[int]) -> Dict:
        normalized = dict(attachment) if isinstance(attachment, dict) else {}  # Копируем вложение, чтобы не трогать оригинал
        att_type = normalized.get("type")  # Получаем тип вложения
        download_url = self._pick_attachment_url(normalized)  # Пытаемся извлечь прямую ссылку
        normalized["local_path"] = None  # Подготавливаем поле для пути
        normalized["download_url"] = download_url  # Сохраняем URL в явном виде
        normalized["transcript"] = normalized.get("transcript")  # Резерв для будущей расшифровки аудио
        if download_url:  # Если удалось получить ссылку
            target_path = self._build_local_path(peer_id, message_id, download_url, att_type or "file")  # Формируем путь сохранения
            saved_path = self._download_file(download_url, target_path)  # Пытаемся скачать файл
            if saved_path:  # Проверяем успешность сохранения
                normalized["local_path"] = str(saved_path)  # Сохраняем путь к файлу
        return normalized  # Возвращаем нормализованное вложение

    def _save_attachments(self, attachments: List[Dict], peer_id: Optional[int], message_id: Optional[int]) -> List[Dict]:
        normalized_list: List[Dict] = []  # Готовим список нормализованных вложений
        if not isinstance(attachments, list):  # Проверяем формат входных данных
            return normalized_list  # Возвращаем пустой список при неверном формате
        unique_attachments = self._deduplicate_attachments(attachments)  # Удаляем дубли перед обработкой
        for attachment in unique_attachments:  # Перебираем уникальные вложения
            normalized_list.append(self._normalize_attachment(attachment, peer_id, message_id))  # Сохраняем каждое вложение
        return normalized_list  # Возвращаем список с локальными путями

    def _normalize_copy_history(self, copy_history: object, peer_id: Optional[int], parent_message_id: Optional[int]) -> List[Dict]:  # Нормализует список репостов и вложений
        normalized: List[Dict] = []  # Готовим список нормализованных репостов
        if not isinstance(copy_history, list):  # Проверяем формат входящих данных
            return normalized  # Возвращаем пустой список при ошибке формата
        for entry in copy_history:  # Перебираем каждый элемент copy_history
            if not isinstance(entry, dict):  # Проверяем тип элемента
                continue  # Пропускаем некорректные записи
            entry_copy = dict(entry)  # Копируем исходный словарь, чтобы не менять оригинал
            entry_copy["attachments"] = self._save_attachments(entry_copy.get("attachments", []), peer_id, entry_copy.get("id") or parent_message_id)  # Сохраняем вложения репоста
            nested_copy = entry_copy.get("copy_history")  # Получаем вложенный copy_history, если он есть
            entry_copy["copy_history"] = self._normalize_copy_history(nested_copy, peer_id, entry_copy.get("id") or parent_message_id) if nested_copy else []  # Рекурсивно нормализуем вложенные репосты
            from_id = entry_copy.get("from_id")  # Получаем автора репоста
            profile = self._resolve_sender_profile(from_id)  # Тянем имя и аватар автора
            entry_copy["from_name"] = profile.get("name")  # Добавляем имя автора
            entry_copy["from_avatar"] = profile.get("avatar")  # Добавляем аватар автора
            normalized.append(entry_copy)  # Кладем готовый репост в итоговый список
        return normalized  # Возвращаем нормализованный список репостов

    def start(self) -> None:
        listener_thread = threading.Thread(target=self._listen, daemon=True)  # Создаем фоновый поток
        listener_thread.start()  # Запускаем поток с лонгпуллом
        logger.info("Лонгпулл запущен в фоновом потоке")  # Пишем в лог успешный запуск

    def _listen(self) -> None:
        longpoll = VkBotLongPoll(self.session, self.group_id)  # Создаем слушателя событий сообщества
        while not self._stop_event.is_set():  # Цикл до получения сигнала остановки
            try:
                for event in longpoll.listen():  # Перебираем входящие события VK
                    if event.type == VkBotEventType.MESSAGE_NEW:  # Если это новое сообщение
                        message = event.object.message  # Извлекаем тело сообщения
                        message = self._hydrate_message_details(message)  # Догружаем полную версию сообщения через API
                        sender_profile = self._resolve_sender_profile(message.get("from_id"))  # Получаем имя и аватар отправителя
                        sender_name = sender_profile.get("name")  # Извлекаем имя из профиля
                        sender_avatar = sender_profile.get("avatar")  # Извлекаем аватар из профиля
                        peer_profile = self._resolve_peer_profile(message.get("peer_id"), sender_name)  # Получаем название и аватар чата
                        peer_title = peer_profile.get("title")  # Извлекаем название чата
                        peer_avatar = peer_profile.get("avatar")  # Извлекаем аватар чата
                        reply_message = message.get("reply_message") if isinstance(message.get("reply_message"), dict) else None  # Получаем исходное сообщение, если это ответ
                        reply_from_id = reply_message.get("from_id") if isinstance(reply_message, dict) else None  # Определяем автора исходного сообщения
                        reply_profile = self._resolve_sender_profile(reply_from_id) if reply_from_id else {"name": None, "avatar": None}  # Запрашиваем профиль автора исходного сообщения
                        if isinstance(reply_message, dict):  # Проверяем, что блок ответа корректный
                            reply_message = dict(reply_message)  # Копируем блок, чтобы не трогать оригинал VK
                            reply_message["from_name"] = reply_profile.get("name")  # Добавляем имя автора исходного сообщения
                            reply_message["from_avatar"] = reply_profile.get("avatar")  # Добавляем аватар автора исходного сообщения
                            message["reply_message"] = reply_message  # Обновляем исходный payload VK для дальнейшей записи
                        message["attachments"] = self._save_attachments(message.get("attachments", []), message.get("peer_id"), message.get("id"))  # Сохраняем вложения на диск и добавляем локальные пути
                        if isinstance(reply_message, dict):  # Проверяем, что есть вложения в исходном сообщении
                            reply_message["attachments"] = self._save_attachments(reply_message.get("attachments", []), message.get("peer_id"), reply_message.get("id"))  # Сохраняем вложения исходного сообщения
                        copy_history = self._normalize_copy_history(message.get("copy_history"), message.get("peer_id"), message.get("id"))  # Нормализуем репосты и вложения внутри них
                        if copy_history:  # Если репосты есть
                            message["copy_history"] = copy_history  # Сохраняем нормализованный список в payload
                        payload = {  # Собираем полезные данные для метрик
                            "id": message.get("id"),  # ID сообщения
                            "from_id": message.get("from_id"),  # ID отправителя
                            "from_name": sender_name,  # Имя отправителя
                            "from_avatar": sender_avatar,  # Аватар отправителя
                            "peer_id": message.get("peer_id"),  # Диалог или чат
                            "peer_title": peer_title,  # Название чата
                            "peer_avatar": peer_avatar,  # Аватар чата
                            "text": message.get("text"),  # Текст сообщения
                            "attachments": message.get("attachments", []),  # Список вложений
                            "copy_history": copy_history,  # Репосты с вложениями
                            "reply_message": reply_message,  # Ответ, если есть
                        }  # Конец сборки payload
                        self.state.mark_event(payload, "message")  # Фиксируем событие в состоянии
                        self.event_logger.log_event(
                            "message",  # Тип события
                            message,  # Сырой payload события
                            peer_title=peer_title,  # Название чата
                            from_name=sender_name,  # Имя отправителя
                            peer_avatar=peer_avatar,  # Аватар чата
                            from_avatar=sender_avatar,  # Аватар отправителя
                        )  # Записываем исходный payload с именами и аватарами в базу
                        logger.info(
                            "Сообщение: peer %s -> %s",  # Текст для лога
                            message.get("peer_id"),  # ID диалога
                            message.get("text"),  # Содержимое сообщения
                        )
                    elif event.type in (
                        VkBotEventType.CHAT_INVITE_USER,  # Приглашение пользователя
                        VkBotEventType.CHAT_KICK_USER,  # Удаление пользователя
                    ):
                        self.state.mark_event({}, "invite")  # Фиксируем событие участников
                        logger.info("Событие участников: %s", event.type)  # Пишем тип события в лог
                    else:  # Для всех остальных типов
                        self.state.mark_event({}, "other")  # Фиксируем как прочее
                        logger.info("Получено событие: %s", event.type)  # Логируем тип события
            except Exception as exc:  # Перехватываем ошибки в лонгпулле
                self.state.errors += 1  # Увеличиваем счетчик ошибок
                logger.exception("Ошибка лонгпулла: %s", exc)  # Пишем стек ошибки

    def _resolve_sender_profile(self, from_id: Optional[int]) -> Dict[str, Optional[str]]:
        if not isinstance(from_id, int):  # Если ID некорректный
            return {"name": None, "avatar": None}  # Возвращаем пустой профиль
        if from_id in self.user_cache:  # Проверяем кэш пользователей
            return self.user_cache[from_id]  # Отдаем сохраненный профиль пользователя
        if from_id in self.group_cache:  # Проверяем кэш сообществ
            return self.group_cache[from_id]  # Отдаем сохраненный профиль сообщества
        try:  # Пробуем выполнить запрос
            if from_id > 0:  # Если это пользователь
                response = self.session.method("users.get", {"user_ids": from_id, "fields": "photo_50"})  # Запрашиваем имя и аватар пользователя
                if response:  # Если ответ не пустой
                    user = response[0]  # Берем первую запись
                    name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()  # Формируем имя из имени и фамилии
                    avatar = user.get("photo_50")  # Берем маленький аватар
                    profile = {"name": name or None, "avatar": avatar}  # Собираем профиль пользователя
                    self.user_cache[from_id] = profile  # Кэшируем профиль пользователя
                    return profile  # Возвращаем профиль
            else:  # Если это сообщество
                response = self.session.method("groups.getById", {"group_id": abs(from_id), "fields": "photo_50"})  # Запрашиваем название и аватар сообщества
                if response:  # Если ответ есть
                    group = response[0]  # Берем первую запись
                    name = group.get("name")  # Достаем имя сообщества
                    avatar = group.get("photo_50")  # Достаем ссылку на аватар
                    profile = {"name": name or None, "avatar": avatar}  # Собираем профиль сообщества
                    self.group_cache[from_id] = profile  # Кэшируем профиль сообщества
                    return profile  # Возвращаем профиль
        except Exception as exc:  # Обрабатываем ошибки VK API
            logger.debug("Не удалось получить профиль отправителя %s: %s", from_id, exc)  # Пишем отладочный лог
        return {"name": None, "avatar": None}  # Возвращаем пустой профиль при неудаче

    def _extract_chat_photo(self, chat_settings: Dict) -> Optional[str]:
        if not isinstance(chat_settings, dict):  # Проверяем, что настройки переданы словарем
            return None  # Возвращаем пустое значение
        photo_block = chat_settings.get("photo", {}) if isinstance(chat_settings.get("photo"), dict) else {}  # Получаем блок фото из настроек беседы
        return photo_block.get("photo_50") or photo_block.get("photo_100")  # Возвращаем подходящий размер, если он есть

    def _resolve_peer_profile(self, peer_id: Optional[int], fallback: Optional[str]) -> Dict[str, Optional[str]]:
        if not isinstance(peer_id, int):  # Если peer_id не число
            return {"title": fallback, "avatar": None}  # Возвращаем запасной профиль
        if peer_id in self.peer_cache:  # Проверяем кэш чатов
            return self.peer_cache[peer_id]  # Возвращаем сохраненный профиль беседы
        try:  # Пробуем запросить данные чата
            if peer_id >= 2000000000:  # Если это беседа
                response = self.session.method("messages.getConversationsById", {"peer_ids": peer_id})  # Запрашиваем данные беседы
                items = response.get("items", []) if isinstance(response, dict) else []  # Получаем список бесед из ответа
                if items:  # Если список не пуст
                    chat_settings = items[0].get("chat_settings", {})  # Достаем настройки чата
                    title = chat_settings.get("title") or fallback  # Берем название беседы или запасной текст
                    avatar = self._extract_chat_photo(chat_settings)  # Пытаемся вытащить аватар беседы
                    profile = {"title": title, "avatar": avatar}  # Собираем профиль беседы
                    self.peer_cache[peer_id] = profile  # Кэшируем профиль беседы
                    return profile  # Возвращаем профиль
            elif peer_id > 0:  # Если это личный диалог с пользователем
                sender_profile = self._resolve_sender_profile(peer_id)  # Получаем профиль пользователя
                title = sender_profile.get("name") or fallback  # Берем имя пользователя или запасной текст
                avatar = sender_profile.get("avatar")  # Берем аватар пользователя
                profile = {"title": title, "avatar": avatar}  # Собираем профиль диалога
                self.peer_cache[peer_id] = profile  # Кэшируем профиль диалога
                return profile  # Возвращаем профиль
            else:  # Если peer_id отрицательный (сообщество)
                group_profile = self._resolve_sender_profile(peer_id)  # Получаем профиль сообщества
                title = group_profile.get("name") or fallback  # Берем название или запасной текст
                avatar = group_profile.get("avatar")  # Берем аватар сообщества
                profile = {"title": title, "avatar": avatar}  # Собираем профиль сообщества
                self.peer_cache[peer_id] = profile  # Кэшируем профиль сообщества
                return profile  # Возвращаем профиль
        except Exception as exc:  # Обрабатываем ошибки запроса
            logger.debug("Не удалось получить профиль чата %s: %s", peer_id, exc)  # Пишем отладку
        return {"title": fallback, "avatar": None}  # Возвращаем запасной профиль при ошибке

    def stop(self) -> None:
        self._stop_event.set()  # Устанавливаем флаг остановки потока


def resolve_db_path() -> str:
    base_dir = os.getenv("EVENT_DB_DIR") or os.path.join(os.getcwd(), "data")  # Определяем директорию для базы
    os.makedirs(base_dir, exist_ok=True)  # Создаем директорию хранения, если её нет
    return os.path.join(base_dir, os.getenv("EVENT_DB_NAME", "logs.db"))  # Собираем итоговый путь с именем файла


def load_settings() -> Dict[str, object]:
    demo_mode = os.getenv("DEMO_MODE", "0") == "1"  # Проверяем, включен ли демо-режим
    if demo_mode:  # Если демо включен
        logger.warning("Включен демо-режим без подключения к VK API")  # Предупреждаем пользователя
        return {"token": "demo", "group_id": 0, "demo_mode": True}  # Возвращаем параметры демо
    token = os.getenv("VK_GROUP_TOKEN", "")  # Получаем токен сообщества
    group_id = os.getenv("VK_GROUP_ID", "")  # Получаем ID сообщества
    if not token or not group_id:  # Если переменные не заданы
        raise RuntimeError("Укажите VK_GROUP_TOKEN и VK_GROUP_ID в .env или переменных окружения")  # Останавливаем запуск с подсказкой
    return {"token": token, "group_id": int(group_id), "demo_mode": False}  # Возвращаем настройки


def fetch_group_profile(session: vk_api.VkApi, group_id: int) -> Dict:
    info = session.method(
        "groups.getById",  # VK метод для информации о сообществе
        {"group_id": group_id, "fields": "description,contacts,members_count,photo_50"},  # Поля, которые запрашиваем, включая аватар
    )
    return info[0] if info else {}  # Возвращаем первый элемент или пустой словарь


def fetch_recent_conversations(session: vk_api.VkApi, limit: int = 10) -> List[Dict]:
    response = session.method(
        "messages.getConversations",  # VK метод для списка переписок
        {"count": limit, "filter": "all"},  # Запрашиваем несколько последних диалогов
    )
    return response.get("items", [])  # Возвращаем список объектов диалогов


def build_demo_payload(state: BotState, event_logger: EventLogger) -> Dict[str, object]:
    demo_messages = [  # Готовим список демонстрационных сообщений
        {
            "id": 1,  # ID сообщения
            "from_id": 111,  # Отправитель сообщения
            "from_name": "Иван Иванов",  # Имя отправителя
            "from_avatar": "https://placehold.co/96x96?text=IV",  # Демо-аватар отправителя
            "peer_id": 1,  # ID диалога
            "peer_title": "Демо-диалог",  # Название диалога
            "peer_avatar": "https://placehold.co/96x96?text=DM",  # Демо-аватар диалога
            "text": "Первое демо-сообщение",  # Текст демонстрационного сообщения
            "attachments": [],  # Список вложений
        },  # Сообщение 1
        {
            "id": 2,  # ID сообщения
            "from_id": 222,  # Отправитель сообщения
            "from_name": "Мария Петрова",  # Имя отправителя
            "from_avatar": "https://placehold.co/96x96?text=MP",  # Демо-аватар отправителя
            "peer_id": 2,  # ID чата
            "peer_title": "Демо-чат",  # Название чата
            "peer_avatar": "https://placehold.co/96x96?text=CH",  # Демо-аватар чата
            "text": "Еще одно демо",  # Текст демонстрационного сообщения
            "attachments": [
                {"type": "sticker", "sticker": {"product_id": 12345, "sticker_id": 67890}}  # Пример вложения стикера
            ],  # Список вложений
            "reply_message": {
                "id": 1,  # ID исходного сообщения
                "from_id": 111,  # Автор исходного сообщения
                "from_name": "Иван Иванов",  # Имя автора исходного сообщения
                "from_avatar": "https://placehold.co/96x96?text=IV",  # Аватар автора исходного сообщения
                "text": "Первое демо-сообщение",  # Текст исходного сообщения
                "attachments": [],  # Вложения исходного сообщения
            },  # Блок ответа на первое сообщение
        },  # Сообщение 2
    ]  # Конец списка демо-сообщений
    for message in demo_messages:  # Перебираем демо-сообщения
        state.mark_event(message, "message")  # Обновляем метрики для демо
        event_logger.log_event(  # Записываем демо в базу с именами и аватарами
            "message",  # Тип события
            message,  # Payload сообщения
            peer_title=message.get("peer_title"),  # Название чата
            from_name=message.get("from_name"),  # Имя автора
            peer_avatar=message.get("peer_avatar"),  # Аватар чата
            from_avatar=message.get("from_avatar"),  # Аватар автора
        )
    state.mark_event({}, "invite")  # Добавляем демо-событие приглашения
    group_info = {
        "name": "Демо-сообщество",  # Название сообщества
        "description": "Образец данных без подключения к VK",  # Описание сообщества
        "members_count": 1234,  # Число участников
        "screen_name": "club_demo",  # Короткий адрес
        "photo_50": "https://placehold.co/96x96?text=VK",  # Демо-аватар сообщества
    }  # Словарь с демонстрационным профилем
    conversations = [
        {"conversation": {"peer": {"id": 1, "type": "chat"}, "chat_settings": {"title": "Демо-чат"}}},  # Демо-чат
        {"conversation": {"peer": {"id": 2, "type": "user"}, "can_write": True}},  # Демо-диалог
    ]  # Список демонстрационных диалогов
    return {"group_info": group_info, "conversations": conversations}  # Возвращаем набор демо-данных


def build_dashboard_app(
    state: BotState,
    group_info: Dict,
    conversations: List[Dict],
    demo_mode: bool,
    event_logger: EventLogger,
    service_events: ServiceEventLogger,
) -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")  # Создаем Flask-приложение

    def detect_peer_type(peer_id: Optional[int]) -> str:
        return "chat" if isinstance(peer_id, int) and peer_id >= 2000000000 else "user" if isinstance(peer_id, int) and peer_id > 0 else "group" if isinstance(peer_id, int) and peer_id < 0 else "unknown"  # Определяем тип чата по peer_id

    def merge_conversations(seed_conversations: List[Dict], peer_rows: List[Dict]) -> List[Dict]:
        combined: Dict[int, Dict] = {}  # Словарь для объединения по peer_id
        for conv in seed_conversations or []:  # Перебираем исходные диалоги
            conversation_body = conv.get("conversation", conv) if isinstance(conv, dict) else {}  # Извлекаем тело диалога
            peer = conversation_body.get("peer", {}) if isinstance(conversation_body, dict) else {}  # Достаем блок peer
            peer_id = peer.get("id")  # Получаем ID чата
            if peer_id is None:  # Если ID нет
                continue  # Пропускаем запись
            combined[peer_id] = conversation_body  # Сохраняем тело диалога без обертки
        for peer_row in peer_rows or []:  # Перебираем чаты из базы
            peer_id = peer_row.get("id")  # Получаем ID чата
            if peer_id is None:  # Если ID отсутствует
                continue  # Пропускаем
            entry = combined.get(peer_id, {"peer": {"id": peer_id}})  # Берем существующий или создаем новый объект
            entry_peer = entry.setdefault("peer", {"id": peer_id})  # Обеспечиваем наличие блока peer
            entry_peer.setdefault("id", peer_id)  # Дублируем ID, если не было
            entry_peer.setdefault("type", detect_peer_type(peer_id))  # Устанавливаем тип чата
            if peer_row.get("avatar"):  # Если в базе есть аватар чата
                entry_peer.setdefault("avatar", peer_row.get("avatar"))  # Сохраняем аватар в блоке peer
            if peer_row.get("title"):  # Если известно название
                chat_settings = entry.setdefault("chat_settings", {})  # Берем блок настроек беседы
                chat_settings.setdefault("title", peer_row.get("title"))  # Устанавливаем название, не затирая существующее
            if peer_row.get("avatar") and isinstance(entry, dict):  # Если есть аватар из базы
                chat_settings = entry.setdefault("chat_settings", {})  # Берем блок настроек беседы
                photo_block = chat_settings.setdefault("photo", {}) if isinstance(chat_settings, dict) else {}  # Готовим блок фото
                photo_block.setdefault("photo_50", peer_row.get("avatar"))  # Сохраняем ссылку на аватар беседы
            combined[peer_id] = entry  # Обновляем словарь
        return list(combined.values())  # Возвращаем объединенный список

    def assemble_conversations() -> List[Dict]:
        peers_from_logs = event_logger.list_peers()  # Получаем чаты из базы
        messages_counts = event_logger.count_messages_by_peer()  # Получаем количество сообщений по каждому peer_id
        merged = merge_conversations(conversations, peers_from_logs)  # Объединяем стартовые диалоги с теми, что накопились в логах
        for conv in merged:  # Перебираем объединенные диалоги
            peer = conv.get("peer", {}) if isinstance(conv, dict) else {}  # Достаем блок peer из диалога
            peer_id = peer.get("id")  # Определяем peer_id текущего диалога
            conv["messages_count"] = messages_counts.get(peer_id, 0)  # Добавляем поле с количеством сообщений
        return merged  # Возвращаем список диалогов с подсчитанными сообщениями

    def resolve_range_minutes(raw_value: Optional[str]) -> int:
        try:  # Пытаемся привести значение к числу
            parsed = int(raw_value) if raw_value is not None else DEFAULT_TIMELINE_MINUTES  # Преобразуем строку или берем дефолт
        except Exception:  # Если приведение не удалось
            return DEFAULT_TIMELINE_MINUTES  # Возвращаем значение по умолчанию
        return parsed if parsed > 0 else DEFAULT_TIMELINE_MINUTES  # Возвращаем только положительные значения

    def assemble_stats(range_minutes: Optional[int] = None) -> Dict[str, object]:
        selected_range = range_minutes if isinstance(range_minutes, int) and range_minutes > 0 else DEFAULT_TIMELINE_MINUTES  # Нормализуем выбранный диапазон
        messages_count = event_logger.count_messages(selected_range)  # Считаем сообщения за выбранный диапазон
        last_messages = [decorate_message_preview(msg) for msg in state.last_messages]  # Нормализуем вложения последних сообщений
        return {  # Собираем словарь статистики
            "events": messages_count,  # Количество событий за диапазон берем из количества сообщений
            "messages": messages_count,  # Количество сообщений за диапазон
            "invites": state.invites,  # Количество приглашений/удалений за текущую сессию
            "errors": state.errors,  # Количество ошибок лонгпулла за текущую сессию
            "last_messages": last_messages,  # История последних сообщений из оперативной памяти с кликабельными вложениями
            "timeline": event_logger.fetch_timeline(selected_range),  # Точки графика из базы по диапазону
            "range_minutes": selected_range,  # Возвращаем выбранный диапазон минут
        }

    def assemble_storage() -> Dict[str, object]:
        return event_logger.describe_storage()  # Возвращаем информацию о файле базы

    def localize_iso(timestamp: Optional[str]) -> Optional[str]:
        try:  # Пытаемся преобразовать ISO-строку
            parsed = datetime.fromisoformat(timestamp) if timestamp else None  # Парсим дату с таймзоной
            return parsed.astimezone().isoformat() if parsed else None  # Конвертируем в локальное время и возвращаем ISO
        except Exception:  # Обрабатываем неверный формат строки
            return None  # Возвращаем None при ошибке

    def build_public_attachment_url(local_path: Optional[str]) -> Optional[str]:  # Строит публичную ссылку на локальный файл вложения
        try:  # Пытаемся собрать публичную ссылку на вложение
            if not local_path:  # Проверяем, передан ли путь
                return None  # Возвращаем пустое значение, если пути нет
            path_obj = Path(local_path).resolve()  # Нормализуем путь до файла
            if not str(path_obj).startswith(str(ATTACHMENTS_ROOT)):  # Проверяем, что файл лежит внутри корневой папки вложений
                return None  # Не отдаём файлы вне разрешенной директории
            relative = path_obj.relative_to(ATTACHMENTS_ROOT)  # Получаем относительный путь внутри папки вложений
            return f"/attachments/{relative.as_posix()}"  # Формируем URL для раздачи через Flask
        except Exception:  # Ловим любые ошибки работы с путями
            return None  # Возвращаем пустое значение при проблеме

    def enrich_attachments_list(attachments: object) -> List[Dict]:  # Добавляет публичные ссылки и нормализует вложения
        enriched: List[Dict] = []  # Готовим список нормализованных вложений
        if not isinstance(attachments, list):  # Проверяем, что входной объект — список
            return enriched  # Возвращаем пустой список при некорректном формате
        for raw in attachments:  # Перебираем все вложения
            if not isinstance(raw, dict):  # Проверяем тип элемента
                continue  # Пропускаем элементы неправильного формата
            item = dict(raw)  # Делаем копию вложения
            local_path = item.get("local_path")  # Читаем локальный путь
            public_url = build_public_attachment_url(local_path)  # Пробуем собрать публичную ссылку
            if public_url:  # Если ссылка собралась
                item["public_url"] = public_url  # Добавляем публичную ссылку для фронтенда
            else:  # Если публичная ссылка не собралась
                item["public_url"] = item.get("download_url") or item.get("url")  # Оставляем исходную ссылку как fallback
            resolved_url = item.get("public_url") or item.get("download_url") or item.get("url")  # Берем итоговую ссылку для совместимости
            if resolved_url:  # Проверяем, что ссылка определена
                item["url"] = resolved_url  # Явно сохраняем итоговую ссылку в поле url, чтобы фронт не терял вложения
            enriched.append(item)  # Добавляем нормализованное вложение в список
        return enriched  # Возвращаем итоговый список

    def count_attachments(attachments: object) -> int:  # Подсчитывает количество вложений в списке
        if not isinstance(attachments, list):  # Проверяем корректность формата входных данных
            return 0  # Возвращаем 0, если данные некорректны
        return sum(1 for att in attachments if isinstance(att, dict))  # Считаем только словари вложений

    def count_copy_history_attachments(entries: object) -> int:  # Рекурсивно считает вложения в репостах
        if not isinstance(entries, list):  # Проверяем формат copy_history
            return 0  # Возвращаем 0 при ошибке формата
        total = 0  # Инициализируем счетчик вложений
        for entry in entries:  # Перебираем каждый репост
            if not isinstance(entry, dict):  # Проверяем тип записи
                continue  # Пропускаем некорректные элементы
            total += count_attachments(entry.get("attachments"))  # Добавляем вложения самого репоста
            nested = entry.get("copy_history")  # Получаем вложенный copy_history
            total += count_copy_history_attachments(nested) if nested else 0  # Добавляем вложения вложенных репостов
        return total  # Возвращаем итоговое количество вложений

    def serialize_copy_history(entries: object) -> List[Dict]:  # Рекурсивно нормализует репосты и их вложения
        prepared: List[Dict] = []  # Готовим список репостов
        if not isinstance(entries, list):  # Проверяем формат входных данных
            return prepared  # Возвращаем пустой список при ошибке
        for entry in entries:  # Перебираем репосты
            if not isinstance(entry, dict):  # Проверяем тип элемента
                continue  # Пропускаем некорректные записи
            serialized = dict(entry)  # Копируем словарь репоста
            serialized["attachments"] = enrich_attachments_list(entry.get("attachments", []))  # Нормализуем вложения репоста
            serialized["copy_history"] = serialize_copy_history(entry.get("copy_history")) if entry.get("copy_history") else []  # Рекурсивно обрабатываем вложенные репосты
            prepared.append(serialized)  # Добавляем репост в итоговый список
        return prepared  # Возвращаем сериализованные репосты

    def decorate_message_preview(message: Dict) -> Dict:  # Добавляет публичные ссылки во вложения последних сообщений
        if not isinstance(message, dict):  # Проверяем формат сообщения
            return {}  # Возвращаем пустой словарь при ошибке
        prepared = dict(message)  # Копируем сообщение, чтобы не менять оригинал
        prepared["attachments"] = enrich_attachments_list(message.get("attachments", []))  # Нормализуем вложения сообщения
        prepared["copy_history"] = serialize_copy_history(message.get("copy_history")) if message.get("copy_history") else []  # Нормализуем репосты
        reply_block = message.get("reply") or message.get("reply_message")  # Получаем блок ответа
        if isinstance(reply_block, dict):  # Проверяем наличие ответа
            reply_copy = dict(reply_block)  # Копируем блок
            reply_copy["attachments"] = enrich_attachments_list(reply_block.get("attachments", []))  # Нормализуем вложения ответа
            prepared["reply"] = reply_copy  # Подменяем блок ответа нормализованной копией
        return prepared  # Возвращаем подготовленное сообщение

    def serialize_service_event(row: Dict) -> Dict[str, object]:
        return {
            "id": row.get("id"),  # ID строки
            "created_at": localize_iso(row.get("created_at")),  # Локальное время создания в ISO-формате
            "event_type": row.get("event_type"),  # Тип события (info/warning/error)
            "status_code": row.get("status_code"),  # Код статуса
            "description": row.get("description"),  # Русское пояснение
            "message": row.get("message"),  # Текстовое сообщение
        }  # Словарь с сервисным событием

    def serialize_log(row: Dict) -> Dict:
        raw_payload = json.loads(row.get("payload") or "{}")  # Сериализуем исходный payload
        reply_payload = raw_payload.get("reply_message") if isinstance(raw_payload, dict) else None  # Получаем блок ответа из payload
        reply = {  # Готовим словарь ответа
            "id": row.get("reply_message_id"),  # ID исходного сообщения
            "text": row.get("reply_message_text"),  # Текст исходного сообщения
            "attachments": enrich_attachments_list(json.loads(row.get("reply_message_attachments") or "[]")),  # Вложения исходного сообщения с публичными ссылками
            "from_id": row.get("reply_message_from_id"),  # Автор исходного сообщения
            "from_name": row.get("reply_message_from_name"),  # Имя автора исходного сообщения
            "from_avatar": row.get("reply_message_from_avatar"),  # Аватар автора исходного сообщения
        }  # Конец словаря ответа
        if isinstance(reply_payload, dict) and not (reply["id"] or reply["text"] or reply["from_id"]):  # Проверяем, нужно ли дополнить данными из payload
            reply["id"] = reply_payload.get("id")  # Подставляем ID исходного сообщения из payload
            reply["text"] = reply_payload.get("text")  # Подставляем текст исходного сообщения
            reply["attachments"] = enrich_attachments_list(reply_payload.get("attachments", []) if isinstance(reply_payload.get("attachments"), list) else [])  # Подставляем вложения исходного сообщения
            reply["from_id"] = reply_payload.get("from_id")  # Подставляем автора исходного сообщения
            reply["from_name"] = reply_payload.get("from_name")  # Подставляем имя автора исходного сообщения
            reply["from_avatar"] = reply_payload.get("from_avatar")  # Подставляем аватар автора исходного сообщения
        attachments = enrich_attachments_list(json.loads(row.get("attachments") or "[]"))  # Подготавливаем вложения с публичными ссылками

        copy_history = serialize_copy_history(raw_payload.get("copy_history")) if isinstance(raw_payload, dict) else []  # Сериализуем репосты и вложения
        return {  # Формируем итоговый словарь лога
            "id": row.get("id"),  # ID записи
            "created_at": localize_iso(row.get("created_at")),  # Локальное время создания в ISO-формате
            "event_type": row.get("event_type"),  # Тип события
            "peer_id": row.get("peer_id"),  # ID чата
            "peer_title": row.get("peer_title"),  # Название чата
            "peer_avatar": row.get("peer_avatar"),  # Аватар чата
            "from_id": row.get("from_id"),  # Автор
            "from_name": row.get("from_name"),  # Имя автора
            "from_avatar": row.get("from_avatar"),  # Аватар автора
            "message_id": row.get("message_id"),  # ID сообщения VK
            "reply": reply,  # Структурированный блок ответа
            "is_bot": row.get("is_bot", 0),  # Флаг, что автор — бот или сообщество
            "text": row.get("text"),  # Текст
            "attachments": attachments,  # Вложения с публичными ссылками
            "copy_history": copy_history,  # Репосты с вложениями
            "attachments_total": len(attachments) + count_copy_history_attachments(copy_history),  # Общее количество вложений в сообщении и репостах
            "payload": raw_payload,  # Сырой payload
        }  # Конец словаря лога

    @app.route("/")
    def index():
        log_service_event(200, "Отдаём главную страницу дашборда")  # Фиксируем успешную отдачу главной страницы
        return render_template(
            "index.html",  # Шаблон дашборда
            initial_group=group_info,  # Передаем словарь с данными сообщества без лишней сериализации
            initial_conversations=assemble_conversations(),  # Список диалогов с учетом базы
            initial_stats=assemble_stats(DEFAULT_TIMELINE_MINUTES),  # Начальные метрики состояния по умолчанию
            initial_peers=event_logger.list_peers(),  # Доступные peer_id из базы
            initial_storage=assemble_storage(),  # Описание файла базы для подсказки
            demo_mode=demo_mode,  # Флаг демо для вывода на страницу
        )  # Возвращаем HTML страницу

    @app.route("/api/stats")
    def stats():
        range_raw = request.args.get("range") or request.args.get("minutes")  # Читаем желаемый диапазон из запроса
        selected_range = resolve_range_minutes(range_raw)  # Нормализуем диапазон
        log_service_event(200, f"Отдаём JSON со статистикой за {selected_range} минут")  # Фиксируем успешную выдачу статистики
        return jsonify(assemble_stats(selected_range))  # Возвращаем актуальную статистику в JSON

    @app.route("/api/overview")
    def overview():
        log_service_event(200, "Отдаём обзор сообщества и диалогов")  # Фиксируем отдачу обзорных данных
        return jsonify(
            {
                "group": group_info,  # Информация о сообществе
                "conversations": assemble_conversations(),  # Список диалогов с учетом базы
                "peers": event_logger.list_peers(),  # Список доступных чатов
                "storage": assemble_storage(),  # Описание файла базы
            }
        )  # Возвращаем обзорную информацию

    @app.route("/api/logs")
    def logs():
        peer_id_raw = request.args.get("peer_id")  # Читаем peer_id из запроса
        limit_raw = request.args.get("limit")  # Читаем лимит из запроса
        peer_id = int(peer_id_raw) if peer_id_raw else None  # Преобразуем в число при наличии
        limit = int(limit_raw) if limit_raw else 50  # Устанавливаем лимит выборки
        limit = max(1, min(limit, 1000))  # Ограничиваем диапазон лимита
        messages = [serialize_log(row) for row in event_logger.fetch_messages(peer_id=peer_id, limit=limit)]  # Запрашиваем логи
        log_service_event(200, f"Отдаём JSON с логами peer_id={peer_id} и лимитом {limit}")  # Логируем успешную отдачу логов
        return jsonify({"items": messages, "peer_id": peer_id})  # Возвращаем JSON с логами

    @app.route("/attachments/<path:subpath>")
    def serve_attachment(subpath: str):  # Отдаем сохраненное вложение из папки
        target_path = (ATTACHMENTS_ROOT / subpath).resolve()  # Строим полный путь до файла
        if not str(target_path).startswith(str(ATTACHMENTS_ROOT)):  # Проверяем, что путь внутри директории вложений
            log_service_event(403, "Запрос вложения вне разрешенной директории отклонен")  # Пишем предупреждение в сервисные логи
            return "Недоступно", 403  # Возвращаем ошибку доступа
        if not target_path.exists():  # Проверяем наличие файла
            log_service_event(404, f"Файл вложения не найден: {subpath}")  # Фиксируем отсутствие файла
            return "Файл не найден", 404  # Отдаем 404
        relative = target_path.relative_to(ATTACHMENTS_ROOT)  # Получаем относительный путь
        return send_from_directory(ATTACHMENTS_ROOT, relative.as_posix())  # Отдаем файл через Flask

    @app.route("/api/logs/clear", methods=["POST"])
    def clear_logs():
        event_logger.clear_messages()  # Очищаем все записи событий в таблице
        log_service_event(201, "Логи сообщений очищены через API")  # Фиксируем факт очистки в сервисных событиях
        return jsonify({"status": "cleared"})  # Возвращаем подтверждение клиенту

    @app.route("/api/logs/<int:log_id>", methods=["DELETE"])
    def delete_log(log_id: int):
        deleted = event_logger.delete_message(log_id)  # Пытаемся удалить строку по ID
        if not deleted:  # Проверяем, была ли найдена запись
            log_service_event(404, f"Запись лога сообщений id={log_id} не найдена для удаления")  # Логируем отсутствие строки
            return jsonify({"status": "not_found", "id": log_id}), 404  # Возвращаем 404, если строка не найдена
        log_service_event(200, f"Запись лога сообщений id={log_id} удалена через API")  # Фиксируем успешное удаление
        return jsonify({"status": "deleted", "id": log_id})  # Отдаем подтверждение успешного удаления

    @app.route("/api/service-logs")
    def service_logs():
        event_type = request.args.get("event_type")  # Читаем тип события из запроса
        limit_raw = request.args.get("limit")  # Читаем желаемый лимит
        offset_raw = request.args.get("offset")  # Читаем смещение для пагинации
        limit = int(limit_raw) if limit_raw else 50  # Преобразуем лимит в число
        limit = max(1, min(limit, 200))  # Ограничиваем лимит разумными рамками
        offset = int(offset_raw) if offset_raw else 0  # Преобразуем смещение
        offset = max(0, offset)  # Не даем отрицательных смещений
        rows = service_events.fetch_events(event_type=event_type, limit=limit, offset=offset)  # Получаем строки из базы
        total = service_events.count_events(event_type=event_type)  # Считаем общее количество
        payload = [serialize_service_event(row) for row in rows]  # Сериализуем события
        log_service_event(200, f"Отдаём сервисные логи type={event_type} лимит={limit} смещение={offset}")  # Фиксируем отдачу
        return jsonify({"items": payload, "total": total, "limit": limit, "offset": offset})  # Возвращаем JSON ответ

    @app.route("/api/service-logs/clear", methods=["POST"])
    def clear_service_logs():
        service_events.clear_events()  # Очищаем таблицу сервисных событий
        log_service_event(201, "Сервисные логи очищены через API")  # Фиксируем очистку
        return jsonify({"status": "cleared"})  # Возвращаем подтверждение

    @app.route("/logs/full")
    def full_logs():
        peer_id_raw = request.args.get("peer_id")  # Читаем фильтр чата из адресной строки
        peer_id = int(peer_id_raw) if peer_id_raw else None  # Преобразуем в число при наличии
        limit_raw = request.args.get("limit")  # Читаем требуемый лимит
        limit = int(limit_raw) if limit_raw else 500  # Преобразуем лимит в число
        limit = max(1, min(limit, 1000))  # Ограничиваем лимит безопасными рамками
        logs_payload = [serialize_log(row) for row in event_logger.fetch_messages(peer_id=peer_id, limit=limit)]  # Получаем список логов
        service_logs_payload = [serialize_service_event(row) for row in service_events.fetch_events(limit=50)]  # Получаем стартовые сервисные логи
        log_service_event(200, f"Отдаём HTML со всеми логами peer_id={peer_id} (лимит {limit})")  # Фиксируем выдачу страницы логов
        return render_template(
            "logs.html",  # Шаблон страницы логов
            initial_logs=logs_payload,  # Начальный список логов
            initial_peers=event_logger.list_peers(),  # Доступные чаты для фильтрации
            initial_peer_id=peer_id,  # Текущий выбранный чат
            initial_limit=limit,  # Текущий выбранный лимит
            initial_service_logs=service_logs_payload,  # Стартовый набор сервисных логов
        )  # Возвращаем HTML страницы

    @app.route("/api/storage")
    def storage():
        log_service_event(200, "Отдаём информацию о файле логов")  # Фиксируем успешную отдачу сведений о файле
        return jsonify(assemble_storage())  # Возвращаем информацию о файле логов

    return app  # Возвращаем готовое Flask-приложение


def main() -> None:
    global service_event_logger  # Сообщаем, что будем обновлять глобальный логгер сервисных событий
    settings = load_settings()  # Загружаем настройки окружения
    service_event_logger = ServiceEventLogger(os.getenv("EVENT_DB", resolve_db_path()))  # Создаем логгер сервисных событий в базе
    log_service_event(200, "Настройки окружения загружены")  # Фиксируем успешную загрузку настроек
    state = BotState()  # Создаем объект состояния
    event_logger = EventLogger(os.getenv("EVENT_DB", resolve_db_path()))  # Готовим логгер с путём из окружения или по умолчанию
    demo_mode = settings.get("demo_mode", False)  # Проверяем, включен ли демо-режим
    if demo_mode:  # Если демо-режим включен
        payload = build_demo_payload(state, event_logger)  # Генерируем демо-данные и пишем их в базу
        group_info = payload["group_info"]  # Получаем демо-профиль
        conversations = payload["conversations"]  # Получаем демо-диалоги
        monitor = None  # Монитор не нужен в демо-режиме
        log_service_event(200, "Демо-режим активирован, лонгпулл не запускается")  # Фиксируем включение демо-режима
    else:  # Обычный режим подключения к VK
        logger.info("Используем ID сообщества: %s", settings["group_id"])  # Логируем ID сообщества
        log_service_event(200, f"Запускаем лонгпулл для сообщества {settings['group_id']}")  # Пишем сервисный лог о старте
        session = vk_api.VkApi(token=settings["token"])  # Создаем сессию VK API
        try:  # Пробуем запросить профиль сообщества
            group_info = fetch_group_profile(session, settings["group_id"])  # Получаем информацию о сообществе
        except Exception as exc:  # Если запрос завершился ошибкой
            logger.exception("Не удалось загрузить информацию о сообществе: %s", exc)  # Логируем подробности
            log_service_event(500, "Ошибка загрузки информации о сообществе")  # Фиксируем ошибку получения профиля
            group_info = {}  # Используем пустой словарь
        try:  # Пробуем получить диалоги
            conversations = fetch_recent_conversations(session)  # Запрашиваем список диалогов
        except Exception as exc:  # Обрабатываем исключения VK API
            logger.exception("Не удалось получить список диалогов: %s", exc)  # Логируем ошибку
            log_service_event(500, "Ошибка загрузки списка диалогов")  # Записываем ошибку в сервисный лог
            conversations = []  # Используем пустой список
        monitor = BotMonitor(settings["token"], settings["group_id"], state, event_logger)  # Создаем монитор лонгпулла
        monitor.start()  # Запускаем лонгпулл
    app = build_dashboard_app(state, group_info, conversations, demo_mode, event_logger, service_event_logger)  # Создаем Flask-приложение
    port = int(os.getenv("PORT", "8000"))  # Определяем порт из окружения
    logger.info("Дашборд запущен на http://127.0.0.1:%s", port)  # Сообщаем адрес запуска
    log_service_event(200, f"Дашборд поднят на порту {port}")  # Фиксируем успешный старт веб-сервера
    app.run(host="0.0.0.0", port=port)  # Запускаем сервер


if __name__ == "__main__":  # Точка входа
    try:  # Защищаем основной запуск от необработанных ошибок
        main()  # Запускаем приложение
    except Exception as exc:  # Если произошла ошибка
        logger.exception("Приложение завершилось с ошибкой: %s", exc)  # Пишем стек ошибки
        log_service_event(500, "Приложение аварийно завершилось")  # Дублируем ошибку в сервисный лог
        input("Нажмите Enter, чтобы закрыть окно...")  # Не даем окну закрыться мгновенно в Windows
