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
try:  # Пробуем подключить дополнительный загрузчик видео
    import yt_dlp as ytdlp  # yt-dlp позволяет скачивать видео по ссылке на плеер VK
except Exception:  # Отлавливаем любую ошибку импорта
    ytdlp = None  # Сохраняем None, чтобы код знал об отсутствии зависимости
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
    408: "Таймаут: сервер не дождался запроса",  # Описание для кода 408
    409: "Конфликт данных: проверьте уникальность или состояние",  # Описание для кода 409
    410: "Ресурс удалён: файл или запись больше недоступны",  # Описание для кода 410
    413: "Слишком большой файл или тело запроса",  # Описание для кода 413
    415: "Неподдерживаемый формат содержимого",  # Описание для кода 415
    418: "Я — чайник: нестандартный ответ сервера",  # Описание для кода 418
    429: "Слишком много запросов: сработало ограничение",  # Описание для кода 429
    500: "Ошибка сервера: смотреть стек",  # Описание для кода 500
    502: "Плохой шлюз: ошибка на промежуточном сервере",  # Описание для кода 502
    503: "Сервис недоступен: попробуйте позже",  # Описание для кода 503
    504: "Гейтвей не дождался ответа: истёк таймаут",  # Описание для кода 504
}  # Справочник кодов и русских пояснений для сервисных логов

def safe_int_env(value: Optional[str], fallback: int) -> int:  # Функция безопасного приведения переменных окружения к int
    try:  # Пробуем выполнить приведение типов
        return int(value) if value is not None else fallback  # Возвращаем число или запасное значение
    except Exception:  # Если приведение не удалось
        return fallback  # Возвращаем запасной вариант


DEFAULT_TIMELINE_MINUTES = safe_int_env(os.getenv("TIMELINE_DEFAULT_MINUTES"), 60)  # Диапазон минут по умолчанию для графика
ATTACHMENTS_ROOT = Path(os.getenv("ATTACHMENTS_DIR") or os.path.join(os.getcwd(), "data", "attachments")).resolve()  # Базовая папка для вложений, доступная через веб
ATTACHMENTS_ROOT.mkdir(parents=True, exist_ok=True)  # Создаем директорию вложений, если её нет
STICKER_CACHE_DIR = ATTACHMENTS_ROOT / "stickers"  # Отдельная папка для кэширования стикеров по их ID
STICKER_CACHE_DIR.mkdir(parents=True, exist_ok=True)  # Создаем папку кэша стикеров, чтобы можно было сохранять старые наклейки
MESSAGES_PAGE_SIZE = 50  # Размер страницы для постраничной подгрузки сообщений


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


def log_service_event(status_code: int, message: str, persist_success: bool = False) -> None:  # Упрощенный вызов для записи сервисных событий
    """Пишет сервисное событие с опциональным сохранением успешных запросов."""

    description = SERVICE_STATUS_EXPLANATIONS.get(status_code, "Сервисное сообщение")  # Находим пояснение по коду
    service_logger.info(message, extra={"status_code": status_code, "status_description": description})  # Логируем событие в файл
    should_persist = persist_success or status_code >= 400  # Решаем, писать ли успешные события в базу
    if should_persist and service_event_logger is not None:  # Проверяем, инициализирован ли логгер базы и нужно ли писать
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

    def mark_message_deleted(self, message_id: Optional[int]) -> bool:
        """Помечает записанное сообщение как удалённое по его VK ID."""

        if not isinstance(message_id, int):  # Проверяем, что передан корректный числовой ID
            return False  # Возвращаем, что обновление не выполнено
        with self._lock:  # Оборачиваем обновление в блокировку для потокобезопасности
            cursor = self._connection.cursor()  # Берём курсор для выполнения запросов
            cursor.execute(  # Выбираем строки с указанным message_id только для событий типа message
                "SELECT id, payload FROM events WHERE message_id = ? AND event_type = ?",
                (message_id, "message"),
            )
            rows = cursor.fetchall()  # Читаем найденные записи
            if not rows:  # Проверяем, есть ли что обновлять
                return False  # Возвращаем отсутствие обновлений
            for row in rows:  # Перебираем каждую подходящую запись
                try:  # Пробуем распарсить payload строки
                    payload = json.loads(row["payload"] or "{}") if isinstance(row, sqlite3.Row) else {}
                except Exception:  # Если JSON некорректен
                    payload = {}  # Используем пустой словарь, чтобы не падать
                payload["deleted"] = True  # Сохраняем признак удаления
                payload["was_deleted"] = True  # Дублируем признак для альтернативных проверок
                payload["is_deleted"] = True  # Ставим явный флаг удаления
                cursor.execute(  # Обновляем payload в базе для конкретной строки
                    "UPDATE events SET payload = ? WHERE id = ?",
                    (json.dumps(payload, ensure_ascii=False), row["id"]),
                )
            self._connection.commit()  # Фиксируем обновлённые данные
            return True  # Сообщаем, что хотя бы одна запись была обновлена

    def clear_messages(self) -> None:
        with self._lock:  # Начинаем потокобезопасную операцию
            cursor = self._connection.cursor()  # Получаем курсор
            cursor.execute("DELETE FROM events")  # Удаляем все строки таблицы событий
            self._connection.commit()  # Фиксируем изменения после удаления
        self._vacuum()  # Запускаем VACUUM вне блокировки, чтобы освободить место и уменьшить файл

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

    def _vacuum(self) -> None:
        with self._lock:  # Начинаем потокобезопасную операцию
            original_isolation = self._connection.isolation_level  # Запоминаем исходный режим автокоммита
            self._connection.isolation_level = None  # Переводим соединение в автокоммит для VACUUM
            try:  # Оборачиваем в try/finally, чтобы вернуть режим даже при ошибке
                self._connection.execute("VACUUM")  # Запускаем VACUUM для сжатия файла базы
            finally:  # Гарантируем возврат исходных настроек
                self._connection.isolation_level = original_isolation  # Восстанавливаем режим автокоммита

    def fetch_messages(
        self, peer_id: Optional[int] = None, limit: int = 50, offset: int = 0, from_id: Optional[int] = None
    ) -> List[Dict]:
        with self._lock:  # Начинаем безопасное чтение
            cursor = self._connection.cursor()  # Берем курсор
            base_query = "SELECT * FROM events WHERE event_type = ?"  # Базовый запрос выборки
            params: List[object] = ["message"]  # Начальные параметры для запроса
            if peer_id is not None:  # Если задан фильтр по чату
                base_query += " AND peer_id = ?"  # Добавляем условие по чату
                params.append(int(peer_id))  # Подставляем значение peer_id
            if from_id is not None:  # Если задан фильтр по отправителю
                base_query += " AND from_id = ?"  # Добавляем условие по отправителю
                params.append(int(from_id))  # Подставляем значение from_id
            base_query += " ORDER BY id DESC LIMIT ? OFFSET ?"  # Добавляем сортировку и пагинацию
            params.extend([int(limit), max(0, int(offset))])  # Добавляем лимит и смещение с защитой от отрицательных значений
            cursor.execute(base_query, tuple(params))  # Выполняем сформированный запрос
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

    def summarize_peer(self, peer_id: int) -> Optional[Dict[str, object]]:
        with self._lock:  # Начинаем потокобезопасное чтение
            cursor = self._connection.cursor()  # Берем курсор
            cursor.execute(  # Считаем основную статистику по чату
                """
                SELECT
                    peer_id,
                    COALESCE(peer_title, '') AS peer_title,
                    COALESCE(peer_avatar, '') AS peer_avatar,
                    COUNT(*) AS total_messages,
                    MAX(created_at) AS last_message_time,
                    COUNT(DISTINCT from_id) AS unique_senders
                FROM events
                WHERE event_type = 'message' AND peer_id = ?
                """,
                (int(peer_id),),
            )
            summary_row = cursor.fetchone()  # Читаем результат агрегации
            cursor.execute(  # Подтягиваем последнюю строку с ненулевым названием для корректной подписи
                """
                SELECT peer_title, peer_avatar
                FROM events
                WHERE event_type = 'message' AND peer_id = ? AND peer_title IS NOT NULL
                ORDER BY id DESC
                LIMIT 1
                """,
                (int(peer_id),),
            )
            title_row = cursor.fetchone()  # Читаем строку с названием
        if not summary_row or summary_row["total_messages"] == 0:  # Проверяем, есть ли сообщения у чата
            return None  # Возвращаем пустой результат при отсутствии данных
        return {  # Собираем словарь сводки по чату
            "peer_id": summary_row["peer_id"],  # ID чата
            "peer_title": (title_row["peer_title"] if title_row else summary_row["peer_title"]) or "Чат без названия",  # Название
            "peer_avatar": (title_row["peer_avatar"] if title_row else summary_row["peer_avatar"]) or None,  # Аватар
            "total_messages": summary_row["total_messages"],  # Общее количество сообщений
            "last_message_time": summary_row["last_message_time"],  # Время последнего сообщения
            "unique_senders": summary_row["unique_senders"],  # Количество уникальных отправителей
        }

    def summarize_user(self, user_id: int) -> Optional[Dict[str, object]]:
        with self._lock:  # Начинаем потокобезопасное чтение
            cursor = self._connection.cursor()  # Берем курсор
            cursor.execute(  # Считаем основную статистику по пользователю
                """
                SELECT
                    from_id,
                    COALESCE(from_name, '') AS from_name,
                    COALESCE(from_avatar, '') AS from_avatar,
                    COUNT(*) AS total_messages,
                    MAX(created_at) AS last_message_time,
                    COUNT(DISTINCT peer_id) AS unique_peers
                FROM events
                WHERE event_type = 'message' AND from_id = ?
                """,
                (int(user_id),),
            )
            summary_row = cursor.fetchone()  # Читаем результат агрегации
            cursor.execute(  # Ищем последнюю запись с заполненным именем, чтобы показать его в карточке
                """
                SELECT from_name, from_avatar
                FROM events
                WHERE event_type = 'message' AND from_id = ? AND from_name IS NOT NULL
                ORDER BY id DESC
                LIMIT 1
                """,
                (int(user_id),),
            )
            name_row = cursor.fetchone()  # Читаем строку с именем
        if not summary_row or summary_row["total_messages"] == 0:  # Проверяем наличие сообщений пользователя
            return None  # Возвращаем пустой результат при отсутствии данных
        return {  # Собираем словарь сводки по пользователю
            "from_id": summary_row["from_id"],  # ID отправителя
            "from_name": (name_row["from_name"] if name_row else summary_row["from_name"]) or "Неизвестный отправитель",  # Имя
            "from_avatar": (name_row["from_avatar"] if name_row else summary_row["from_avatar"]) or None,  # Аватар
            "total_messages": summary_row["total_messages"],  # Количество сообщений
            "last_message_time": summary_row["last_message_time"],  # Время последнего сообщения
            "unique_peers": summary_row["unique_peers"],  # Количество уникальных чатов
        }

    def fetch_messages_by_user(
        self, user_id: int, limit: int = 50, peer_id: Optional[int] = None, offset: int = 0
    ) -> List[Dict]:
        with self._lock:  # Начинаем безопасное чтение
            cursor = self._connection.cursor()  # Берем курсор
            params: List[object] = ["message", int(user_id)]  # Готовим параметры запроса
            base_query = "SELECT * FROM events WHERE event_type = ? AND from_id = ?"  # Базовый запрос по отправителю
            if peer_id is not None:  # Если нужно ограничить конкретным чатом
                base_query += " AND peer_id = ?"  # Добавляем фильтр по чату
                params.append(int(peer_id))  # Подставляем значение peer_id
            base_query += " ORDER BY id DESC LIMIT ? OFFSET ?"  # Добавляем сортировку и пагинацию
            params.extend([int(limit), max(0, int(offset))])  # Добавляем лимит и смещение с защитой от отрицательных значений
            cursor.execute(base_query, tuple(params))  # Выполняем запрос
            rows = cursor.fetchall()  # Читаем строки
        return [dict(row) for row in rows]  # Возвращаем список словарей

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
        self._vacuum()  # Запускаем VACUUM вне блокировки, чтобы уменьшить файл базы после очистки

    def _vacuum(self) -> None:
        with self._lock:  # Начинаем защищенную операцию
            original_isolation = self._connection.isolation_level  # Запоминаем исходный режим автокоммита
            self._connection.isolation_level = None  # Переключаем соединение в автокоммит для VACUUM
            try:  # Оборачиваем в try/finally, чтобы гарантировать возврат настроек
                self._connection.execute("VACUUM")  # Запускаем VACUUM для физического сжатия файла базы
            finally:  # Независимо от результата
                self._connection.isolation_level = original_isolation  # Возвращаем исходный режим автокоммита


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
        self.sticker_cache_dir = self.attachments_dir / "stickers"  # Директория для кэша стикеров по их ID
        self.sticker_cache_dir.mkdir(parents=True, exist_ok=True)  # Создаем папку кэша стикеров, если её нет

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

    def _resolve_message_id_by_conversation(self, peer_id: Optional[int], conversation_message_id: Optional[int]) -> Optional[int]:
        """Пытается получить глобальный message_id по паре peer_id + conversation_message_id."""

        if not isinstance(peer_id, int) or not isinstance(conversation_message_id, int):  # Проверяем корректность входных данных
            return None  # Возвращаем None, если нельзя построить запрос
        try:  # Пробуем запросить данные о сообщении через VK API
            response = self.session.method(  # Делаем запрос по conversation_message_id
                "messages.getByConversationMessageId",
                {
                    "peer_id": peer_id,  # Передаём идентификатор чата
                    "conversation_message_ids": conversation_message_id,  # Передаём ID сообщения внутри переписки
                    "group_id": self.group_id,  # Указываем ID группы для корректных прав
                    "extended": 0,  # Дополнительные данные не нужны, берём только сообщение
                },
            )
            items = response.get("items", []) if isinstance(response, dict) else []  # Извлекаем список сообщений из ответа
            if not items:  # Проверяем, что ответ содержит данные
                return None  # Возвращаем None, если ничего не нашли
            first_item = items[0] if isinstance(items[0], dict) else {}  # Берём первое сообщение из списка
            resolved_id = first_item.get("id") if isinstance(first_item.get("id"), int) else None  # Достаём глобальный ID
            return resolved_id  # Возвращаем найденный ID или None
        except Exception as exc:  # Ловим ошибки запроса к API
            logger.debug(
                "Не удалось разрешить message_id по conversation_message_id %s в peer %s: %s",
                conversation_message_id,
                peer_id,
                exc,
            )  # Пишем отладочный лог при неудаче
            return None  # Возвращаем None при исключении

    def _handle_deletion_event(self, event) -> bool:
        """Обрабатывает событие удаления сообщения и возвращает, было ли оно обработано."""

        candidate = None  # Подготавливаем контейнер для полезной нагрузки
        if hasattr(event, "object") and isinstance(getattr(event, "object"), dict):  # Проверяем, что объект события — словарь
            candidate = event.object  # Сохраняем словарь объекта
        elif hasattr(event, "object") and hasattr(event.object, "message") and isinstance(event.object.message, dict):  # Проверяем сообщение внутри объекта
            candidate = event.object.message  # Извлекаем словарь сообщения
        if not isinstance(candidate, dict):  # Если полезная нагрузка не словарь
            return False  # Завершаем без обработки
        action_block = candidate.get("action") if isinstance(candidate.get("action"), dict) else {}  # Получаем блок action
        action_type = action_block.get("type") if isinstance(action_block, dict) else None  # Читаем тип действия
        if action_type not in ("chat_message_delete", "message_delete"):  # Проверяем, относится ли событие к удалению
            return False  # Возвращаем, что событие не обработано
        message_id = action_block.get("message_id") or candidate.get("id")  # Пытаемся получить глобальный ID сообщения
        peer_id = candidate.get("peer_id") or action_block.get("peer_id")  # Извлекаем peer_id из события
        conversation_message_id = action_block.get("conversation_message_id") or candidate.get("conversation_message_id")  # Достаём ID сообщения в переписке
        if not isinstance(message_id, int):  # Если глобальный ID не получен
            message_id = self._resolve_message_id_by_conversation(peer_id, conversation_message_id)  # Пробуем вычислить его по переписке
        updated = self.event_logger.mark_message_deleted(message_id) if message_id is not None else False  # Пытаемся обновить запись в базе
        if updated:  # Проверяем, удалось ли обновить хотя бы одну строку
            logger.info("Пометили сообщение %s как удалённое", message_id)  # Пишем информационный лог
        else:  # Если обновить не удалось
            logger.warning("Не нашли сообщение %s для пометки удаления", message_id)  # Логируем предупреждение
        return updated  # Возвращаем, было ли событие обработано

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
        files_block = video_block.get("files") if isinstance(video_block.get("files"), dict) else {}  # Забираем готовые ссылки mp4 из payload
        if files_block:  # Проверяем, что блок файлов присутствует
            candidates = [files_block.get(key) for key in sorted(files_block.keys()) if key.startswith("mp4") or key == "mp4"]  # Собираем ссылки mp4 прямо из сообщения
            candidates = [url for url in candidates if isinstance(url, str)]  # Оставляем только строки URL
            if candidates:  # Проверяем, что нашлись прямые ссылки
                return candidates[-1]  # Возвращаем ссылку с максимальным качеством
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

    def _resolve_video_player_url(self, video_block: Dict) -> Optional[str]:
        if not isinstance(video_block, dict):  # Проверяем, что блок видео представлен словарем
            return None  # Возвращаем пустое значение при некорректном формате
        player_link = video_block.get("player")  # Пытаемся достать ссылку на плеер из исходного payload
        if isinstance(player_link, str) and player_link:  # Проверяем, что ссылка на плеер валидная строка
            return player_link  # Возвращаем найденную ссылку на плеер
        owner_id = video_block.get("owner_id")  # Получаем owner_id для обращения к VK API
        video_id = video_block.get("id")  # Получаем id видео для запроса
        access_key = video_block.get("access_key")  # Получаем access_key, если он присутствует
        if owner_id is None or video_id is None:  # Проверяем наличие обязательных идентификаторов
            return None  # Без идентификаторов нельзя запросить player через API
        videos_param = f"{owner_id}_{video_id}" + (f"_{access_key}" if access_key else "")  # Формируем параметр videos для запроса
        try:  # Пытаемся обратиться к VK API за ссылкой на плеер
            response = self.session.method("video.get", {"videos": videos_param})  # Запрашиваем данные видео через video.get
            items = response.get("items", []) if isinstance(response, dict) else []  # Забираем список элементов из ответа
            if not items:  # Проверяем, что ответ содержит данные
                return None  # Если элементов нет, вернуть нечего
            player_link = items[0].get("player") if isinstance(items[0], dict) else None  # Достаём ссылку на плеер из первого элемента
            if isinstance(player_link, str) and player_link:  # Проверяем, что ссылка корректна
                return player_link  # Возвращаем найденную ссылку на плеер
        except Exception as exc:  # Обрабатываем любые исключения от VK API
            logger.debug("Не удалось запросить ссылку плеера: %s", exc)  # Пишем отладочное сообщение о неудаче
        page_link = f"https://vk.com/video{owner_id}_{video_id}"  # Собираем ссылку на страницу видео по owner_id и id
        if access_key:  # Проверяем, что есть access_key для приватных роликов
            page_link += f"?access_key={access_key}"  # Добавляем access_key в строку запроса
        return page_link  # Возвращаем хотя бы ссылку на страницу видео, даже если плеер не вернулся из API

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
        if att_type == "sticker":  # Обрабатываем особый случай стикера
            sticker_block = attachment.get("sticker") if isinstance(attachment.get("sticker"), dict) else {}  # Извлекаем блок стикера
            sticker_id = sticker_block.get("sticker_id") if isinstance(sticker_block, dict) else None  # Читаем sticker_id для сигнатуры
            product_id = sticker_block.get("product_id") if isinstance(sticker_block, dict) else None  # Читаем product_id, если он есть
            if sticker_id is not None:  # Проверяем, что sticker_id найден
                return f"sticker:{sticker_id}:{product_id or ''}"  # Формируем сигнатуру по sticker_id и product_id
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

    def _pick_sticker_image_url(self, sticker_block: Dict) -> Optional[str]:
        if not isinstance(sticker_block, dict):  # Проверяем, что блок стикера — словарь
            return None  # Возвращаем пустое значение при неверном формате
        images = []  # Подготавливаем список вариантов изображений
        raw_images = sticker_block.get("images") if isinstance(sticker_block.get("images"), list) else []  # Забираем список обычных изображений
        raw_bg = sticker_block.get("images_with_background") if isinstance(sticker_block.get("images_with_background"), list) else []  # Забираем список изображений с фоном
        images.extend(raw_images)  # Добавляем обычные варианты в общий список
        images.extend(raw_bg)  # Добавляем варианты с фоном в общий список
        if not images:  # Проверяем, что хотя бы один вариант найден
            return None  # Возвращаем пустое значение при отсутствии ссылок
        sorted_images = sorted(images, key=lambda item: (item.get("width", 0) or 0) * (item.get("height", 0) or 0), reverse=True)  # Сортируем по площади изображения
        best = sorted_images[0] if sorted_images else None  # Берем самый крупный вариант
        return best.get("url") if isinstance(best, dict) else None  # Возвращаем ссылку на лучший вариант

    def _get_cached_sticker_file(self, sticker_id: Optional[int]) -> Optional[Path]:
        if not isinstance(sticker_id, int):  # Проверяем корректность идентификатора стикера
            return None  # Возвращаем пустое значение при неверном ID
        for candidate in self.sticker_cache_dir.glob(f"sticker_{sticker_id}.*"):  # Перебираем файлы подходящего шаблона
            if candidate.exists():  # Проверяем, что файл действительно существует
                return candidate  # Возвращаем найденный путь к файлу
        return None  # Возвращаем пустое значение, если кэш не найден

    def _sticker_fallback_urls(self, sticker_id: Optional[int]) -> List[str]:
        if not isinstance(sticker_id, int):  # Проверяем, что ID стикера корректный
            return []  # Возвращаем пустой список при отсутствии ID
        base_sizes = [512, 256, 128]  # Задаем список размеров, которые попробуем скачать
        urls: List[str] = []  # Подготавливаем список кандидатов
        for size in base_sizes:  # Перебираем каждый размер
            urls.append(f"https://vk.com/sticker/{sticker_id}-{size}-{size}-0")  # Добавляем ссылку на PNG без фона
            urls.append(f"https://vk.com/sticker/{sticker_id}-{size}-{size}-1")  # Добавляем ссылку на PNG с фоном
        return urls  # Возвращаем список кандидатов для загрузки

    def _build_sticker_cache_path(self, sticker_id: int, source_url: Optional[str]) -> Path:
        parsed = urlparse(source_url or "")  # Парсим URL, даже если он пустой
        suffix = Path(parsed.path).suffix  # Пытаемся извлечь расширение файла
        safe_suffix = suffix if suffix else ".webp"  # Подставляем расширение WebP по умолчанию
        filename = f"sticker_{sticker_id}{safe_suffix}"  # Формируем имя файла стикера
        return self.sticker_cache_dir / filename  # Возвращаем полный путь к файлу в кэше

    def _cache_sticker_image(self, sticker_id: Optional[int], primary_url: Optional[str]) -> tuple[Optional[Path], Optional[str]]:
        if not isinstance(sticker_id, int):  # Проверяем корректность ID перед попыткой кэширования
            return None, "Стикер без sticker_id нельзя сохранить в кэш"  # Сообщаем причину невозможности кэширования
        cached = self._get_cached_sticker_file(sticker_id)  # Пытаемся найти готовый кэш
        if cached:  # Проверяем, найден ли файл
            return cached, None  # Возвращаем путь без ошибки
        candidates: List[str] = []  # Подготавливаем список ссылок для скачивания
        if primary_url:  # Проверяем, передана ли основная ссылка
            candidates.append(primary_url)  # Добавляем основную ссылку первой
        candidates.extend(self._sticker_fallback_urls(sticker_id))  # Добавляем обходные ссылки по ID стикера
        error_reason = None  # Подготавливаем переменную для текста ошибки
        for candidate_url in candidates:  # Перебираем все кандидаты
            target_path = self._build_sticker_cache_path(sticker_id, candidate_url)  # Строим путь сохранения для конкретной ссылки
            saved_path, download_error, _status_code = self._download_file(candidate_url, target_path)  # Пытаемся скачать файл
            if saved_path:  # Проверяем, удалось ли сохранить файл
                return saved_path, None  # Возвращаем путь к скачанному файлу и отсутствие ошибки
            error_reason = download_error or f"Не удалось скачать стикер по ссылке {candidate_url}"  # Сохраняем последнюю причину
        return None, error_reason or "Нет доступных ссылок для скачивания стикера"  # Возвращаем итоговую ошибку при неудаче

    def _download_file(self, url: str, target_path: Path) -> tuple[Optional[Path], Optional[str], Optional[int]]:
        try:  # Пробуем скачать файл
            response = requests.get(url, timeout=30, stream=True)  # Выполняем HTTP-запрос с таймаутом
            status_code = getattr(response, "status_code", 0) or 0  # Сохраняем код ответа для логов
            response.raise_for_status()  # Бросаем исключение при ошибке статуса
            with target_path.open("wb") as file_handle:  # Открываем файл для записи
                for chunk in response.iter_content(chunk_size=8192):  # Читаем ответ блоками
                    if not chunk:  # Пропускаем пустые блоки
                        continue  # Переходим к следующему блоку
                    file_handle.write(chunk)  # Записываем блок в файл
            return target_path, None, status_code  # Возвращаем путь к файлу и успешный статус
        except requests.HTTPError as exc:  # Обрабатываем HTTP-ошибки с кодами
            response = exc.response  # Извлекаем ответ сервера из исключения
            status_code = response.status_code if response is not None else 500  # Берём код ответа или ставим 500
            reason_phrase = response.reason if response is not None else str(exc)  # Читаем текстовое пояснение
            error_message = f"HTTP {status_code}: {reason_phrase}"  # Формируем сообщение об ошибке
            logger.warning("Не удалось сохранить вложение %s: %s", url, error_message)  # Пишем предупреждение в лог
            log_service_event(status_code, f"Ошибка скачивания вложения {url}: {error_message}")  # Дублируем ошибку в сервисные логи
            return None, error_message, status_code  # Возвращаем пустой путь и причину
        except Exception as exc:  # Обрабатываем прочие ошибки скачивания
            error_message = str(exc)  # Сохраняем текст исключения для фронта
            logger.warning("Не удалось сохранить вложение %s: %s", url, error_message)  # Пишем предупреждение в лог
            log_service_event(500, f"Ошибка скачивания вложения {url}: {error_message}")  # Дублируем ошибку без кода ответа
            return None, error_message, None  # Возвращаем пустой путь и текст ошибки

    def _download_video_via_player(self, player_url: Optional[str], target_path: Path) -> tuple[Optional[Path], Optional[str]]:
        if not player_url or not isinstance(player_url, str):  # Проверяем, что ссылка на плеер передана корректно
            return None, "Нет ссылки на плеер VK для скачивания"  # Возвращаем причину отсутствия ссылки
        if ytdlp is None:  # Проверяем, доступна ли библиотека yt-dlp
            return None, "yt-dlp не установлен: добавьте зависимость для скачивания через плеер"  # Сообщаем, что нужен пакет
        download_error_cls = getattr(getattr(ytdlp, "utils", None), "DownloadError", None)  # Достаём класс ошибки yt-dlp, если он есть
        safe_base = target_path.with_suffix("")  # Убираем расширение, чтобы yt-dlp добавил своё
        out_template = f"{safe_base}.%(ext)s"  # Формируем шаблон имени файла для yt-dlp
        ydl_options = {"outtmpl": out_template, "quiet": True, "no_warnings": True}  # Настраиваем yt-dlp без лишнего вывода
        try:  # Пытаемся скачать видео через yt-dlp
            with ytdlp.YoutubeDL(ydl_options) as downloader:  # Создаем загрузчик yt-dlp с заданными опциями
                downloader.download([player_url])  # Запускаем скачивание по ссылке на плеер VK
            downloaded_path = None  # Подготавливаем переменную для найденного файла
            for candidate in safe_base.parent.glob(f"{safe_base.name}.*"):  # Перебираем файлы, созданные yt-dlp по шаблону
                downloaded_path = candidate  # Запоминаем найденный файл
                break  # Достаточно первого совпадения
            if downloaded_path and downloaded_path.exists():  # Проверяем, что файл действительно создан
                return downloaded_path, None  # Возвращаем путь и отсутствие ошибки
            return None, "yt-dlp не сохранил файл по ссылке плеера"  # Сообщаем об отсутствии результата
        except Exception as exc:  # Обрабатываем любые сбои yt-dlp
            if download_error_cls and isinstance(exc, download_error_cls):  # Проверяем, что поймали специфичную ошибку yt-dlp
                error_text = str(exc)  # Извлекаем текст ошибки
                if "Access restricted" in error_text:  # Ищем признак ограниченного доступа к ролику
                    friendly_message = (
                        "Доступ к видео ограничен владельцем: нужен access_key или авторизация, "
                        "иначе лонгпулл не отдаёт mp4 и плеер нельзя скачать"
                    )  # Готовим понятное сообщение пользователю с пояснением про ограничения лонгпулла
                    logger.warning("Видео недоступно для скачивания: %s", friendly_message)  # Логируем предупреждение
                    log_service_event(403, f"yt-dlp отказано в доступе для {player_url}: {error_text}")  # Пишем сервисное событие с кодом 403
                    return None, friendly_message  # Возвращаем понятную причину
            error_message = f"yt-dlp: {exc}"  # Формируем человекочитаемое сообщение
            logger.warning("Не удалось скачать видео через плеер %s: %s", player_url, error_message)  # Пишем предупреждение в лог
            log_service_event(500, f"Ошибка yt-dlp при скачивании {player_url}: {exc}")  # Дублируем ошибку в сервисные логи
            return None, error_message  # Возвращаем причину сбоя

    def _describe_missing_download_url(self, att_type: Optional[str], attachment: Dict) -> str:
        attachment = attachment if isinstance(attachment, dict) else {}  # Нормализуем вложение к словарю
        content = attachment.get(att_type) if isinstance(att_type, str) else {}  # Получаем вложенный блок по типу
        if att_type == "video":  # Обрабатываем случай видео
            video_block = content if isinstance(content, dict) else {}  # Нормализуем блок видео
            files_block = video_block.get("files") if isinstance(video_block.get("files"), dict) else {}  # Извлекаем блок файлов mp4, если есть
            if files_block:  # Проверяем наличие блока files без mp4-ссылок
                return "Есть блок files, но внутри нет mp4-ссылок"  # Возвращаем пояснение про пустые ссылки
            player_link = video_block.get("player") if isinstance(video_block, dict) else None  # Забираем ссылку на плеер, если она есть
            if player_link:  # Проверяем, есть ли хотя бы ссылка на плеер
                return "Есть только ссылка на плеер VK, mp4 отсутствует"  # Сообщаем, что доступен лишь плеер
            if not video_block:  # Проверяем, пришел ли блок video
                return "Видео без блока video в payload"  # Поясняем, что данных для скачивания нет
            return "Видео без ссылок mp4 и без fallback плеера"  # Сообщаем, что нет ни mp4, ни плеера
        if att_type == "photo":  # Обрабатываем случай фото
            if isinstance(content, dict) and content.get("sizes"):  # Проверяем наличие размеров
                return "Фото без пригодных размеров для скачивания"  # Сообщаем, что размеры есть, но ссылки нет
            return "Фото без поля sizes/url в данных"  # Поясняем отсутствие ключевых полей
        if att_type == "audio_message":  # Обрабатываем голосовые сообщения
            return "Аудиосообщение без ссылок link_ogg/link_mp3"  # Сообщаем про отсутствие ссылок на аудио
        if att_type == "doc":  # Обрабатываем документы
            doc_block = content if isinstance(content, dict) else {}  # Нормализуем блок документа
            if doc_block:  # Проверяем, что блок документа присутствует
                return "Документ без поля url в payload"  # Поясняем, что ссылка не передана
            return "Документ без блока doc"  # Сообщаем про полное отсутствие данных документа
        if isinstance(content, dict) and content.get("url"):  # Проверяем наличие универсального поля url
            return "Ссылка url есть, но не подошла под известные типы"  # Сообщаем о неклассифицированной ссылке
        if att_type:  # Проверяем наличие типа вложения
            return f"Тип {att_type} без поля url"  # Формируем сообщение по типу без ссылки
        return "Неизвестный тип вложения без ссылки url"  # Сообщаем, что тип не определён и ссылки нет

    def _normalize_attachment(self, attachment: Dict, peer_id: Optional[int], message_id: Optional[int]) -> Dict:
        normalized = dict(attachment) if isinstance(attachment, dict) else {}  # Копируем вложение, чтобы не трогать оригинал
        att_type = normalized.get("type")  # Получаем тип вложения
        sticker_block = normalized.get("sticker") if isinstance(normalized.get("sticker"), dict) else {}  # Извлекаем блок стикера при наличии
        if att_type == "sticker":  # Обрабатываем особый случай стикеров сразу, не смешивая с общими правилами
            download_url = self._pick_sticker_image_url(sticker_block)  # Пытаемся взять ссылку из набора изображений стикера
            normalized["local_path"] = None  # Инициализируем путь до локального файла
            normalized["download_url"] = download_url  # Сохраняем исходную ссылку, даже если её нет
            normalized["transcript"] = normalized.get("transcript")  # Оставляем поле для совместимости с аудио
            normalized["download_state"] = "pending"  # Ставим статус «в процессе», пока не проверили кэш
            normalized["download_error"] = None  # Очищаем текст ошибки по умолчанию
            sticker_id = sticker_block.get("sticker_id") if isinstance(sticker_block, dict) else None  # Читаем sticker_id для ключа кэша
            cached_path, cache_error = self._cache_sticker_image(sticker_id, download_url)  # Пробуем достать или скачать файл стикера
            if cached_path:  # Проверяем, удалось ли получить файл из кэша или скачать
                normalized["local_path"] = str(cached_path)  # Записываем путь к локальному файлу
                normalized["download_state"] = "ready"  # Помечаем готовность вложения
                normalized["download_error"] = None  # Убираем возможные ошибки
                return normalized  # Возвращаем вложение без дополнительных попыток
            normalized["download_state"] = "failed"  # Помечаем, что сохранить стикер не удалось
            normalized["download_error"] = cache_error or "Не удалось сохранить стикер: нет доступных ссылок"  # Записываем понятную причину сбоя
            log_service_event(422, normalized["download_error"])  # Фиксируем проблему в сервисных логах
            return normalized  # Возвращаем вложение с ошибкой
        download_url = self._pick_attachment_url(normalized)  # Для остальных типов подбираем ссылку скачивания
        normalized["local_path"] = None  # Подготавливаем поле для пути
        normalized["download_url"] = download_url  # Сохраняем URL в явном виде
        normalized["transcript"] = normalized.get("transcript")  # Резерв для будущей расшифровки аудио
        normalized["download_state"] = "pending" if download_url else "missing"  # Помечаем статус скачивания по умолчанию
        normalized["download_error"] = None  # Подготавливаем поле для сообщения об ошибке скачивания
        video_block = normalized.get("video") if isinstance(normalized.get("video"), dict) else {}  # Извлекаем блок видео при наличии
        player_fallback = self._resolve_video_player_url(video_block) if att_type == "video" else None  # Пытаемся достать ссылку на плеер из payload или через API
        if not download_url and att_type == "video" and player_fallback:  # Проверяем, что mp4 не найден, но есть ссылка на плеер
            normalized["url"] = player_fallback  # Сохраняем ссылку на плеер, чтобы фронт мог открыть видео хотя бы во вкладке VK
            target_path = self._build_local_path(peer_id, message_id, player_fallback, att_type or "video")  # Формируем путь для сохранения через yt-dlp
            saved_path, error_reason = self._download_video_via_player(player_fallback, target_path)  # Пытаемся скачать видео через плеер VK
            if saved_path:  # Проверяем, что файл сохранён
                normalized["local_path"] = str(saved_path)  # Записываем путь до локального файла
                normalized["download_state"] = "ready"  # Помечаем успешную загрузку через yt-dlp
            else:  # Если сохранить не удалось
                normalized["download_state"] = "failed"  # Фиксируем неуспешную загрузку
                normalized["download_error"] = error_reason or "Не удалось скачать видео через плеер VK"  # Сохраняем причину сбоя
            return normalized  # Возвращаем вложение после попытки работы с плеером
        if not download_url:  # Проверяем, что ссылка для скачивания так и не появилась
            missing_reason = self._describe_missing_download_url(att_type, normalized)  # Формируем развернутое пояснение, почему ссылка не найдена
            normalized["download_error"] = f"Нет доступной ссылки для скачивания вложения: {missing_reason}"  # Сообщаем причину отсутствия файла с деталями
            log_service_event(422, normalized["download_error"])  # Фиксируем проблему в сервисных логах для диагностики
            return normalized  # Возвращаем вложение без попытки загрузки
        if download_url:  # Если удалось получить ссылку
            target_path = self._build_local_path(peer_id, message_id, download_url, att_type or "file")  # Формируем путь сохранения
            download_result = self._download_file(download_url, target_path)  # Пытаемся скачать файл с возвратом причины и кодом ответа
            if isinstance(download_result, tuple):  # Проверяем, вернулся ли кортеж с детальной информацией
                saved_path, error_reason, _status_code = download_result  # Распаковываем путь, причину и код ответа
            else:  # Обработка старых заглушек, которые возвращают только путь
                saved_path, error_reason, _status_code = download_result, None, None  # Подставляем пустые значения для причины и кода
            if saved_path:  # Проверяем успешность сохранения
                normalized["local_path"] = str(saved_path)  # Сохраняем путь к файлу
                normalized["download_state"] = "ready"  # Отмечаем успешную загрузку вложения
            else:  # Если скачать не удалось
                normalized["download_state"] = "failed"  # Фиксируем неуспешное скачивание
                normalized["download_error"] = error_reason or "Не удалось скачать вложение"  # Добавляем причину ошибки для фронта
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
                    if self._handle_deletion_event(event):  # Проверяем, является ли событие удалением сообщения
                        continue  # Переходим к следующему событию, чтобы не считать его новым сообщением
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
        db_storage = event_logger.describe_storage()  # Читаем информацию о файле базы
        attachments_root = ATTACHMENTS_ROOT  # Определяем корневую папку вложений
        attachments_exists = attachments_root.exists()  # Проверяем наличие папки вложений
        attachments_size = 0  # Инициализируем суммарный размер вложений
        if attachments_exists:  # Если папка существует
            for root_dir, _dirs, files in os.walk(attachments_root):  # Обходим все подпапки и файлы
                for filename in files:  # Перебираем файлы
                    try:  # Пытаемся прочитать размер
                        file_path = Path(root_dir) / filename  # Формируем путь до файла
                        attachments_size += file_path.stat().st_size  # Добавляем размер файла к сумме
                    except Exception:  # При ошибке чтения пропускаем файл
                        continue  # Продолжаем обход без падения
        sticker_cache_root = STICKER_CACHE_DIR  # Папка кэша стикеров
        sticker_cache_exists = sticker_cache_root.exists()  # Проверяем, создан ли кэш
        sticker_cache_size = 0  # Инициализируем суммарный размер кэша стикеров
        if sticker_cache_exists:  # Если папка кэша существует
            for root_dir, _dirs, files in os.walk(sticker_cache_root):  # Обходим кэш по подпапкам
                for filename in files:  # Перебираем файлы кэша
                    try:  # Пробуем прочитать размер файла
                        file_path = Path(root_dir) / filename  # Получаем путь до файла кэша
                        sticker_cache_size += file_path.stat().st_size  # Добавляем размер к общему объему кэша
                    except Exception:  # Если чтение размера не удалось
                        continue  # Пропускаем файл, не прерывая суммирование
        return {  # Возвращаем объединенную информацию
            **db_storage,  # Данные по файлу базы
            "attachments_path": str(attachments_root),  # Путь до папки вложений
            "attachments_exists": attachments_exists,  # Флаг существования вложений
            "attachments_size_bytes": attachments_size,  # Суммарный размер вложений в байтах
            "sticker_cache_path": str(sticker_cache_root),  # Путь к кэшу стикеров
            "sticker_cache_exists": sticker_cache_exists,  # Флаг существования кэша стикеров
            "sticker_cache_size_bytes": sticker_cache_size,  # Суммарный размер кэша стикеров
        }

    def localize_iso(timestamp: Optional[str]) -> Optional[str]:
        try:  # Пытаемся преобразовать ISO-строку
            parsed = datetime.fromisoformat(timestamp) if timestamp else None  # Парсим дату с таймзоной
            return parsed.astimezone().isoformat() if parsed else None  # Конвертируем в локальное время и возвращаем ISO
        except Exception:  # Обрабатываем неверный формат строки
            return None  # Возвращаем None при ошибке

    def build_chat_payload(peer_id: int, limit: int = MESSAGES_PAGE_SIZE, offset: int = 0) -> Dict[str, object]:
        summary = event_logger.summarize_peer(peer_id)  # Получаем сводку по чату из базы
        if summary and summary.get("last_message_time"):  # Проверяем наличие временной метки
            summary["last_message_time"] = localize_iso(summary.get("last_message_time"))  # Переводим время в локальную зону
        if summary is not None:  # Убеждаемся, что словарь сводки существует
            summary.setdefault("peer_id", peer_id)  # Добавляем ID чата для единообразия в шаблоне
            summary.setdefault("from_id", None)  # Явно прописываем пустой from_id, чтобы избежать undefined в JavaScript
        messages = [
            serialize_log(row)
            for row in event_logger.fetch_messages(peer_id=peer_id, limit=limit, offset=offset)
        ]  # Получаем логи по чату с пагинацией
        return {"summary": summary, "messages": messages}  # Возвращаем словарь с данными страницы

    def build_user_payload(
        user_id: int, limit: int = MESSAGES_PAGE_SIZE, offset: int = 0, peer_id: Optional[int] = None
    ) -> Dict[str, object]:
        summary = event_logger.summarize_user(user_id)  # Получаем сводку по отправителю
        if summary and summary.get("last_message_time"):  # Проверяем, есть ли время последнего сообщения
            summary["last_message_time"] = localize_iso(summary.get("last_message_time"))  # Конвертируем время в локальную зону
        if summary is not None:  # Убеждаемся, что сводка существует
            summary.setdefault("from_id", user_id)  # Явно прописываем ID отправителя для фронтенда
            summary.setdefault("peer_id", peer_id)  # Добавляем текущий peer_id (или None), чтобы шаблон не получал undefined
        messages = [
            serialize_log(row)
            for row in event_logger.fetch_messages_by_user(user_id=user_id, limit=limit, peer_id=peer_id, offset=offset)
        ]  # Получаем логи пользователя с пагинацией
        return {"summary": summary, "messages": messages}  # Возвращаем словарь с данными страницы

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

    def pick_sticker_url_for_history(attachment: Dict) -> Optional[str]:  # Выбирает лучшую ссылку изображения стикера для бэкапа
        sticker_block = attachment.get("sticker") if isinstance(attachment.get("sticker"), dict) else {}  # Забираем блок стикера
        if not sticker_block:  # Проверяем, что блок стикера найден
            return None  # Возвращаем пустое значение при отсутствии данных
        images: List[Dict] = []  # Готовим список вариантов изображений
        raw_images = sticker_block.get("images") if isinstance(sticker_block.get("images"), list) else []  # Извлекаем базовые изображения
        raw_bg = sticker_block.get("images_with_background") if isinstance(sticker_block.get("images_with_background"), list) else []  # Извлекаем изображения с фоном
        images.extend(raw_images)  # Добавляем базовые изображения в общий список
        images.extend(raw_bg)  # Добавляем варианты с фоном в общий список
        if not images:  # Проверяем, что список не пустой
            return None  # Возвращаем пустое значение, если ссылок нет
        sorted_images = sorted(  # Сортируем варианты по площади
            images,  # Список изображений
            key=lambda item: (item.get("width", 0) or 0) * (item.get("height", 0) or 0),  # Ключ сортировки по площади
            reverse=True,  # Сортируем по убыванию площади
        )  # Завершаем сортировку
        best = sorted_images[0] if sorted_images else None  # Берем лучший вариант
        return best.get("url") if isinstance(best, dict) else None  # Возвращаем ссылку лучшего изображения

    def get_cached_sticker_file(sticker_id: Optional[int]) -> Optional[Path]:  # Ищет готовый файл стикера в кэше
        if not isinstance(sticker_id, int):  # Проверяем корректность ID
            return None  # Возвращаем пустое значение при неверном ID
        for candidate in STICKER_CACHE_DIR.glob(f"sticker_{sticker_id}.*"):  # Перебираем подходящие файлы
            if candidate.exists():  # Проверяем, что файл существует
                return candidate  # Возвращаем найденный путь
        return None  # Возвращаем пустое значение, если файла нет

    def sticker_fallback_urls(sticker_id: Optional[int]) -> List[str]:  # Формирует обходные ссылки VK для скачивания стикера
        if not isinstance(sticker_id, int):  # Проверяем корректность ID
            return []  # Возвращаем пустой список при ошибке
        urls: List[str] = []  # Подготавливаем список ссылок
        for size in (512, 256, 128):  # Перебираем набор размеров
            urls.append(f"https://vk.com/sticker/{sticker_id}-{size}-{size}-0")  # Добавляем ссылку без фона
            urls.append(f"https://vk.com/sticker/{sticker_id}-{size}-{size}-1")  # Добавляем ссылку с фоном
        return urls  # Возвращаем список кандидатов

    def build_sticker_cache_path(sticker_id: int, source_url: Optional[str]) -> Path:  # Строит путь сохранения файла стикера
        parsed = urlparse(source_url or "")  # Парсим исходный URL, даже если он пустой
        suffix = Path(parsed.path).suffix  # Выбираем расширение файла из пути
        safe_suffix = suffix if suffix else ".webp"  # Используем WebP по умолчанию
        filename = f"sticker_{sticker_id}{safe_suffix}"  # Формируем имя файла кэша
        return STICKER_CACHE_DIR / filename  # Возвращаем путь внутри кэша

    def download_sticker_to_cache(sticker_id: Optional[int], primary_url: Optional[str]) -> tuple[Optional[Path], Optional[str]]:  # Скачивает стикер в кэш по ID или ссылке
        if not isinstance(sticker_id, int):  # Проверяем корректность ID
            return None, "Стикер без sticker_id нельзя восстановить из истории"  # Возвращаем ошибку при неверном ID
        cached = get_cached_sticker_file(sticker_id)  # Пробуем найти готовый кэш
        if cached:  # Проверяем наличие файла
            return cached, None  # Возвращаем путь без ошибки
        candidates: List[str] = []  # Формируем список кандидатов ссылок
        if primary_url:  # Проверяем, что есть исходная ссылка
            candidates.append(primary_url)  # Добавляем исходный URL в начало списка
        candidates.extend(sticker_fallback_urls(sticker_id))  # Добавляем обходные ссылки VK
        last_error = None  # Подготавливаем переменную для текста ошибки
        for candidate in candidates:  # Перебираем все ссылки для попыток
            target_path = build_sticker_cache_path(sticker_id, candidate)  # Формируем путь для сохранения файла
            try:  # Пытаемся скачать файл по ссылке
                response = requests.get(candidate, timeout=30, stream=True)  # Выполняем запрос с таймаутом и потоком
                status_code = getattr(response, "status_code", 0) or 0  # Получаем код ответа
                response.raise_for_status()  # Бросаем исключение при неуспешном статусе
                with target_path.open("wb") as handle:  # Открываем файл для записи
                    for chunk in response.iter_content(chunk_size=8192):  # Читаем ответ блоками
                        if not chunk:  # Пропускаем пустые блоки
                            continue  # Переходим к следующему блоку
                        handle.write(chunk)  # Записываем блок в файл
                return target_path, None  # Возвращаем путь при успехе
            except requests.HTTPError as exc:  # Обрабатываем HTTP-ошибку
                resp = exc.response  # Достаём ответ сервера
                code = resp.status_code if resp is not None else status_code  # Берём код ответа или последний код
                reason = resp.reason if resp is not None else str(exc)  # Берём пояснение ошибки
                last_error = f"HTTP {code}: {reason}"  # Формируем текст ошибки
                log_service_event(code or 500, f"Не удалось скачать стикер {sticker_id} по {candidate}: {last_error}")  # Пишем событие в сервисные логи
            except Exception as exc:  # Обрабатываем прочие ошибки
                last_error = str(exc)  # Сохраняем текст исключения
                log_service_event(500, f"Сбой скачивания стикера {sticker_id} по {candidate}: {last_error}")  # Логируем ошибку в сервисные логи
        return None, last_error or "Не найдено ни одной ссылки для скачивания стикера"  # Возвращаем ошибку, если ничего не скачалось

    def enrich_attachments_list(attachments: object) -> List[Dict]:  # Добавляет публичные ссылки и нормализует вложения
        enriched: List[Dict] = []  # Готовим список нормализованных вложений
        if not isinstance(attachments, list):  # Проверяем, что входной объект — список
            return enriched  # Возвращаем пустой список при некорректном формате
        seen_signatures: set[str] = set()  # Подготавливаем множество сигнатур, чтобы не дублировать вложения
        for raw in attachments:  # Перебираем все вложения
            if not isinstance(raw, dict):  # Проверяем тип элемента
                continue  # Пропускаем элементы неправильного формата
            item = dict(raw)  # Делаем копию вложения
            signature = None  # Инициализируем переменную для уникальной сигнатуры
            try:  # Оборачиваем расчёт сигнатуры, чтобы не упасть на неожиданных данных
                if item.get("type") == "sticker":  # Проверяем, что перед нами стикер
                    sticker_block = item.get("sticker") if isinstance(item.get("sticker"), dict) else {}  # Извлекаем блок стикера
                    sticker_id = sticker_block.get("sticker_id") if isinstance(sticker_block, dict) else None  # Читаем sticker_id
                    product_id = sticker_block.get("product_id") if isinstance(sticker_block, dict) else None  # Читаем product_id
                    if sticker_id is not None:  # Проверяем наличие sticker_id
                        signature = f"sticker:{sticker_id}:{product_id or ''}"  # Формируем сигнатуру стикера по его ID
                if signature is None:  # Если сигнатура пока не рассчитана
                    nested_type = item.get("type")  # Читаем тип вложения
                    nested_block = item.get(nested_type) if isinstance(nested_type, str) else None  # Забираем вложенный блок по типу
                    nested_obj = nested_block if isinstance(nested_block, dict) else {}  # Нормализуем вложенный блок
                    owner_id = nested_obj.get("owner_id")  # Читаем owner_id при наличии
                    item_id = nested_obj.get("id")  # Читаем id вложения при наличии
                    access_key = nested_obj.get("access_key")  # Читаем access_key при наличии
                    if owner_id is not None and item_id is not None:  # Проверяем, что есть уникальные идентификаторы VK
                        signature = f"{nested_type}:{owner_id}_{item_id}_{access_key or ''}"  # Формируем сигнатуру по типу и ID
                if signature is None:  # Если уникальные ID не нашлись
                    url = item.get("download_url") or item.get("url")  # Пробуем использовать ссылку вложения
                    if url:  # Проверяем, что ссылка существует
                        signature = f"{item.get('type') or 'file'}:{url}"  # Строим сигнатуру по типу и ссылке
                if signature is None:  # Если других вариантов нет
                    signature = json.dumps(item, sort_keys=True, ensure_ascii=False)  # Фолбэк: сериализуем вложение целиком
            except Exception:  # Ловим любые ошибки при расчёте сигнатуры
                signature = None  # Сбрасываем сигнатуру при сбое
            if signature and signature in seen_signatures:  # Проверяем, не встречалось ли вложение раньше
                continue  # Пропускаем дубликат
            if signature:  # Если сигнатура рассчитана успешно
                seen_signatures.add(signature)  # Добавляем её в множество, чтобы отсечь повторы
            if item.get("type") == "sticker":  # Дополнительно обрабатываем стикеры при чтении истории
                sticker_block = item.get("sticker") if isinstance(item.get("sticker"), dict) else {}  # Забираем блок стикера
                sticker_id = sticker_block.get("sticker_id") if isinstance(sticker_block, dict) else None  # Читаем sticker_id
                cached_path = get_cached_sticker_file(sticker_id)  # Ищем готовый файл в кэше
                if not cached_path:  # Проверяем, что файл не найден
                    primary_url = item.get("download_url") or item.get("url") or pick_sticker_url_for_history(item)  # Подбираем исходную ссылку
                    cached_path, cache_error = download_sticker_to_cache(sticker_id, primary_url)  # Пытаемся скачать и закэшировать стикер
                    if cache_error:  # Проверяем наличие ошибки скачивания
                        item["download_error"] = item.get("download_error") or cache_error  # Сохраняем ошибку в вложении
                if cached_path:  # Проверяем, что путь к файлу появился
                    item["local_path"] = str(cached_path)  # Записываем путь к файлу в вложение
                    item["download_state"] = item.get("download_state") or "ready"  # Помечаем успешное состояние
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
        payload_text = row.get("payload") or "{}"  # Берем сырой payload или пустой JSON
        try:  # Пытаемся распарсить payload
            raw_payload = json.loads(payload_text)  # Преобразуем текст в словарь
        except Exception:  # При ошибке парсинга
            raw_payload = {}  # Возвращаем пустой словарь, чтобы не ронять страницу
        reply_payload = raw_payload.get("reply_message") if isinstance(raw_payload, dict) else None  # Получаем блок ответа из payload
        deleted_flag = False  # Флаг, указывающий, что сообщение было удалено пользователем или системой
        if isinstance(raw_payload, dict):  # Проверяем, что payload представлен словарем
            deletion_candidates = [  # Формируем список полей, которые могут означать удаление сообщения
                raw_payload.get("deleted"),  # Поле deleted из VK API
                raw_payload.get("was_deleted"),  # Альтернативное поле was_deleted
                raw_payload.get("is_deleted"),  # Возможное поле is_deleted из других источников
            ]  # Завершаем список кандидатов на признак удаления
            deleted_flag = any(bool(value) for value in deletion_candidates)  # Вычисляем, установлен ли хотя бы один признак удаления
            action_block = raw_payload.get("action") if isinstance(raw_payload.get("action"), dict) else {}  # Извлекаем блок action при наличии
            action_type = action_block.get("type") if isinstance(action_block, dict) else None  # Читаем тип действия из блока action
            if action_type in ("chat_message_delete", "message_delete"):  # Проверяем, относится ли действие к удалению сообщения
                deleted_flag = True  # Фиксируем, что сообщение нужно считать удаленным
        reply_attachments_raw = row.get("reply_message_attachments") or "[]"  # Берем текст вложений ответа или пустой список
        try:  # Пытаемся распарсить вложения ответа
            reply_attachments = enrich_attachments_list(json.loads(reply_attachments_raw))  # Преобразуем вложения в структурированный список
        except Exception:  # При ошибке парсинга вложений
            reply_attachments = []  # Используем пустой список, чтобы не ронять страницу
        reply = {  # Готовим словарь ответа
            "id": row.get("reply_message_id"),  # ID исходного сообщения
            "text": row.get("reply_message_text"),  # Текст исходного сообщения
            "attachments": reply_attachments,  # Вложения исходного сообщения с публичными ссылками
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
        attachments_raw = row.get("attachments") or "[]"  # Берем строку вложений или пустой список
        try:  # Пытаемся распарсить вложения
            attachments = enrich_attachments_list(json.loads(attachments_raw))  # Подготавливаем вложения с публичными ссылками
        except Exception:  # При ошибке разбора вложений
            attachments = []  # Используем пустой список, чтобы не ломать страницу профиля

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
            "is_deleted": deleted_flag,  # Флаг, что сообщение удалено и должно подсвечиваться
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
            initial_logs=[serialize_log(row) for row in event_logger.fetch_messages(limit=MESSAGES_PAGE_SIZE, offset=0)],  # Стартовый список логов для главной страницы
            page_size=MESSAGES_PAGE_SIZE,  # Размер страницы для бесконечной ленты сообщений
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

    @app.route("/chat/<int:peer_id>")
    def chat_page(peer_id: int):
        payload = build_chat_payload(peer_id, limit=MESSAGES_PAGE_SIZE)  # Собираем данные чата с базовым размером страницы
        if not payload.get("summary"):  # Проверяем, удалось ли найти чат
            log_service_event(404, f"Чат {peer_id} не найден для страницы профиля")  # Фиксируем отсутствие данных
            return "Чат не найден", 404  # Возвращаем 404
        log_service_event(200, f"Отдаём страницу профиля чата {peer_id}")  # Фиксируем успешную отдачу страницы
        return render_template(
            "entity.html",  # Шаблон страницы профиля
            entity_type="chat",  # Тип сущности — чат
            payload=payload,  # Данные профиля и сообщений
            page_size=MESSAGES_PAGE_SIZE,  # Размер страницы для подгрузки сообщений
            demo_mode=demo_mode,  # Флаг демо-режима
        )  # Возвращаем HTML страницы

    @app.route("/user/<int:user_id>")
    def user_page(user_id: int):
        payload = build_user_payload(user_id, limit=MESSAGES_PAGE_SIZE)  # Собираем данные пользователя и его сообщения
        if not payload.get("summary"):  # Проверяем, удалось ли найти пользователя
            log_service_event(404, f"Пользователь {user_id} не найден для страницы профиля")  # Пишем в сервисные логи
            return "Пользователь не найден", 404  # Возвращаем 404
        log_service_event(200, f"Отдаём страницу профиля пользователя {user_id}")  # Фиксируем отдачу страницы
        return render_template(
            "entity.html",  # Шаблон страницы профиля
            entity_type="user",  # Тип сущности — пользователь
            payload=payload,  # Данные профиля и сообщений
            page_size=MESSAGES_PAGE_SIZE,  # Размер страницы для подгрузки сообщений
            demo_mode=demo_mode,  # Флаг демо-режима
        )  # Возвращаем HTML страницы

    @app.route("/api/logs")
    def logs():
        peer_id_raw = request.args.get("peer_id")  # Читаем peer_id из запроса
        limit_raw = request.args.get("limit")  # Читаем лимит из запроса
        offset_raw = request.args.get("offset")  # Читаем смещение из запроса
        from_id_raw = request.args.get("from_id")  # Читаем фильтр по отправителю
        peer_id = int(peer_id_raw) if peer_id_raw else None  # Преобразуем в число при наличии
        from_id = int(from_id_raw) if from_id_raw else None  # Преобразуем отправителя при наличии
        limit = int(limit_raw) if limit_raw else MESSAGES_PAGE_SIZE  # Устанавливаем лимит выборки
        limit = max(1, min(limit, 500))  # Ограничиваем диапазон лимита на одну подгрузку
        offset = int(offset_raw) if offset_raw else 0  # Читаем смещение
        offset = max(0, offset)  # Страхуем от отрицательного значения
        messages = [
            serialize_log(row)
            for row in event_logger.fetch_messages(peer_id=peer_id, limit=limit, offset=offset, from_id=from_id)
        ]  # Запрашиваем логи
        log_service_event(
            200,
            f"Отдаём JSON с логами peer_id={peer_id} from_id={from_id} лимитом {limit} смещением {offset}",
        )  # Логируем успешную отдачу логов
        return jsonify({"items": messages, "peer_id": peer_id, "offset": offset, "from_id": from_id})  # Возвращаем JSON с логами

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
        logs_payload = [
            serialize_log(row)
            for row in event_logger.fetch_messages(peer_id=peer_id, limit=MESSAGES_PAGE_SIZE, offset=0)
        ]  # Получаем стартовый список логов
        service_logs_payload = [serialize_service_event(row) for row in service_events.fetch_events(limit=50)]  # Получаем стартовые сервисные логи
        log_service_event(200, f"Отдаём HTML со всеми логами peer_id={peer_id} без общего лимита")  # Фиксируем выдачу страницы логов
        return render_template(
            "logs.html",  # Шаблон страницы логов
            initial_logs=logs_payload,  # Начальный список логов
            initial_peers=event_logger.list_peers(),  # Доступные чаты для фильтрации
            initial_peer_id=peer_id,  # Текущий выбранный чат
            initial_page_size=MESSAGES_PAGE_SIZE,  # Размер страницы для подгрузки
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
