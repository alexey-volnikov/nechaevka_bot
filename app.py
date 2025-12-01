import json  # Сериализация объектов в JSON для шаблона
import logging  # Настройка логирования событий приложения
import os  # Работа с переменными окружения
import sqlite3  # Работа с базой SQLite для логов
import threading  # Запуск фонового потока лонгпулла
from dataclasses import dataclass, field  # Упрощенное объявление классов состояния
from datetime import datetime  # Фиксация времени событий для графиков
from typing import Dict, List, Optional  # Подсказки типов для словарей и списков

from logging.handlers import RotatingFileHandler  # Обработчик логов с ротацией файлов

from dotenv import load_dotenv  # Загрузка переменных окружения из .env
from flask import Flask, jsonify, render_template, request  # Веб-сервер, рендер и разбор запросов
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
            cursor.execute(  # Создаем таблицу при отсутствии
                """
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    peer_id INTEGER,
                    peer_title TEXT,
                    from_id INTEGER,
                    from_name TEXT,
                    message_id INTEGER,
                    reply_to INTEGER,
                    is_bot INTEGER DEFAULT 0,
                    text TEXT,
                    attachments TEXT,
                    payload TEXT
                )
                """
            )
            cursor.execute("PRAGMA table_info(events)")  # Читаем описание колонок для миграции
            columns = {row[1] for row in cursor.fetchall()}  # Собираем имена колонок в множество
            if "is_bot" not in columns:  # Если колонки для флага бота нет
                cursor.execute("ALTER TABLE events ADD COLUMN is_bot INTEGER DEFAULT 0")  # Добавляем колонку миграцией
            if "peer_title" not in columns:  # Если нет колонки для названия чата
                cursor.execute("ALTER TABLE events ADD COLUMN peer_title TEXT")  # Добавляем поле для названия чата
            if "from_name" not in columns:  # Если нет колонки для имени автора
                cursor.execute("ALTER TABLE events ADD COLUMN from_name TEXT")  # Добавляем поле для имени отправителя
            self._connection.commit()  # Сохраняем изменения

    def describe_storage(self) -> Dict[str, object]:
        return {
            "path": self.db_path,  # Путь до файла базы
            "exists": os.path.exists(self.db_path),  # Флаг существования файла
            "size_bytes": os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0,  # Размер файла в байтах
        }  # Словарь с описанием хранилища

    def log_event(self, event_type: str, payload: Dict, peer_title: Optional[str] = None, from_name: Optional[str] = None) -> None:
        created_at = datetime.now().astimezone().isoformat()  # Фиксируем локальное время вставки с таймзоной
        peer_id = payload.get("peer_id")  # Берем ID чата
        from_id = payload.get("from_id")  # Берем автора
        message_id = payload.get("id")  # Берем ID сообщения
        reply_to = payload.get("reply_message", {}).get("from_id") if isinstance(payload.get("reply_message"), dict) else None  # Берем ID адресата ответа
        text = payload.get("text")  # Берем текст
        attachments = payload.get("attachments", [])  # Берем вложения
        is_bot = 1 if isinstance(from_id, int) and from_id < 0 else 0  # Фиксируем, что автор — бот или сообщество
        with self._lock:  # Начинаем потокобезопасную запись
            cursor = self._connection.cursor()  # Получаем курсор
            cursor.execute(  # Выполняем вставку строки
                """
                INSERT INTO events (created_at, event_type, peer_id, peer_title, from_id, from_name, message_id, reply_to, is_bot, text, attachments, payload)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    created_at,  # Время вставки
                    event_type,  # Тип события
                    peer_id,  # Чат
                    peer_title,  # Название чата
                    from_id,  # Автор
                    from_name,  # Имя автора
                    message_id,  # ID сообщения
                    reply_to,  # Кому отвечали
                    is_bot,  # Флаг автора-бота
                    text,  # Текст
                    json.dumps(attachments, ensure_ascii=False),  # Сериализуем вложения
                    json.dumps(payload, ensure_ascii=False),  # Сохраняем сырой payload
                ),
            )
            self._connection.commit()  # Сохраняем изменения

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
            cursor.execute("SELECT DISTINCT peer_id, peer_title FROM events WHERE peer_id IS NOT NULL ORDER BY peer_id")  # Запрос уникальных чатов с названиями
            rows = cursor.fetchall()  # Читаем строки
        return [  # Возвращаем список словарей с ID и названием
            {"id": row["peer_id"], "title": row["peer_title"]}  # Словарь с ID и названием
            for row in rows  # Перебираем строки результата
            if row["peer_id"] is not None  # Фильтруем пустые значения
        ]


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
        self.user_cache: Dict[int, str] = {}  # Кэш имен пользователей для уменьшения запросов
        self.group_cache: Dict[int, str] = {}  # Кэш названий сообществ
        self.peer_cache: Dict[int, str] = {}  # Кэш названий чатов по peer_id

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
                        sender_name = self._resolve_sender_name(message.get("from_id"))  # Находим имя отправителя
                        peer_title = self._resolve_peer_title(message.get("peer_id"), sender_name)  # Находим название чата
                        payload = {  # Собираем полезные данные для метрик
                            "id": message.get("id"),  # ID сообщения
                            "from_id": message.get("from_id"),  # ID отправителя
                            "from_name": sender_name,  # Имя отправителя
                            "peer_id": message.get("peer_id"),  # Диалог или чат
                            "peer_title": peer_title,  # Название чата
                            "text": message.get("text"),  # Текст сообщения
                            "attachments": message.get("attachments", []),  # Список вложений
                            "reply_message": message.get("reply_message"),  # Ответ, если есть
                        }  # Конец сборки payload
                        self.state.mark_event(payload, "message")  # Фиксируем событие в состоянии
                        self.event_logger.log_event("message", message, peer_title=peer_title, from_name=sender_name)  # Записываем исходный payload с именами в базу
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

    def _resolve_sender_name(self, from_id: Optional[int]) -> Optional[str]:
        if not isinstance(from_id, int):  # Если ID некорректный
            return None  # Возвращаем пустое значение
        if from_id in self.user_cache:  # Проверяем кэш пользователей
            return self.user_cache[from_id]  # Возвращаем сохраненное имя
        if from_id in self.group_cache:  # Проверяем кэш групп
            return self.group_cache[from_id]  # Возвращаем сохраненное название
        try:  # Пробуем выполнить запрос
            if from_id > 0:  # Если это пользователь
                response = self.session.method("users.get", {"user_ids": from_id})  # Запрашиваем имя пользователя
                if response:  # Если ответ не пустой
                    user = response[0]  # Берем первую запись
                    name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()  # Формируем имя
                    self.user_cache[from_id] = name  # Кэшируем имя
                    return name  # Возвращаем имя
            else:  # Если это сообщество
                response = self.session.method("groups.getById", {"group_id": abs(from_id)})  # Запрашиваем название сообщества
                if response:  # Если ответ есть
                    group = response[0]  # Берем первую запись
                    name = group.get("name")  # Достаем имя
                    self.group_cache[from_id] = name or ""  # Кэшируем название
                    return name  # Возвращаем название
        except Exception as exc:  # Обрабатываем ошибки VK API
            logger.debug("Не удалось получить имя отправителя %s: %s", from_id, exc)  # Пишем отладочный лог
        return None  # Возвращаем None при неудаче

    def _resolve_peer_title(self, peer_id: Optional[int], fallback: Optional[str]) -> Optional[str]:
        if not isinstance(peer_id, int):  # Если peer_id не число
            return fallback  # Возвращаем запасной текст
        if peer_id in self.peer_cache:  # Проверяем кэш чатов
            return self.peer_cache[peer_id]  # Возвращаем сохраненное название
        try:  # Пробуем запросить название
            if peer_id >= 2000000000:  # Если это беседа
                response = self.session.method("messages.getConversationsById", {"peer_ids": peer_id})  # Запрашиваем данные чата
                items = response.get("items", []) if isinstance(response, dict) else []  # Получаем список чатов
                if items:  # Если список не пуст
                    title = items[0].get("chat_settings", {}).get("title")  # Достаем название беседы
                    if title:  # Если название найдено
                        self.peer_cache[peer_id] = title  # Кэшируем
                        return title  # Возвращаем название
            elif peer_id > 0:  # Если это личный диалог с пользователем
                name = self._resolve_sender_name(peer_id)  # Используем имя пользователя
                if name:  # Если имя найдено
                    self.peer_cache[peer_id] = name  # Кэшируем
                    return name  # Возвращаем имя
            else:  # Если peer_id отрицательный (сообщество)
                name = self._resolve_sender_name(peer_id)  # Получаем название сообщества
                if name:  # Если нашли
                    self.peer_cache[peer_id] = name  # Кэшируем
                    return name  # Возвращаем название
        except Exception as exc:  # Обрабатываем ошибки запроса
            logger.debug("Не удалось получить название чата %s: %s", peer_id, exc)  # Пишем отладку
        return fallback  # Возвращаем запасной текст

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
        {"group_id": group_id, "fields": "description,contacts,members_count"},  # Поля, которые запрашиваем
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
        {"id": 1, "from_id": 111, "from_name": "Иван Иванов", "peer_id": 1, "peer_title": "Демо-диалог", "text": "Первое демо-сообщение", "attachments": []},  # Сообщение 1
        {"id": 2, "from_id": 222, "from_name": "Мария Петрова", "peer_id": 2, "peer_title": "Демо-чат", "text": "Еще одно демо", "attachments": []},  # Сообщение 2
    ]  # Конец списка демо-сообщений
    for message in demo_messages:  # Перебираем демо-сообщения
        state.mark_event(message, "message")  # Обновляем метрики для демо
        event_logger.log_event("message", message, peer_title=message.get("peer_title"), from_name=message.get("from_name"))  # Записываем демо в базу с именами
    state.mark_event({}, "invite")  # Добавляем демо-событие приглашения
    group_info = {
        "name": "Демо-сообщество",  # Название сообщества
        "description": "Образец данных без подключения к VK",  # Описание сообщества
        "members_count": 1234,  # Число участников
        "screen_name": "club_demo",  # Короткий адрес
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
            if peer_row.get("title"):  # Если известно название
                chat_settings = entry.setdefault("chat_settings", {})  # Берем блок настроек беседы
                chat_settings.setdefault("title", peer_row.get("title"))  # Устанавливаем название, не затирая существующее
            combined[peer_id] = entry  # Обновляем словарь
        return list(combined.values())  # Возвращаем объединенный список

    def assemble_conversations() -> List[Dict]:
        peers_from_logs = event_logger.list_peers()  # Получаем чаты из базы
        return merge_conversations(conversations, peers_from_logs)  # Объединяем стартовые диалоги с теми, что накопились в логах

    def assemble_stats() -> Dict[str, object]:
        return {
            "events": state.total_events,  # Общее количество событий
            "messages": state.new_messages,  # Количество сообщений
            "invites": state.invites,  # Количество приглашений/удалений
            "errors": state.errors,  # Количество ошибок
            "last_messages": state.last_messages,  # История последних сообщений
            "timeline": state.events_timeline,  # Точки для графиков
        }  # Словарь статистики

    def assemble_storage() -> Dict[str, object]:
        return event_logger.describe_storage()  # Возвращаем информацию о файле базы

    def localize_iso(timestamp: Optional[str]) -> Optional[str]:
        try:  # Пытаемся преобразовать ISO-строку
            parsed = datetime.fromisoformat(timestamp) if timestamp else None  # Парсим дату с таймзоной
            return parsed.astimezone().isoformat() if parsed else None  # Конвертируем в локальное время и возвращаем ISO
        except Exception:  # Обрабатываем неверный формат строки
            return None  # Возвращаем None при ошибке

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
        return {
            "id": row.get("id"),  # ID записи
            "created_at": localize_iso(row.get("created_at")),  # Локальное время создания в ISO-формате
            "event_type": row.get("event_type"),  # Тип события
            "peer_id": row.get("peer_id"),  # ID чата
            "peer_title": row.get("peer_title"),  # Название чата
            "from_id": row.get("from_id"),  # Автор
            "from_name": row.get("from_name"),  # Имя автора
            "message_id": row.get("message_id"),  # ID сообщения VK
            "reply_to": row.get("reply_to"),  # Кому ответили
            "is_bot": row.get("is_bot", 0),  # Флаг, что автор — бот или сообщество
            "text": row.get("text"),  # Текст
            "attachments": json.loads(row.get("attachments") or "[]"),  # Вложения
            "payload": json.loads(row.get("payload") or "{}"),  # Сырой payload
        }  # Конец словаря лога

    @app.route("/")
    def index():
        log_service_event(200, "Отдаём главную страницу дашборда")  # Фиксируем успешную отдачу главной страницы
        return render_template(
            "index.html",  # Шаблон дашборда
            initial_group=group_info,  # Передаем словарь с данными сообщества без лишней сериализации
            initial_conversations=assemble_conversations(),  # Список диалогов с учетом базы
            initial_stats=assemble_stats(),  # Начальные метрики состояния без двойного JSON
            initial_peers=event_logger.list_peers(),  # Доступные peer_id из базы
            initial_storage=assemble_storage(),  # Описание файла базы для подсказки
            demo_mode=demo_mode,  # Флаг демо для вывода на страницу
        )  # Возвращаем HTML страницу

    @app.route("/api/stats")
    def stats():
        log_service_event(200, "Отдаём JSON со статистикой событий")  # Фиксируем успешную выдачу статистики
        return jsonify(assemble_stats())  # Возвращаем актуальную статистику в JSON

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
