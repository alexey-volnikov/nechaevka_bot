import json  # Сериализация объектов в JSON для шаблона
import logging  # Настройка логирования событий приложения
import os  # Работа с переменными окружения
import sqlite3  # Работа с базой SQLite для логов
import threading  # Запуск фонового потока лонгпулла
from dataclasses import dataclass, field  # Упрощенное объявление классов состояния
from datetime import datetime  # Фиксация времени событий для графиков
from typing import Dict, List, Optional  # Подсказки типов для словарей и списков

from dotenv import load_dotenv  # Загрузка переменных окружения из .env
from flask import Flask, jsonify, render_template, request  # Веб-сервер, рендер и разбор запросов
import vk_api  # Клиент VK API
from vk_api.bot_longpoll import VkBotEventType, VkBotLongPoll  # Лонгпулл сообщества для чтения событий

load_dotenv()  # Инициализируем загрузку переменных окружения при старте скрипта

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")  # Формат логов
logger = logging.getLogger(__name__)  # Получаем логгер для текущего модуля


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
        timestamp = datetime.utcnow().strftime("%H:%M:%S")  # Фиксируем время события в UTC
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
                    from_id INTEGER,
                    message_id INTEGER,
                    reply_to INTEGER,
                    text TEXT,
                    attachments TEXT,
                    payload TEXT
                )
                """
            )
            self._connection.commit()  # Сохраняем изменения

    def log_event(self, event_type: str, payload: Dict) -> None:
        created_at = datetime.utcnow().isoformat()  # Фиксируем время вставки
        peer_id = payload.get("peer_id")  # Берем ID чата
        from_id = payload.get("from_id")  # Берем автора
        message_id = payload.get("id")  # Берем ID сообщения
        reply_to = payload.get("reply_message", {}).get("from_id") if isinstance(payload.get("reply_message"), dict) else None  # Берем ID адресата ответа
        text = payload.get("text")  # Берем текст
        attachments = payload.get("attachments", [])  # Берем вложения
        with self._lock:  # Начинаем потокобезопасную запись
            cursor = self._connection.cursor()  # Получаем курсор
            cursor.execute(  # Выполняем вставку строки
                """
                INSERT INTO events (created_at, event_type, peer_id, from_id, message_id, reply_to, text, attachments, payload)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    created_at,  # Время вставки
                    event_type,  # Тип события
                    peer_id,  # Чат
                    from_id,  # Автор
                    message_id,  # ID сообщения
                    reply_to,  # Кому отвечали
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

    def list_peers(self) -> List[int]:
        with self._lock:  # Начинаем безопасное чтение
            cursor = self._connection.cursor()  # Берем курсор
            cursor.execute("SELECT DISTINCT peer_id FROM events WHERE peer_id IS NOT NULL ORDER BY peer_id")  # Запрос уникальных чатов
            rows = cursor.fetchall()  # Читаем строки
        return [row[0] for row in rows if row[0] is not None]  # Возвращаем список ID


class BotMonitor:
    """Фоновый монитор лонгпулла без отправки сообщений."""

    def __init__(self, token: str, group_id: int, state: BotState, event_logger: EventLogger):
        self.state = state  # Запоминаем объект состояния для обновлений
        self.token = token  # Токен сообщества
        self.group_id = group_id  # ID сообщества
        self.session = vk_api.VkApi(token=self.token)  # Сессия VK API для запросов
        self._stop_event = threading.Event()  # Флаг корректной остановки потока
        self.event_logger = event_logger  # Объект записи логов

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
                        payload = {  # Собираем полезные данные для метрик
                            "id": message.get("id"),  # ID сообщения
                            "from_id": message.get("from_id"),  # ID отправителя
                            "peer_id": message.get("peer_id"),  # Диалог или чат
                            "text": message.get("text"),  # Текст сообщения
                            "attachments": message.get("attachments", []),  # Список вложений
                            "reply_message": message.get("reply_message"),  # Ответ, если есть
                        }  # Конец сборки payload
                        self.state.mark_event(payload, "message")  # Фиксируем событие в состоянии
                        self.event_logger.log_event("message", message)  # Записываем исходный payload в базу
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

    def stop(self) -> None:
        self._stop_event.set()  # Устанавливаем флаг остановки потока


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
        {"id": 1, "from_id": 111, "peer_id": 1, "text": "Первое демо-сообщение", "attachments": []},  # Сообщение 1
        {"id": 2, "from_id": 222, "peer_id": 2, "text": "Еще одно демо", "attachments": []},  # Сообщение 2
    ]  # Конец списка демо-сообщений
    for message in demo_messages:  # Перебираем демо-сообщения
        state.mark_event(message, "message")  # Обновляем метрики для демо
        event_logger.log_event("message", message)  # Записываем демо в базу
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
    state: BotState, group_info: Dict, conversations: List[Dict], demo_mode: bool, event_logger: EventLogger
) -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")  # Создаем Flask-приложение

    def assemble_stats() -> Dict[str, object]:
        return {
            "events": state.total_events,  # Общее количество событий
            "messages": state.new_messages,  # Количество сообщений
            "invites": state.invites,  # Количество приглашений/удалений
            "errors": state.errors,  # Количество ошибок
            "last_messages": state.last_messages,  # История последних сообщений
            "timeline": state.events_timeline,  # Точки для графиков
        }  # Словарь статистики

    def serialize_log(row: Dict) -> Dict:
        return {
            "id": row.get("id"),  # ID записи
            "created_at": row.get("created_at"),  # Время создания
            "event_type": row.get("event_type"),  # Тип события
            "peer_id": row.get("peer_id"),  # ID чата
            "from_id": row.get("from_id"),  # Автор
            "message_id": row.get("message_id"),  # ID сообщения VK
            "reply_to": row.get("reply_to"),  # Кому ответили
            "text": row.get("text"),  # Текст
            "attachments": json.loads(row.get("attachments") or "[]"),  # Вложения
            "payload": json.loads(row.get("payload") or "{}"),  # Сырой payload
        }  # Конец словаря лога

    @app.route("/")
    def index():
        return render_template(
            "index.html",  # Шаблон дашборда
            initial_group=json.dumps(group_info, ensure_ascii=False),  # Начальные данные о сообществе
            initial_conversations=json.dumps(
                [conv.get("conversation", {}) for conv in conversations],  # Достаём тела диалогов
                ensure_ascii=False,  # Сохраняем кириллицу
            ),
            initial_stats=json.dumps(assemble_stats(), ensure_ascii=False),  # Начальные метрики
            initial_peers=json.dumps(event_logger.list_peers(), ensure_ascii=False),  # Доступные peer_id
            demo_mode=demo_mode,  # Флаг демо для вывода на страницу
        )  # Возвращаем HTML страницу

    @app.route("/api/stats")
    def stats():
        return jsonify(assemble_stats())  # Возвращаем актуальную статистику в JSON

    @app.route("/api/overview")
    def overview():
        return jsonify(
            {
                "group": group_info,  # Информация о сообществе
                "conversations": [conv.get("conversation", {}) for conv in conversations],  # Список диалогов
                "peers": event_logger.list_peers(),  # Список доступных чатов
            }
        )  # Возвращаем обзорную информацию

    @app.route("/api/logs")
    def logs():
        peer_id_raw = request.args.get("peer_id")  # Читаем peer_id из запроса
        peer_id = int(peer_id_raw) if peer_id_raw else None  # Преобразуем в число при наличии
        messages = [serialize_log(row) for row in event_logger.fetch_messages(peer_id=peer_id, limit=50)]  # Запрашиваем логи
        return jsonify({"items": messages, "peer_id": peer_id})  # Возвращаем JSON с логами

    return app  # Возвращаем готовое Flask-приложение


def main() -> None:
    settings = load_settings()  # Загружаем настройки окружения
    state = BotState()  # Создаем объект состояния
    event_logger = EventLogger(os.getenv("EVENT_DB", "logs.db"))  # Готовим логгер с путём из окружения
    demo_mode = settings.get("demo_mode", False)  # Проверяем, включен ли демо-режим
    if demo_mode:  # Если демо-режим включен
        payload = build_demo_payload(state, event_logger)  # Генерируем демо-данные и пишем их в базу
        group_info = payload["group_info"]  # Получаем демо-профиль
        conversations = payload["conversations"]  # Получаем демо-диалоги
        monitor = None  # Монитор не нужен в демо-режиме
    else:  # Обычный режим подключения к VK
        logger.info("Используем ID сообщества: %s", settings["group_id"])  # Логируем ID сообщества
        session = vk_api.VkApi(token=settings["token"])  # Создаем сессию VK API
        try:  # Пробуем запросить профиль сообщества
            group_info = fetch_group_profile(session, settings["group_id"])  # Получаем информацию о сообществе
        except Exception as exc:  # Если запрос завершился ошибкой
            logger.exception("Не удалось загрузить информацию о сообществе: %s", exc)  # Логируем подробности
            group_info = {}  # Используем пустой словарь
        try:  # Пробуем получить диалоги
            conversations = fetch_recent_conversations(session)  # Запрашиваем список диалогов
        except Exception as exc:  # Обрабатываем исключения VK API
            logger.exception("Не удалось получить список диалогов: %s", exc)  # Логируем ошибку
            conversations = []  # Используем пустой список
        monitor = BotMonitor(settings["token"], settings["group_id"], state, event_logger)  # Создаем монитор лонгпулла
        monitor.start()  # Запускаем лонгпулл
    app = build_dashboard_app(state, group_info, conversations, demo_mode, event_logger)  # Создаем Flask-приложение
    port = int(os.getenv("PORT", "8000"))  # Определяем порт из окружения
    logger.info("Дашборд запущен на http://127.0.0.1:%s", port)  # Сообщаем адрес запуска
    app.run(host="0.0.0.0", port=port)  # Запускаем сервер


if __name__ == "__main__":  # Точка входа
    try:  # Защищаем основной запуск от необработанных ошибок
        main()  # Запускаем приложение
    except Exception as exc:  # Если произошла ошибка
        logger.exception("Приложение завершилось с ошибкой: %s", exc)  # Пишем стек ошибки
        input("Нажмите Enter, чтобы закрыть окно...")  # Не даем окну закрыться мгновенно в Windows
