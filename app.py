import json  # Сериализация объектов в JSON для шаблона
import logging  # Настройка логирования событий приложения
import os  # Работа с переменными окружения
import sqlite3  # Хранение системных событий в базе SQLite
import threading  # Запуск фонового потока лонгпулла
from dataclasses import dataclass, field  # Упрощенное объявление классов состояния
from datetime import datetime  # Фиксация времени событий для графиков
from typing import Dict, List, Optional  # Подсказки типов для словарей, списков и необязательных значений

from dotenv import load_dotenv  # Загрузка переменных окружения из .env
from flask import Flask, jsonify, render_template, request  # Веб-сервер, рендер HTML и доступ к запросам
import vk_api  # Клиент VK API
from vk_api.bot_longpoll import VkBotEventType, VkBotLongPoll  # Лонгпулл сообщества для чтения событий

load_dotenv()  # Инициализируем загрузку переменных окружения при старте скрипта

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")  # Формат логов
logging.getLogger("werkzeug").setLevel(logging.WARNING)  # Поднимаем уровень логгера werkzeug, чтобы убрать шум запросов
logger = logging.getLogger(__name__)  # Получаем логгер для текущего модуля


class SystemEventLogger:
    """Менеджер системных событий с потокобезопасной записью в SQLite."""

    def __init__(self, db_path: str):
        self.db_path = db_path  # Путь до файла базы данных
        self._lock = threading.Lock()  # Блокировка для синхронизации потоков
        self._event_explanations = {  # Человеческие пояснения к кодам событий
            "APP_START": "Приложение запущено",  # Старт приложения
            "DEMO_MODE": "Запуск в демо-режиме без подключения к VK",  # Информация о демо
            "MONITOR_START": "Запущен фоновый монитор лонгпулла",  # Старт мониторинга
            "FETCH_GROUP_FAIL": "Не удалось получить информацию о сообществе",  # Ошибка запроса профиля
            "FETCH_DIALOGS_FAIL": "Не удалось получить список диалогов",  # Ошибка запроса диалогов
            "LONGPOLL_ERROR": "Ошибка при обработке событий лонгпулла",  # Ошибка лонгпулла
            "APP_CRASH": "Критическая ошибка приложения",  # Критическая ошибка
            "LOG_CLEARED": "Системные логи очищены пользователем",  # Очистка логов
        }  # Словарь пояснений
        self._last_seen_id = 0  # ID последней просмотренной записи
        self._ensure_schema()  # Создаем таблицу при инициализации

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, check_same_thread=False)  # Открываем соединение с отключенной проверкой потока
        connection.row_factory = sqlite3.Row  # Включаем режим доступа к колонкам по имени
        return connection  # Возвращаем соединение

    def _ensure_schema(self) -> None:
        with self._lock:  # Захватываем блокировку
            conn = self._connect()  # Открываем соединение
            try:  # Оформляем создание таблицы
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS system_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        code TEXT NOT NULL,
                        level TEXT NOT NULL,
                        message TEXT NOT NULL,
                        explanation TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    )
                    """  # SQL для таблицы
                )  # Выполняем создание таблицы
                conn.commit()  # Сохраняем изменения
            finally:
                conn.close()  # Закрываем соединение

    def log_event(self, code: str, level: str, message: str) -> None:
        explanation = self._event_explanations.get(code, "Неизвестное событие")  # Получаем пояснение по коду
        timestamp = datetime.utcnow().isoformat()  # Формируем метку времени
        with self._lock:  # Захватываем блокировку
            conn = self._connect()  # Открываем соединение
            try:  # Пытаемся записать событие
                conn.execute(
                    "INSERT INTO system_events (code, level, message, explanation, created_at) VALUES (?, ?, ?, ?, ?)",  # SQL вставки
                    (code, level.upper(), message, explanation, timestamp),  # Параметры вставки
                )  # Выполняем вставку
                conn.commit()  # Фиксируем изменения
            finally:
                conn.close()  # Закрываем соединение

    def fetch_events(self, limit: int = 200, mark_read: bool = True) -> Dict[str, object]:
        with self._lock:  # Захватываем блокировку
            conn = self._connect()  # Открываем соединение
            try:  # Читаем записи
                cursor = conn.execute(
                    "SELECT id, code, level, message, explanation, created_at FROM system_events ORDER BY id DESC LIMIT ?",
                    (limit,),
                )  # Получаем курсор
                rows = cursor.fetchall()  # Читаем все строки
                events = [dict(row) for row in rows]  # Преобразуем в словари
                has_new_important = any(  # Проверяем наличие новых важных событий
                    row["id"] > self._last_seen_id and row["level"] in {"WARNING", "ERROR"} for row in events
                )  # Условие важности
                if mark_read and events:  # Если нужно обновить отметку прочтения
                    self._last_seen_id = max(self._last_seen_id, events[0]["id"])  # Обновляем ID последнего события
                return {
                    "events": events,  # Список событий
                    "has_new_important": has_new_important,  # Флаг новых предупреждений/ошибок
                    "last_seen_id": self._last_seen_id,  # Последнее просмотренное
                }  # Возвращаем результат
            finally:
                conn.close()  # Закрываем соединение

    def clear_events(self) -> None:
        with self._lock:  # Захватываем блокировку
            conn = self._connect()  # Открываем соединение
            try:  # Пытаемся очистить таблицу
                conn.execute("DELETE FROM system_events")  # Удаляем все строки
                conn.commit()  # Фиксируем изменения
                self._last_seen_id = 0  # Сбрасываем отметку прочтения
            finally:
                conn.close()  # Закрываем соединение

    def summary(self) -> Dict[str, object]:
        with self._lock:  # Захватываем блокировку
            conn = self._connect()  # Открываем соединение
            try:  # Выполняем агрегацию
                cursor = conn.execute(
                    "SELECT level, COUNT(*) as count, MAX(id) as max_id FROM system_events GROUP BY level"
                )  # Выполняем запрос агрегатов
                rows = cursor.fetchall()  # Читаем все строки
                totals = {row["level"]: row["count"] for row in rows}  # Строим словарь с количеством
                max_id = max((row["max_id"] or 0 for row in rows), default=0)  # Находим максимальный ID
                has_new_important = max_id > self._last_seen_id and any(  # Проверяем новые важные события
                    (row["level"] in {"WARNING", "ERROR"} and (row["max_id"] or 0) > self._last_seen_id)
                    for row in rows
                )  # Условие важности
                return {
                    "total": sum(totals.values()),  # Общее количество
                    "info": totals.get("INFO", 0),  # Количество INFO
                    "warning": totals.get("WARNING", 0),  # Количество WARNING
                    "error": totals.get("ERROR", 0),  # Количество ERROR
                    "has_new_important": has_new_important,  # Флаг новых важных событий
                    "last_seen_id": self._last_seen_id,  # ID последнего просмотренного
                }  # Возвращаем сводку
            finally:
                conn.close()  # Закрываем соединение


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


class BotMonitor:
    """Фоновый монитор лонгпулла без отправки сообщений."""

    def __init__(self, token: str, group_id: int, state: BotState, event_logger: Optional[SystemEventLogger]):
        self.state = state  # Запоминаем объект состояния для обновлений
        self.token = token  # Токен сообщества
        self.group_id = group_id  # ID сообщества
        self.session = vk_api.VkApi(token=self.token)  # Сессия VK API для запросов
        self._stop_event = threading.Event()  # Флаг корректной остановки потока
        self.event_logger = event_logger  # Системный логгер для записи ошибок

    def start(self) -> None:
        listener_thread = threading.Thread(target=self._listen, daemon=True)  # Создаем фоновый поток
        listener_thread.start()  # Запускаем поток с лонгпуллом
        logger.info("Лонгпулл запущен в фоновом потоке")  # Пишем в лог успешный запуск
        if self.event_logger:  # Если системный логгер доступен
            self.event_logger.log_event("MONITOR_START", "INFO", "Фоновый монитор запущен")  # Фиксируем событие запуска

    def _listen(self) -> None:
        longpoll = VkBotLongPoll(self.session, self.group_id)  # Создаем слушателя событий сообщества
        while not self._stop_event.is_set():  # Цикл до получения сигнала остановки
            try:
                for event in longpoll.listen():  # Перебираем входящие события VK
                    if event.type == VkBotEventType.MESSAGE_NEW:  # Если это новое сообщение
                        message = event.object.message  # Извлекаем тело сообщения
                        self.state.mark_event(  # Фиксируем событие в состоянии
                            {
                                "from_id": message.get("from_id"),  # ID отправителя
                                "peer_id": message.get("peer_id"),  # Диалог или чат
                                "text": message.get("text"),  # Текст сообщения
                            },
                            "message",  # Помечаем тип события как сообщение
                        )
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
                if self.event_logger:  # Если доступен системный логгер
                    self.event_logger.log_event("LONGPOLL_ERROR", "ERROR", str(exc))  # Фиксируем ошибку в SQLite

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


def build_demo_payload(state: BotState) -> Dict[str, object]:
    state.mark_event({"from_id": 111, "peer_id": 1, "text": "Первое демо-сообщение"}, "message")  # Добавляем демо-сообщение
    state.mark_event({"from_id": 222, "peer_id": 2, "text": "Еще одно демо"}, "message")  # Добавляем второе демо-сообщение
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
    state: BotState, group_info: Dict, conversations: List[Dict], demo_mode: bool, event_logger: SystemEventLogger
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
            }
        )  # Возвращаем обзорную информацию

    @app.route("/api/system-logs")
    def system_logs():
        mark_read = request.args.get("mark_read", "true").lower() != "false"  # Проверяем, нужно ли отмечать прочитанным
        limit = int(request.args.get("limit", "200"))  # Получаем лимит выдачи
        return jsonify(event_logger.fetch_events(limit=limit, mark_read=mark_read))  # Отдаем события в JSON

    @app.route("/api/system-logs/summary")
    def system_logs_summary():
        return jsonify(event_logger.summary())  # Возвращаем сводку по уровням

    @app.route("/api/system-logs/clear", methods=["POST"])
    def clear_system_logs():
        event_logger.clear_events()  # Очищаем таблицу событий
        event_logger.log_event("LOG_CLEARED", "INFO", "Журнал очищен через API")  # Фиксируем событие очистки
        return jsonify({"status": "ok"})  # Возвращаем успешный ответ

    @app.route("/system-logs")
    def system_logs_page():
        initial_logs = event_logger.fetch_events(limit=200, mark_read=True)  # Получаем стартовый набор логов
        return render_template(
            "system_logs.html",  # Шаблон страницы логов
            initial_logs=json.dumps(initial_logs, ensure_ascii=False),  # JSON логов для инициализации
        )  # Рендерим страницу логов

    return app  # Возвращаем готовое Flask-приложение


def main() -> None:
    settings = load_settings()  # Загружаем настройки окружения
    event_logger = SystemEventLogger(os.getenv("SYSTEM_LOG_DB", "system_events.db"))  # Создаем системный логгер
    event_logger.log_event("APP_START", "INFO", "Приложение инициализировано")  # Фиксируем старт приложения
    state = BotState()  # Создаем объект состояния
    demo_mode = settings.get("demo_mode", False)  # Проверяем, включен ли демо-режим
    if demo_mode:  # Если демо-режим включен
        event_logger.log_event("DEMO_MODE", "WARNING", "Запуск без подключения к VK (демо)")  # Пишем событие демо-режима
        payload = build_demo_payload(state)  # Генерируем демо-данные
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
            event_logger.log_event("FETCH_GROUP_FAIL", "WARNING", str(exc))  # Пишем событие об ошибке запроса профиля
            group_info = {}  # Используем пустой словарь
        try:  # Пробуем получить диалоги
            conversations = fetch_recent_conversations(session)  # Запрашиваем список диалогов
        except Exception as exc:  # Обрабатываем исключения VK API
            logger.exception("Не удалось получить список диалогов: %s", exc)  # Логируем ошибку
            event_logger.log_event("FETCH_DIALOGS_FAIL", "WARNING", str(exc))  # Фиксируем ошибку загрузки диалогов
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
        try:  # Пробуем записать событие в системный журнал
            SystemEventLogger(os.getenv("SYSTEM_LOG_DB", "system_events.db")).log_event(
                "APP_CRASH", "ERROR", str(exc)
            )  # Фиксируем критическую ошибку
        except Exception:  # Если логирование не удалось
            pass  # Игнорируем сбой записи системного события
        logger.exception("Приложение завершилось с ошибкой: %s", exc)  # Пишем стек ошибки
        input("Нажмите Enter, чтобы закрыть окно...")  # Не даем окну закрыться мгновенно в Windows
