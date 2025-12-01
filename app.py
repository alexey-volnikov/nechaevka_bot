import logging  # Настраиваем модуль логирования для отслеживания работы скрипта
import os  # Работа с переменными окружения и путями
import threading  # Фоновый поток для лонгпулла
from dataclasses import dataclass, field  # Удобное хранение состояния бота
from typing import Dict, List  # Подсказки типов для словарей и списков

from flask import Flask, jsonify  # Минимальный дашборд через HTTP
from dotenv import load_dotenv  # Загрузка настроек из .env файла
import vk_api  # Клиент для работы с API ВКонтакте
from vk_api.bot_longpoll import VkBotEventType, VkBotLongPoll  # Лонгпулл сообщества

load_dotenv()  # Загружаем переменные окружения из файла .env если он есть

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")  # Формат логов
logger = logging.getLogger(__name__)  # Инициализация логгера модуля


@dataclass
class BotState:
    """Простое состояние бота для статистики событий."""

    total_events: int = 0  # Общее число полученных событий
    new_messages: int = 0  # Число входящих сообщений
    invites: int = 0  # Приглашения/удаления в чатах
    errors: int = 0  # Количество ошибок при обработке
    last_messages: List[Dict] = field(default_factory=list)  # История последних сообщений для просмотра

    def remember_message(self, payload: Dict, keep: int = 10) -> None:
        """Сохраняем последние сообщения без отправки ответов."""

        self.last_messages.append(payload)  # Добавляем новое сообщение в список
        if len(self.last_messages) > keep:  # Проверяем, не превысил ли список лимит
            self.last_messages.pop(0)  # Удаляем самое старое сообщение, чтобы список не рос бесконечно


class BotMonitor:
    """Фоновая обертка над лонгпуллом без отправки сообщений."""

    def __init__(self, token: str, group_id: int, state: BotState):
        self.state = state  # Храним ссылку на объект состояния
        self.token = token  # Токен сообщества
        self.group_id = group_id  # ID сообщества
        self.session = vk_api.VkApi(token=self.token)  # Сессия VK API с указанным токеном
        self._stop_event = threading.Event()  # Флаг для мягкой остановки

    def start(self) -> None:
        listener_thread = threading.Thread(target=self._listen, daemon=True)  # Поток для лонгпулла
        listener_thread.start()  # Запускаем поток
        logger.info("Лонгпулл запущен в фоновом потоке")  # Сообщаем в лог о запуске

    def _listen(self) -> None:
        longpoll = VkBotLongPoll(self.session, self.group_id)  # Создаем слушателя событий сообщества
        while not self._stop_event.is_set():  # Работаем, пока не получен сигнал на остановку
            try:
                for event in longpoll.listen():  # Перебираем входящие события
                    self.state.total_events += 1  # Увеличиваем счетчик событий
                    if event.type == VkBotEventType.MESSAGE_NEW:  # Новое сообщение
                        self.state.new_messages += 1  # Увеличиваем счетчик сообщений
                        message = event.object.message  # Достаем тело сообщения
                        self.state.remember_message(
                            {
                                "from_id": message.get("from_id"),  # ID отправителя
                                "peer_id": message.get("peer_id"),  # Диалог или чат
                                "text": message.get("text"),  # Текст сообщения
                            }
                        )  # Сохраняем информацию для мониторинга
                        logger.info("Сообщение: peer %s -> %s", message.get("peer_id"), message.get("text"))  # Логируем без ответа
                    elif event.type in (VkBotEventType.CHAT_INVITE_USER, VkBotEventType.CHAT_KICK_USER):  # Приглашения/удаления
                        self.state.invites += 1  # Отмечаем событие работы с участниками чата
                        logger.info("Событие участников: %s", event.type)  # Логируем тип события
                    else:  # Другие типы событий
                        logger.info("Получено событие: %s", event.type)  # Просто фиксируем
            except Exception as exc:  # Обрабатываем любые ошибки лонгпулла
                self.state.errors += 1  # Считаем ошибку
                logger.exception("Ошибка лонгпулла: %s", exc)  # Записываем стек в лог

    def stop(self) -> None:
        self._stop_event.set()  # Сигнализируем о завершении работы


def load_settings() -> Dict[str, str]:
    token = os.getenv("VK_GROUP_TOKEN", "")  # Получаем токен сообщества
    group_id = os.getenv("VK_GROUP_ID", "")  # Получаем ID сообщества
    if not token or not group_id:  # Проверяем, заданы ли необходимые значения
        raise RuntimeError("Укажите VK_GROUP_TOKEN и VK_GROUP_ID в .env или переменных окружения")  # Если нет, падаем с подсказкой
    return {"token": token, "group_id": int(group_id)}  # Возвращаем настройки в виде словаря


def fetch_group_profile(session: vk_api.VkApi, group_id: int) -> Dict:
    """Получаем информацию о сообществе для обзора."""

    info = session.method("groups.getById", {"group_id": group_id, "fields": "description,contacts,members_count"})  # Запрос к VK API
    return info[0] if info else {}  # Возвращаем первый элемент списка или пустой словарь


def fetch_recent_conversations(session: vk_api.VkApi, limit: int = 10) -> List[Dict]:
    """Получаем список доступных бесед без отправки сообщений."""

    response = session.method(
        "messages.getConversations",  # Метод для чтения переписок
        {"count": limit, "filter": "all"},  # Берем несколько последних диалогов
    )
    return response.get("items", [])  # Возвращаем список объектов диалогов


def build_dashboard_app(state: BotState, group_info: Dict, conversations: List[Dict]) -> Flask:
    """Создаем минимальный Flask-приложение для просмотра состояния."""

    app = Flask(__name__)  # Инициализируем веб-приложение

    @app.route("/")
    def index():
        return jsonify(
            {
                "message": "Мониторинг бота активен. Сообщения не отправляются.",  # Краткое описание
                "group": group_info,  # Данные о сообществе
                "conversations": [conv.get("conversation", {}) for conv in conversations],  # Последние диалоги
            }
        )  # Возвращаем JSON с обзором

    @app.route("/stats")
    def stats():
        return jsonify(
            {
                "events": state.total_events,  # Общее количество событий
                "messages": state.new_messages,  # Количество сообщений
                "invites": state.invites,  # Приглашения/удаления
                "errors": state.errors,  # Ошибки
                "last_messages": state.last_messages,  # Последние сообщения
            }
        )  # Возвращаем JSON статистики

    return app  # Возвращаем готовое приложение


def main() -> None:
    settings = load_settings()  # Читаем настройки из окружения
    state = BotState()  # Создаем объект состояния для статистики
    session = vk_api.VkApi(token=settings["token"])  # Сессия VK API для запросов
    group_info = fetch_group_profile(session, settings["group_id"])  # Получаем данные о сообществе
    conversations = fetch_recent_conversations(session)  # Берем список доступных диалогов
    monitor = BotMonitor(settings["token"], settings["group_id"], state)  # Создаем монитора
    monitor.start()  # Запускаем лонгпулл в фоне
    app = build_dashboard_app(state, group_info, conversations)  # Конфигурируем дашборд
    port = int(os.getenv("PORT", "8000"))  # Определяем порт для веб-сервера
    logger.info("Дашборд запущен на http://127.0.0.1:%s", port)  # Пишем в лог адрес
    app.run(host="0.0.0.0", port=port)  # Стартуем Flask сервер


if __name__ == "__main__":  # Точка входа при запуске файла
    main()  # Запускаем основную функцию
