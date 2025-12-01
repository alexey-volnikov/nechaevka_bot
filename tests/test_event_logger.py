import json  # Импортируем модуль для работы с JSON
import os  # Импортируем os для удаления временного файла
import tempfile  # Импортируем tempfile для создания временных файлов
import unittest  # Импортируем unittest для написания тестов
from pathlib import Path  # Импортируем Path для работы с путями вложений

from app import BotMonitor, BotState, EventLogger  # Импортируем классы приложения для тестов


class EventLoggerAttachmentsTest(unittest.TestCase):  # Определяем тестовый класс для вложений
    def setUp(self) -> None:  # Подготовительный шаг перед каждым тестом
        self.temp_db = tempfile.NamedTemporaryFile(delete=False)  # Создаем временный файл базы
        self.temp_db.close()  # Закрываем файловый дескриптор, чтобы SQLite мог использовать файл
        self.logger = EventLogger(self.temp_db.name)  # Создаем экземпляр логгера с временной базой

    def tearDown(self) -> None:  # Очистка после каждого теста
        self.logger._connection.close()  # Закрываем соединение с базой
        os.unlink(self.temp_db.name)  # Удаляем временный файл базы

    def test_multiple_attachments_preserved(self):  # Тестируем, что сохраняется несколько вложений
        payload = {  # Формируем тестовый payload сообщения
            "peer_id": 1,  # ID чата
            "from_id": 123,  # ID отправителя
            "id": 99,  # ID сообщения
            "attachments": [  # Список вложений
                {"type": "photo", "url": "http://example.com/1.jpg"},  # Первое вложение
                {"type": "photo", "url": "http://example.com/2.jpg"},  # Второе вложение
            ],  # Конец списка вложений
        }  # Конец payload
        self.logger.log_event("message", payload)  # Сохраняем событие с вложениями
        rows = self.logger.fetch_messages(limit=10)  # Загружаем строки из базы
        self.assertEqual(len(rows), 1)  # Проверяем, что записана одна строка
        stored = json.loads(rows[0]["attachments"])  # Десериализуем вложения из базы
        self.assertEqual(len(stored), 2)  # Проверяем, что сохранились оба вложения
        self.assertEqual(stored[1]["url"], "http://example.com/2.jpg")  # Проверяем целостность второго вложения

    def test_serialize_log_keeps_many_attachments(self):  # Проверяем, что сериализация сохраняет все вложения
        payload = {  # Формируем тестовый payload с большим количеством вложений
            "peer_id": 5,  # ID чата для теста
            "from_id": 456,  # ID отправителя
            "id": 101,  # ID сообщения
            "attachments": [  # Список из девяти вложений
                {"type": "photo", "url": f"http://example.com/{idx}.jpg"}  # Вложение с уникальной ссылкой
                for idx in range(9)  # Генерируем девять вложений
            ],  # Завершили список вложений
        }  # Завершили payload
        self.logger.log_event("message", payload)  # Сохраняем событие в базу
        row = self.logger.fetch_messages(limit=1)[0]  # Забираем свежую запись из базы
        stored_attachments = json.loads(row["attachments"])  # Десериализуем вложения из базы
        self.assertEqual(len(stored_attachments), 9)  # Убеждаемся, что все девять вложений присутствуют
        self.assertTrue(all(att.get("url") for att in stored_attachments))  # Проверяем, что у каждого есть ссылка

    def test_payload_keeps_all_attachments_for_gallery(self):  # Проверяем, что payload сохраняет все вложения для галереи
        payload = {  # Формируем payload с вложениями и репостом
            "peer_id": 7,  # ID чата
            "from_id": 789,  # Автор сообщения
            "id": 202,  # ID сообщения
            "attachments": [  # Вложения основного сообщения
                {"type": "photo", "url": "http://example.com/a.jpg"},  # Первое вложение
                {"type": "photo", "url": "http://example.com/b.jpg"},  # Второе вложение
                {"type": "photo", "url": "http://example.com/c.jpg"},  # Третье вложение
            ],  # Завершаем список вложений
            "copy_history": [  # Добавляем репост
                {"attachments": [{"type": "doc", "url": "http://example.com/file.pdf"}]},  # Репост с документом
            ],  # Завершаем copy_history
        }  # Завершаем формирование payload
        self.logger.log_event("message", payload)  # Сохраняем событие в базу
        row = self.logger.fetch_messages(limit=1)[0]  # Забираем свежую запись из базы
        stored_payload = json.loads(row["payload"])  # Десериализуем сохраненный payload
        self.assertEqual(len(stored_payload.get("attachments", [])), 3)  # Проверяем, что все три вложения основного сообщения сохранены
        self.assertEqual(len(stored_payload.get("copy_history", [])), 1)  # Проверяем, что репост сохранен
        nested_attachments = stored_payload.get("copy_history", [])[0].get("attachments", [])  # Извлекаем вложения из репоста
        self.assertEqual(len(nested_attachments), 1)  # Убеждаемся, что вложение репоста присутствует


class DummySession:  # Определяем поддельную сессию VK для теста гидрации
    def method(self, name: str, params: dict):  # Метод имитирует вызовы VK API
        return {  # Возвращаем фиксированный ответ с полным набором вложений
            "items": [  # Список сообщений
                {  # Детальное сообщение
                    "id": params.get("message_ids"),  # Повторяем ID, который запросили
                    "attachments": [  # Полный список вложений, который должен вернуться
                        {"type": "photo", "url": "http://example.com/full1.jpg"},  # Первое вложение
                        {"type": "photo", "url": "http://example.com/full2.jpg"},  # Второе вложение
                    ],  # Завершаем список вложений
                    "copy_history": [  # Добавляем copy_history с вложением
                        {"attachments": [{"type": "doc", "url": "http://example.com/doc.pdf"}]},  # Репост с документом
                    ],  # Завершаем copy_history
                    "reply_message": {  # Добавляем исходное сообщение-ответ
                        "id": 500,  # ID исходного сообщения
                        "attachments": [  # Вложения ответа
                            {"type": "photo", "url": "http://example.com/reply.jpg"},  # Вложение ответа
                        ],  # Завершаем список вложений ответа
                    },  # Завершаем блок reply_message
                },  # Завершаем детальное сообщение
            ]  # Завершаем список сообщений
        }  # Завершаем словарь ответа


class BotMonitorHydrationTest(unittest.TestCase):  # Тестовый класс для проверки догрузки сообщения из API
    def setUp(self) -> None:  # Подготовка перед тестом
        self.temp_db = tempfile.NamedTemporaryFile(delete=False)  # Создаем временный файл базы
        self.temp_db.close()  # Закрываем файл, чтобы SQLite мог использовать его
        self.logger = EventLogger(self.temp_db.name)  # Создаем экземпляр логгера событий
        self.monitor = BotMonitor("token", 1, BotState(), self.logger)  # Создаем монитор бота
        self.monitor.session = DummySession()  # Подменяем сессию VK на поддельную

    def tearDown(self) -> None:  # Очистка после теста
        self.logger._connection.close()  # Закрываем соединение с базой
        os.unlink(self.temp_db.name)  # Удаляем временный файл базы

    def test_hydrate_message_loads_all_attachments(self):  # Проверяем, что догрузка заменяет усеченные вложения
        minimal_message = {  # Формируем усеченное сообщение
            "id": 321,  # ID сообщения
            "attachments": [{"type": "photo", "url": "http://example.com/short.jpg"}],  # Только одно вложение
        }  # Завершаем подготовку сообщения
        hydrated = self.monitor._hydrate_message_details(minimal_message)  # Догружаем сообщение через API
        self.assertEqual(len(hydrated.get("attachments", [])), 2)  # Проверяем, что вернулось два вложения
        self.assertEqual(len(hydrated.get("copy_history", [])), 1)  # Проверяем, что copy_history подставился
        reply_block = hydrated.get("reply_message", {})  # Извлекаем блок ответа
        self.assertEqual(len(reply_block.get("attachments", [])), 1)  # Убеждаемся, что вложения ответа присутствуют


class BotMonitorAttachmentDedupTest(unittest.TestCase):  # Тестируем удаление дублей вложений при сохранении
    def setUp(self) -> None:  # Подготовка перед тестом
        self.temp_db = tempfile.NamedTemporaryFile(delete=False)  # Создаем временный файл базы
        self.temp_db.close()  # Закрываем файловый дескриптор базы
        self.temp_dir = tempfile.TemporaryDirectory()  # Создаем временную директорию для вложений
        self.logger = EventLogger(self.temp_db.name)  # Создаем логгер событий
        self.monitor = BotMonitor("token", 1, BotState(), self.logger)  # Создаем монитор бота
        self.monitor.attachments_dir = Path(self.temp_dir.name)  # Направляем вложения в временную директорию
        self.monitor.attachments_dir.mkdir(parents=True, exist_ok=True)  # Убеждаемся, что папка существует

    def tearDown(self) -> None:  # Очистка после теста
        self.logger._connection.close()  # Закрываем соединение с базой
        os.unlink(self.temp_db.name)  # Удаляем временный файл базы
        self.temp_dir.cleanup()  # Удаляем временную директорию вложений

    def test_save_attachments_removes_duplicates(self):  # Проверяем, что дубль вложений отфильтровывается
        def fake_download(url: str, target_path: Path) -> Path:  # Определяем заглушку скачивания файла
            target_path.parent.mkdir(parents=True, exist_ok=True)  # Создаем каталоги для файла
            target_path.touch()  # Создаем пустой файл, имитируя скачивание
            return target_path  # Возвращаем путь к файлу

        self.monitor._download_file = fake_download  # Подменяем скачивание вложений на заглушку
        attachments = [  # Формируем список вложений с дублями
            {"type": "photo", "photo": {"owner_id": 1, "id": 10, "sizes": [], "url": "http://example.com/1.jpg"}},  # Первое вложение
            {"type": "photo", "photo": {"owner_id": 1, "id": 10, "sizes": [], "url": "http://example.com/1.jpg"}},  # Дубликат первого
            {"type": "photo", "url": "http://example.com/2.jpg"},  # Второе вложение
            {"type": "photo", "url": "http://example.com/2.jpg"},  # Дубликат второго
        ]  # Завершили список вложений
        normalized = self.monitor._save_attachments(attachments, peer_id=55, message_id=77)  # Сохраняем вложения с удалением дублей
        self.assertEqual(len(normalized), 2)  # Проверяем, что осталось два уникальных вложения
        urls = [item.get("download_url") or item.get("url") for item in normalized]  # Извлекаем ссылки для проверки
        self.assertIn("http://example.com/1.jpg", urls)  # Убеждаемся, что первое вложение присутствует
        self.assertIn("http://example.com/2.jpg", urls)  # Убеждаемся, что второе вложение присутствует


class DummySessionConversation:  # Поддельная сессия, возвращающая расширенный ответ по conversation_message_id
    def method(self, name: str, params: dict):  # Имитация вызова VK API
        if name == "messages.getById":  # Ветка для запроса по message_id
            return {  # Возвращаем усеченный ответ
                "items": [  # Список сообщений
                    {  # Единственный элемент
                        "id": params.get("message_ids"),  # ID сообщения
                        "attachments": [  # Отдаем только одно вложение, имитируя усеченный ответ
                            {"type": "photo", "url": "http://example.com/short.jpg"},  # Единственное вложение
                        ],  # Завершили список вложений
                    },  # Завершили сообщение
                ]  # Завершили список items
            }  # Завершили словарь ответа
        return {  # Ответ для messages.getByConversationMessageId
            "items": [  # Список сообщений
                {  # Полное сообщение с несколькими вложениями
                    "id": params.get("conversation_message_ids"),  # ID сообщения в переписке
                    "attachments": [  # Полный список вложений
                        {"type": "photo", "url": "http://example.com/full1.jpg"},  # Первое вложение
                        {"type": "photo", "url": "http://example.com/full2.jpg"},  # Второе вложение
                        {"type": "photo", "url": "http://example.com/full3.jpg"},  # Третье вложение
                    ],  # Завершили список вложений
                    "copy_history": [],  # Пустой список репостов
                    "reply_message": None,  # Нет ответа в этой фиксации
                },  # Завершили сообщение
            ]  # Завершили список items
        }  # Завершили словарь ответа


class BotMonitorConversationHydrationTest(unittest.TestCase):  # Тестируем догрузку через conversation_message_id
    def setUp(self) -> None:  # Подготовка перед тестом
        self.temp_db = tempfile.NamedTemporaryFile(delete=False)  # Создаем временную базу
        self.temp_db.close()  # Закрываем файл, чтобы им мог пользоваться SQLite
        self.logger = EventLogger(self.temp_db.name)  # Создаем логгер событий
        self.monitor = BotMonitor("token", 1, BotState(), self.logger)  # Создаем монитор
        self.monitor.session = DummySessionConversation()  # Подменяем сессию на поддельную

    def tearDown(self) -> None:  # Очистка после теста
        self.logger._connection.close()  # Закрываем соединение с базой
        os.unlink(self.temp_db.name)  # Удаляем временный файл базы

    def test_conversation_hydration_restores_all_photos(self):  # Проверяем, что догрузка по conversation_message_id возвращает все вложения
        minimal_message = {  # Формируем усеченное сообщение
            "id": 111,  # Глобальный ID сообщения
            "conversation_message_id": 222,  # ID сообщения в переписке
            "peer_id": 333,  # ID чата
            "attachments": [{"type": "photo", "url": "http://example.com/short.jpg"}],  # Единственное вложение
        }  # Завершили подготовку сообщения
        hydrated = self.monitor._hydrate_message_details(minimal_message)  # Догружаем сообщение через API
        attachments = hydrated.get("attachments", [])  # Берем список вложений после догрузки
        self.assertEqual(len(attachments), 3)  # Проверяем, что вернулись все три вложения
        self.assertEqual(attachments[2].get("url"), "http://example.com/full3.jpg")  # Проверяем, что третье вложение доступно


if __name__ == "__main__":  # Точка входа для запуска файла напрямую
    unittest.main()  # Запускаем тестовый раннер
