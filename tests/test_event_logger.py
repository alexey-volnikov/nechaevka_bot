import json  # Импортируем модуль для работы с JSON
import os  # Импортируем os для удаления временного файла
import tempfile  # Импортируем tempfile для создания временных файлов
import unittest  # Импортируем unittest для написания тестов

from app import EventLogger  # Импортируем класс EventLogger из приложения


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


if __name__ == "__main__":  # Точка входа для запуска файла напрямую
    unittest.main()  # Запускаем тестовый раннер
