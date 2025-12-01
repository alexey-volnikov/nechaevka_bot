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


if __name__ == "__main__":  # Точка входа для запуска файла напрямую
    unittest.main()  # Запускаем тестовый раннер
