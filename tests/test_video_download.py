import os  # Импортируем os для удаления временных файлов после тестов
import tempfile  # Импортируем tempfile для создания временных директорий и файлов
import unittest  # Импортируем unittest для написания тестовых кейсов
from pathlib import Path  # Импортируем Path для работы с путями к файлам
from unittest.mock import MagicMock, patch  # Импортируем инструменты для подмены функций и объектов в тестах

from app import BotMonitor, BotState, EventLogger  # Импортируем классы приложения, которые будем тестировать


class BotMonitorVideoDownloadTest(unittest.TestCase):  # Определяем набор тестов для скачивания видео
    def setUp(self) -> None:  # Подготавливаем окружение перед каждым тестом
        self.temp_db = tempfile.NamedTemporaryFile(delete=False)  # Создаем временный файл базы, чтобы не трогать реальные данные
        self.temp_db.close()  # Закрываем файловый дескриптор, позволяя SQLite использовать файл
        self.temp_dir = tempfile.TemporaryDirectory()  # Создаем временную директорию для вложений
        self.logger = EventLogger(self.temp_db.name)  # Создаем логгер событий, который пишет в временную базу
        self.monitor = BotMonitor("token", 1, BotState(), self.logger)  # Создаем экземпляр монитора бота
        self.monitor.attachments_dir = Path(self.temp_dir.name)  # Перенаправляем вложения в временную папку
        self.monitor.attachments_dir.mkdir(parents=True, exist_ok=True)  # Убеждаемся, что временная папка существует
        self.monitor.session = MagicMock()  # Подменяем сессию VK API на заглушку, чтобы не ходить в сеть

    def tearDown(self) -> None:  # Очищаем временные ресурсы после каждого теста
        self.logger._connection.close()  # Закрываем соединение с временной базой
        os.unlink(self.temp_db.name)  # Удаляем файл базы
        self.temp_dir.cleanup()  # Удаляем временную директорию вложений

    def test_video_downloads_from_direct_mp4(self):  # Проверяем, что видео с прямой mp4-ссылкой скачивается
        fake_body = b"video-bytes"  # Задаем тестовое содержимое файла

        class FakeResponse:  # Определяем фейковый ответ requests
            def raise_for_status(self):  # Метод для проверки статуса
                return None  # Ничего не делаем, имитируя успешный ответ

            def iter_content(self, chunk_size=8192):  # Итератор по частям ответа
                yield fake_body  # Отдаем заранее подготовленные байты

        fake_response = FakeResponse()  # Создаем экземпляр фейкового ответа
        with patch("app.requests.get", return_value=fake_response):  # Подменяем requests.get, чтобы не ходить в интернет
            attachment = {  # Формируем вложение видео с готовыми mp4-ссылками
                "type": "video",  # Указываем тип вложения
                "video": {"files": {"mp4_240": "http://example.com/low.mp4", "mp4_720": "http://example.com/high.mp4"}},  # Блок файлов видео
            }  # Завершаем словарь вложения
            normalized = self.monitor._normalize_attachment(attachment, peer_id=10, message_id=20)  # Нормализуем и скачиваем вложение
        self.assertEqual(normalized.get("download_url"), "http://example.com/high.mp4")  # Проверяем, что выбрана ссылка высокого качества
        self.assertEqual(normalized.get("download_state"), "ready")  # Проверяем, что статус скачивания успешный
        local_path = normalized.get("local_path")  # Читаем путь до сохраненного файла
        self.assertIsNotNone(local_path)  # Убеждаемся, что путь заполнен
        with open(local_path, "rb") as saved_file:  # Открываем сохраненный файл
            self.assertEqual(saved_file.read(), fake_body)  # Проверяем, что содержимое совпадает с фейковым ответом

    def test_video_fallback_uses_api_and_saves(self):  # Проверяем, что при отсутствии mp4 в payload используем VK API
        fake_body = b"api-video"  # Задаем тестовое содержимое файла для API-ветки

        class FakeResponse:  # Определяем фейковый ответ requests для скачивания
            def raise_for_status(self):  # Метод проверки статуса
                return None  # Ничего не делаем, имитируя успешный ответ

            def iter_content(self, chunk_size=8192):  # Итератор по содержимому ответа
                yield fake_body  # Отдаем тестовые байты

        fake_response = FakeResponse()  # Создаем экземпляр фейкового ответа
        self.monitor.session.method = MagicMock(  # Подменяем метод VK API
            return_value={"items": [{"files": {"mp4": "http://example.com/from_api.mp4"}}]}  # Возвращаем структуру с mp4-ссылкой
        )  # Завершаем настройку заглушки
        with patch("app.requests.get", return_value=fake_response):  # Подменяем запросы для скачивания
            attachment = {  # Формируем вложение без блока files
                "type": "video",  # Указываем тип видео
                "video": {"owner_id": 1, "id": 2, "access_key": "key"},  # Добавляем поля для вызова video.get
            }  # Завершаем словарь вложения
            normalized = self.monitor._normalize_attachment(attachment, peer_id=11, message_id=22)  # Нормализуем вложение через API
        self.assertEqual(normalized.get("download_url"), "http://example.com/from_api.mp4")  # Проверяем, что ссылка взята из API
        self.assertEqual(normalized.get("download_state"), "ready")  # Проверяем успешный статус скачивания
        local_path = normalized.get("local_path")  # Берем путь до сохраненного файла
        self.assertTrue(local_path and Path(local_path).exists())  # Убеждаемся, что файл реально создан
        with open(local_path, "rb") as saved_file:  # Открываем сохраненный файл
            self.assertEqual(saved_file.read(), fake_body)  # Сверяем содержимое с тестовыми данными

    def test_video_without_files_keeps_player_link(self):  # Проверяем, что при отсутствии mp4 сохраняется ссылка на плеер
        attachment = {  # Формируем вложение без прямых ссылок на mp4
            "type": "video",  # Указываем тип вложения как видео
            "video": {"player": "https://vk.com/video_ext.php?test"},  # Добавляем только ссылку на плеер VK
        }  # Завершаем словарь вложения
        normalized = self.monitor._normalize_attachment(attachment, peer_id=33, message_id=44)  # Нормализуем вложение без скачивания
        self.assertIsNone(normalized.get("download_url"))  # Убеждаемся, что прямая ссылка mp4 не определена
        self.assertEqual(normalized.get("download_state"), "failed")  # Проверяем, что статус помечен как неуспешный
        self.assertEqual(normalized.get("url"), "https://vk.com/video_ext.php?test")  # Убеждаемся, что ссылка на плеер сохранена
        self.assertIsNone(normalized.get("local_path"))  # Проверяем, что файл не создавался
        self.assertTrue(normalized.get("download_error"))  # Убеждаемся, что причина ошибки записана


if __name__ == "__main__":  # Точка входа для запуска файла напрямую
    unittest.main()  # Запускаем тестовый раннер
