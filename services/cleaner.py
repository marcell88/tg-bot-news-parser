import asyncio
import logging
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import asyncpg 
from telethon import TelegramClient
from telethon.tl.types import Message, Channel
from telethon.tl.functions.messages import GetHistoryRequest

# Импортируем общий менеджер БД
from database.database import Database
from database.database_config import DatabaseConfig

# Загружаем переменные окружения
load_dotenv()

# Настраиваем логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Конфигурация (минимально необходимая для БД и логики очистки) ---
class Config:
    """
    Класс для хранения конфигурации приложения (DB и параметры очистки).
    """
    # --- НАСТРОЙКИ БАЗЫ ДАННЫХ (Копируются из listener.py для автономности) ---
    DB_HOST = DatabaseConfig.DB_HOST
    DB_PORT = DatabaseConfig.DB_PORT
    DB_NAME = DatabaseConfig.DB_NAME
    DB_USER = DatabaseConfig.DB_USER
    DB_PASS = DatabaseConfig.DB_PASS
    
    # --- НАСТРОЙКИ ОЧИСТКИ ---
    # CLEANUP_INTERVAL_HOURS: Интервал между запусками задачи очистки (в часах)
    CLEANUP_INTERVAL_HOURS = 24  # Теперь раз в сутки
    # RETENTION_DAYS: Возраст записей для удаления (в днях), если finished=TRUE
    RETENTION_DAYS = 7 
    # RETENTION_HOURS_TOP: Возраст записей для удаления из telegram_posts_top (в часах)
    RETENTION_HOURS_TOP = 24  # 3) для таблицы telegram_posts_top - старше 24 часов
    # RETENTION_HOURS_TOP_TOP: Возраст записей для удаления из telegram_posts_top_top (в часах)
    RETENTION_HOURS_TOP_TOP = 72  # для таблицы telegram_posts_top_top - старше 72 часов
    
    # --- НАСТРОЙКИ TELEGRAM ---
    PUBLISH_BOT_API_KEY = os.getenv('PUBLISH_BOT_API_KEY')
    TELEGRAM_GROUP = '@news_anthology'  # Имя группы/канала
    TELEGRAM_RETENTION_DAYS = 30  # Удалять сообщения старше 2 недель

# --- Класс DBCleaner ---
class DBCleaner:
    """
    Класс-служба, отвечающая за регулярное подключение к БД и удаление
    старых, уже обработанных записей.
    """
    def __init__(self):
        self.db_pool = None
        self.telegram_client = None
        # Интервал между запусками цикла очистки (24 часа)
        self.cleanup_interval = timedelta(hours=Config.CLEANUP_INTERVAL_HOURS)
        # Период хранения для telegram_posts (7 дней)
        self.retention_period_posts = timedelta(days=Config.RETENTION_DAYS)
        # Период хранения для telegram_posts_top (24 часа)
        self.retention_period_top = timedelta(hours=Config.RETENTION_HOURS_TOP)
        # Период хранения для telegram_posts_top_top (72 часа)
        self.retention_period_top_top = timedelta(hours=Config.RETENTION_HOURS_TOP_TOP)
        # Период хранения для Telegram группы (14 дней)
        self.telegram_retention_period = timedelta(days=Config.TELEGRAM_RETENTION_DAYS)
        
        logging.info(f"Cleaner: Служба очистки настроена: интервал {Config.CLEANUP_INTERVAL_HOURS}ч, "
                    f"telegram_posts: {Config.RETENTION_DAYS} дней, "
                    f"telegram_posts_top: {Config.RETENTION_HOURS_TOP} часов, "
                    f"telegram_posts_top_top: {Config.RETENTION_HOURS_TOP_TOP} часов, "
                    f"Telegram группа: {Config.TELEGRAM_RETENTION_DAYS} дней.")

    async def _setup_database(self):
        """Получает общий пул подключений из Database менеджера."""
        logging.info("Cleaner: Получение общего пула подключений...")
        try:
            # Используем общий пул вместо создания нового
            self.db_pool = await Database.get_pool()
            logging.info("Cleaner: Пул подключений получен успешно.")
        except Exception as e:
            logging.critical(f"Cleaner: Ошибка при получении пула БД: {e}")
            raise

    async def _setup_telegram(self):
        """Инициализирует клиент Telegram."""
        logging.info("Cleaner: Инициализация Telegram клиента...")
        try:
            # Создаем клиент Telethon
            # API ID и Hash не требуются для бота, только токен
            self.telegram_client = TelegramClient(
                session='cleaner_session',
                api_id=1,  # Заглушка, не используется для бота
                api_hash='',  # Заглушка, не используется для бота
            ).start(bot_token=Config.PUBLISH_BOT_API_KEY)
            
            logging.info("Cleaner: Telegram клиент инициализирован успешно.")
        except Exception as e:
            logging.error(f"Cleaner: Ошибка при инициализации Telegram клиента: {e}")
            raise

    async def clean_telegram_group(self):
        """
        Очищает сообщения из Telegram группы старше 2 недель.
        """
        if not self.telegram_client:
            logging.error("Cleaner: Невозможно выполнить очистку Telegram, клиент не инициализирован.")
            return

        logging.info("Cleaner: Начало очистки Telegram группы...")
        
        try:
            # Получаем информацию о группе/канале
            entity = await self.telegram_client.get_entity(Config.TELEGRAM_GROUP)
            
            # Рассчитываем дату отсечения
            cutoff_date = datetime.now() - self.telegram_retention_period
            logging.info(f"Cleaner: Удаление сообщений из {Config.TELEGRAM_GROUP} старше {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')}")
            
            deleted_count = 0
            error_count = 0
            
            # Получаем историю сообщений
            async for message in self.telegram_client.iter_messages(entity):
                # Проверяем дату сообщения
                if message.date.replace(tzinfo=None) < cutoff_date:
                    try:
                        # Удаляем сообщение
                        await message.delete()
                        deleted_count += 1
                        logging.debug(f"Cleaner: Удалено сообщение ID {message.id} от {message.date}")
                        
                        # Небольшая задержка чтобы не превысить лимиты API
                        await asyncio.sleep(0.5)
                        
                    except Exception as e:
                        error_count += 1
                        logging.warning(f"Cleaner: Не удалось удалить сообщение ID {message.id}: {e}")
                        # Продолжаем несмотря на ошибки
                        continue
            
            logging.info(f"Cleaner: Очистка Telegram группы завершена. Удалено: {deleted_count}, Ошибок: {error_count}")
            
        except Exception as e:
            logging.error(f"Cleaner: Ошибка при очистке Telegram группы: {e}")

    async def clean_old_posts(self):
        """
        Выполняет SQL-запрос для удаления записей из трех таблиц:
        1. telegram_posts: finished = TRUE и старше RETENTION_DAYS
        2. telegram_posts_top: finished = TRUE и старше RETENTION_HOURS_TOP
        3. telegram_posts_top_top: finished = TRUE и старше RETENTION_HOURS_TOP_TOP
        """
        if not self.db_pool:
            logging.error("Cleaner: Невозможно выполнить очистку, пул БД не инициализирован.")
            return

        # Определяем точки отсечения для всех таблиц
        cutoff_time_posts = datetime.now() - self.retention_period_posts
        cutoff_time_top = datetime.now() - self.retention_period_top
        cutoff_time_top_top = datetime.now() - self.retention_period_top_top
        
        logging.info(f"Cleaner: Запуск очистки БД.")
        logging.info(f"Cleaner: telegram_posts - удаляем до {cutoff_time_posts.strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"Cleaner: telegram_posts_top - удаляем до {cutoff_time_top.strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"Cleaner: telegram_posts_top_top - удаляем до {cutoff_time_top_top.strftime('%Y-%m-%d %H:%M:%S')}")

        total_deleted = 0

        try:
            async with self.db_pool.acquire() as conn:
                # 1) Очистка таблицы telegram_posts
                result_posts = await conn.execute("""
                    DELETE FROM telegram_posts 
                    WHERE finished = TRUE AND post_time < $1
                """, cutoff_time_posts)
                
                # Извлекаем количество удаленных строк из ответа
                deleted_posts = result_posts.split(' ')[1] if len(result_posts.split(' ')) > 1 else '0'
                total_deleted += int(deleted_posts)
                logging.info(f"Cleaner: Из telegram_posts удалено {deleted_posts} записей.")

                # 2) Очистка таблицы telegram_posts_top
                result_top = await conn.execute("""
                    DELETE FROM telegram_posts_top 
                    WHERE finished = TRUE AND post_time < $1
                """, cutoff_time_top)
                
                # Извлекаем количество удаленных строк из ответа
                deleted_top = result_top.split(' ')[1] if len(result_top.split(' ')) > 1 else '0'
                total_deleted += int(deleted_top)
                logging.info(f"Cleaner: Из telegram_posts_top удалено {deleted_top} записей.")

                # 3) Очистка таблицы telegram_posts_top_top
                result_top_top = await conn.execute("""
                    DELETE FROM telegram_posts_top_top 
                    WHERE finished = TRUE AND post_time < $1
                """, cutoff_time_top_top)
                
                # Извлекаем количество удаленных строк из ответа
                deleted_top_top = result_top_top.split(' ')[1] if len(result_top_top.split(' ')) > 1 else '0'
                total_deleted += int(deleted_top_top)
                logging.info(f"Cleaner: Из telegram_posts_top_top удалено {deleted_top_top} записей.")

                logging.info(f"Cleaner: Очистка БД завершена. Всего удалено {total_deleted} записей.")

        except Exception as e:
            logging.error(f"Cleaner: Ошибка при выполнении операции очистки БД: {e}")

    async def _cleanup_loop(self):
        """Асинхронный цикл для регулярного запуска очистки."""
        # Первый запуск сразу, чтобы убедиться в работоспособности и почистить при старте
        await self.clean_old_posts()
        await self.clean_telegram_group()
        
        while True:
            # Ожидаем заданный интервал (24 часа)
            await asyncio.sleep(self.cleanup_interval.total_seconds())
            await self.clean_old_posts()
            await self.clean_telegram_group()

    async def run(self):
        """Инициализирует БД, Telegram и запускает цикл очистки."""
        try:
            await self._setup_database()
            await self._setup_telegram()
            # Запускаем цикл очистки как долгоживущую задачу
            await self._cleanup_loop()
        except Exception as e:
            logging.critical(f"Cleaner: Критическая ошибка в службе очистки. Остановка: {e}")

    async def close(self):
        """Корректно закрывает соединения."""
        if self.telegram_client:
            await self.telegram_client.disconnect()
            logging.info("Cleaner: Telegram клиент отключен.")

async def main():
    """Точка входа для запуска службы очистки."""
    cleaner = DBCleaner()
    try:
        await cleaner.run()
    except KeyboardInterrupt:
        logging.info("Cleaner: Остановка службы по запросу пользователя.")
    finally:
        await cleaner.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Cleaner: Остановка службы по запросу пользователя.")