import asyncio
import logging
import os 
import asyncpg 
from datetime import datetime, timedelta
# Импортируем общий менеджер БД
from database.database import Database
from database.database_config import DatabaseConfig

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
    CLEANUP_INTERVAL_HOURS = 1  # 1) подключался раз в час (а не в два)
    # RETENTION_DAYS: Возраст записей для удаления (в днях), если finished=TRUE
    RETENTION_DAYS = 7 
    # RETENTION_HOURS_TOP: Возраст записей для удаления из telegram_posts_top (в часах)
    RETENTION_HOURS_TOP = 24  # 3) для таблицы telegram_posts_top - старше 24 часов
    # RETENTION_HOURS_TOP_TOP: Возраст записей для удаления из telegram_posts_top_top (в часах)
    RETENTION_HOURS_TOP_TOP = 72  # для таблицы telegram_posts_top_top - старше 72 часов

# --- Класс DBCleaner ---
class DBCleaner:
    """
    Класс-служба, отвечающая за регулярное подключение к БД и удаление
    старых, уже обработанных записей.
    """
    def __init__(self):
        self.db_pool = None
        # Интервал между запусками цикла очистки (1 час)
        self.cleanup_interval = timedelta(hours=Config.CLEANUP_INTERVAL_HOURS)
        # Период хранения для telegram_posts (7 дней)
        self.retention_period_posts = timedelta(days=Config.RETENTION_DAYS)
        # Период хранения для telegram_posts_top (24 часа)
        self.retention_period_top = timedelta(hours=Config.RETENTION_HOURS_TOP)
        # Период хранения для telegram_posts_top_top (72 часа)
        self.retention_period_top_top = timedelta(hours=Config.RETENTION_HOURS_TOP_TOP)
        logging.info(f"Cleaner: Служба очистки настроена: интервал {Config.CLEANUP_INTERVAL_HOURS}ч, "
                    f"telegram_posts: {Config.RETENTION_DAYS} дней, "
                    f"telegram_posts_top: {Config.RETENTION_HOURS_TOP} часов, "
                    f"telegram_posts_top_top: {Config.RETENTION_HOURS_TOP_TOP} часов.")

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
        
        logging.info(f"Cleaner: Запуск очистки.")
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

                logging.info(f"Cleaner: Очистка завершена. Всего удалено {total_deleted} записей.")

        except Exception as e:
            logging.error(f"Cleaner: Ошибка при выполнении операции очистки БД: {e}")

    async def _cleanup_loop(self):
        """Асинхронный цикл для регулярного запуска очистки."""
        # Первый запуск сразу, чтобы убедиться в работоспособности и почистить при старте
        await self.clean_old_posts()
        
        while True:
            # Ожидаем заданный интервал (1 час)
            await asyncio.sleep(self.cleanup_interval.total_seconds())
            await self.clean_old_posts()

    async def run(self):
        """Инициализирует БД и запускает цикл очистки."""
        try:
            await self._setup_database()
            # Запускаем цикл очистки как долгоживущую задачу
            await self._cleanup_loop()
        except Exception as e:
            logging.critical(f"Cleaner: Критическая ошибка в службе очистки. Остановка: {e}")

async def main():
    """Точка входа для запуска службы очистки."""
    cleaner = DBCleaner()
    await cleaner.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Cleaner: Остановка службы по запросу пользователя.")