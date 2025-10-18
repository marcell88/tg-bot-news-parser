import asyncio
import logging
import os 
import asyncpg 
from datetime import datetime, timedelta
# Импортируем общий менеджер БД
from database import Database  # <-- ДОБАВИТЬ ЭТОТ ИМПОРТ

# Настраиваем логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Конфигурация (минимально необходимая для БД и логики очистки) ---
class Config:
    """
    Класс для хранения конфигурации приложения (DB и параметры очистки).
    """
    # --- НАСТРОЙКИ БАЗЫ ДАННЫХ (Копируются из listener.py для автономности) ---
    DB_HOST = os.getenv('DB_HOST', 'tg-parsed-db-2-marcell88.db-msk0.amvera.tech')
    DB_PORT = int(os.getenv('DB_PORT', 5432))
    DB_NAME = os.getenv('DB_NAME', 'tg-parsed-db-2')
    DB_USER = os.getenv('DB_USER', 'marcell')
    DB_PASS = os.getenv('DB_PASS', '12345')
    
    # --- НАСТРОЙКИ ОЧИСТКИ ---
    # CLEANUP_INTERVAL_HOURS: Интервал между запусками задачи очистки (в часах)
    CLEANUP_INTERVAL_HOURS = 2 
    # RETENTION_DAYS: Возраст записей для удаления (в днях), если finished=TRUE
    RETENTION_DAYS = 7 

# --- Класс DBCleaner ---
class DBCleaner:
    """
    Класс-служба, отвечающая за регулярное подключение к БД и удаление
    старых, уже обработанных записей.
    """
    def __init__(self):
        self.db_pool = None
        # Интервал между запусками цикла очистки (2 часа)
        self.cleanup_interval = timedelta(hours=Config.CLEANUP_INTERVAL_HOURS)
        # Период хранения (7 дней)
        self.retention_period = timedelta(days=Config.RETENTION_DAYS)
        logging.info(f"Cleaner: Служба очистки настроена: интервал {Config.CLEANUP_INTERVAL_HOURS}ч, хранение {Config.RETENTION_DAYS} дней.")

    async def _setup_database(self):
        """Получает общий пул подключений из Database менеджера."""
        logging.info("Cleaner: Получение общего пула подключений...")
        try:
            # Используем общий пул вместо создания нового
            self.db_pool = await Database.get_pool()  # <-- ИЗМЕНИТЬ ЭТУ СТРОКУ
            logging.info("Cleaner: Пул подключений получен успешно.")
        except Exception as e:
            logging.critical(f"Cleaner: Ошибка при получении пула БД: {e}")
            raise

    async def clean_old_posts(self):
        """
        Выполняет SQL-запрос для удаления записей, которые:
        1. Имеют статус finished = TRUE.
        2. Старше, чем заданный период хранения (Config.RETENTION_DAYS).
        """
        if not self.db_pool:
            logging.error("Cleaner: Невозможно выполнить очистку, пул БД не инициализирован.")
            return

        # Определяем точку отсечения: время, до которого записи будут удалены
        cutoff_time = datetime.now() - self.retention_period
        
        logging.info(f"Cleaner: Запуск очистки. Удаляем посты, обработанные и созданные до {cutoff_time.strftime('%Y-%m-%d %H:%M:%S')}.")

        try:
            async with self.db_pool.acquire() as conn:
                # SQL-запрос на удаление
                result = await conn.execute("""
                    DELETE FROM telegram_posts 
                    WHERE finished = TRUE AND post_time < $1
                """, cutoff_time)
                
                # Извлекаем количество удаленных строк из ответа
                deleted_rows = result.split(' ')[1] if len(result.split(' ')) > 1 else '0'
                logging.info(f"Cleaner: Очистка завершена. Удалено {deleted_rows} старых записей.")

        except Exception as e:
            logging.error(f"Cleaner: Ошибка при выполнении операции очистки БД: {e}")

    async def _cleanup_loop(self):
        """Асинхронный цикл для регулярного запуска очистки."""
        # Первый запуск сразу, чтобы убедиться в работоспособности и почистить при старте
        await self.clean_old_posts()
        
        while True:
            # Ожидаем заданный интервал
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