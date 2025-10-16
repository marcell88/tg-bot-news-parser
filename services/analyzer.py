import asyncio
import logging
import os
import asyncpg
from database import Database
# Импортируем функцию-обработчик из нового модуля
# Внимание: для работы этого файла требуется файл msg_processing/msg_handler.py
from msg_processing.msg_handle import process_message_by_form, process_message_by_essence
from prompts import CONTEXT_THRESHOLD, ESSENCE_THRESHOLD, ESSENCE_MAX


# Настраиваем логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Конфигурация (минимально необходимая для БД) ---
class Config:
    """
    Класс для хранения конфигурации приложения (DB и параметры анализа).
    """
    # --- НАСТРОЙКИ БАЗЫ ДАННЫХ (Копируются из listener.py) ---
    DB_HOST = os.getenv('DB_HOST', 'telegram-parsed-db-marcell88.db-msk0.amvera.tech') 
    DB_PORT = int(os.getenv('DB_PORT', 5432)) 
    DB_NAME = os.getenv('DB_NAME', 'tg-parsed-db') 
    DB_USER = os.getenv('DB_USER', 'marcell') 
    DB_PASS = os.getenv('DB_PASS', '12345') 
    
    # --- НАСТРОЙКИ АНАЛИЗА ---
    # ANALYZER_INTERVAL_SECONDS: Интервал между запусками цикла проверки необработанных постов.
    ANALYZER_INTERVAL_SECONDS = 15
    # BATCH_SIZE: Сколько сообщений выбирать для обработки за один раз.
    BATCH_SIZE = 5

# --- Класс TextAnalyzer ---
class TextAnalyzer:
    """
    Класс-служба, отвечающая за: 
    1. Подключение и мониторинг БД.
    2. Выборку необработанных записей.
    3. Анализ сообщений по форме msg_form для каждой записи.
    4. Анализ сообщений по сути msg_essence для каждой записи.
    """
    def __init__(self):
        self.db_pool = None
        self.analyze_interval = Config.ANALYZER_INTERVAL_SECONDS
        logging.info("Analyzer: Служба анализа настроена для вызова msg_handler.")

    async def _setup_database(self):
        """
        Получает общий пул подключений из Database менеджера.
        """
        logging.info("Analyzer: Получение общего пула подключений...")
        try:
            # Используем общий пул вместо создания нового
            self.db_pool = await Database.get_pool()  # <-- ИЗМЕНИТЬ ЭТУ СТРОКУ
            logging.info("Analyzer: Пул подключений получен успешно.")
        except Exception as e:
            logging.critical(f"Analyzer: Ошибка при получении пула БД: {e}")
            raise

    async def _process_unprocessed_posts(self):
        """
        Выбирает необработанные записи из БД, вызывает обработчик и обновляет БД.
        """
        if not self.db_pool:
            logging.error("Analyzer: Невозможно выполнить анализ, пул БД не инициализирован.")
            return

        posts_to_analyze = []
        try:
            async with self.db_pool.acquire() as conn:
                # 1. Выборка необработанных записей
                posts_to_analyze = await conn.fetch("""
                    SELECT id, text_content 
                    FROM telegram_posts
                    WHERE analyzed = FALSE 
                    ORDER BY post_time ASC 
                    LIMIT $1
                """, Config.BATCH_SIZE)
            
                if not posts_to_analyze:
                    logging.debug("Analyzer: Необработанных постов для анализа не найдено.")
                    return

                logging.info(f"Analyzer: Найдено {len(posts_to_analyze)} постов для обработки.")
                
                # 2. Обработка каждой записи
                for post in posts_to_analyze:

                    post_id = post['id']
                    text = post['text_content']
                    
                    # Формальная фильтрация: первыичный отсев и контекст
                    logging.info(f"Analyzer: начинаем формальную фильтрацию для:{post_id}...")
                    filter_initial, filter_initial_explain, context_score, context_explain = await process_message_by_form(
                        post_id, text
                    )
                    context = context_score >= CONTEXT_THRESHOLD


                    # Фильтрация по сути: первыичный отсев и контекст
                    if context & filter_initial:
                        logging.info(f"Analyzer: начинаем фильтрацию по сути для:{post_id}...")

                        essence_score, essence_max, essence_explain = await process_message_by_essence(
                            post_id, text
                        )
                        essence = (essence_score >= ESSENCE_THRESHOLD) & (essence_max >= ESSENCE_MAX)

                    else:
                        essence = False
                        essence_score = 0.0
                        essence_max = 0.0
                        essence_explain = 'Проверка по сути не проводилась'

                    # 3. Обновление БД с новыми столбцами и пометка finished=TRUE
                    await conn.execute("""
                        UPDATE telegram_posts 
                        SET 
                            filter_initial = $1,
                            filter_initial_explain = $2,
                            context_score = $3,
                            context_explain = $4,
                            context = $5,
                            essence_score = $6,
                            essence_explain = $7,
                            essence = $8,                            
                            analyzed = TRUE
                        WHERE id = $9
                    """, 
                    filter_initial, 
                    filter_initial_explain,
                    context_score,
                    context_explain,
                    context,
                    essence_score,
                    essence_explain,
                    essence,
                    post_id)

        except Exception as e:
            logging.error(f"Analyzer: Ошибка при обработке или выборке из БД: {e}")

    async def _analysis_loop(self):
        """Асинхронный цикл для регулярного запуска анализа."""
        while True:
            await self._process_unprocessed_posts()
            await asyncio.sleep(self.analyze_interval)

    async def run(self):
        """Инициализирует БД и запускает цикл анализа."""
        try:
            await self._setup_database()
            await self._analysis_loop()
        except Exception as e:
            logging.critical(f"Analyzer: Критическая ошибка в службе анализа. Остановка: {e}")

async def main():
    """Точка входа для запуска службы анализа."""
    analyzer = TextAnalyzer()
    await analyzer.run()

if __name__ == "__main__":
    # Запуск анализатора (требует настроенного event loop, например, с помощью asyncio.run)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Analyzer: Остановка службы по запросу пользователя.")
