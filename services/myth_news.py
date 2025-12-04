import asyncio
import logging
import asyncpg
from database.database import Database
from database.database_config import DatabaseConfig
from msg_processing.deepseek_service import call_deepseek_api
from prompts import SHORTEN_PROMPT, SHORTEN_SCHEMA, MYTH_PROMPT, MYTH_SCHEMA

# Настраиваем логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ShortenConfig:
    """
    Конфигурация для службы сокращения текстов.
    """
    # Настройки базы данных
    DB_HOST = DatabaseConfig.DB_HOST
    DB_PORT = DatabaseConfig.DB_PORT
    DB_NAME = DatabaseConfig.DB_NAME
    DB_USER = DatabaseConfig.DB_USER
    DB_PASS = DatabaseConfig.DB_PASS
    
    # Настройки службы
    MYTH_INTERVAL_SECONDS = 10
    BATCH_SIZE = 5

class TextShortener:
    """
    Служба для сокращения текстов сообщений через DeepSeek API.
    """
    def __init__(self):
        self.db_pool = None
        self.interval = ShortenConfig.MYTH_INTERVAL_SECONDS
        logging.info("Shortener: Служба сокращения текстов инициализирована.")

    async def _setup_database(self):
        """Настройка подключения к базе данных."""
        logging.info("Shortener: Получение пула подключений...")
        try:
            self.db_pool = await Database.get_pool()
            logging.info("Shortener: Пул подключений получен успешно.")
        except Exception as e:
            logging.critical(f"Shortener: Ошибка при настройке базы данных: {e}")
            raise

    async def _process_shorten_texts(self):
        """
        Обрабатывает записи из telegram_posts_top где myth = FALSE.
        Создает сокращенную версию текста через DeepSeek API.
        """
        if not self.db_pool:
            logging.error("Shortener: Невозможно выполнить обработку, пул БД не инициализирован.")
            return

        try:
            async with self.db_pool.acquire() as conn:
                # Выборка записей где myth = FALSE
                posts_to_shorten = await conn.fetch("""
                    SELECT id, text_content
                    FROM telegram_posts_top 
                    WHERE myth = FALSE
                    ORDER BY id ASC 
                    LIMIT $1
                """, ShortenConfig.BATCH_SIZE)
            
                if not posts_to_shorten:
                    logging.debug("Shortener: Не найдено записей для сокращения текста.")
                    return

                logging.info(f"Shortener: Найдено {len(posts_to_shorten)} записей для сокращения текста.")
                
                for post in posts_to_shorten:
                    post_id = post['id']
                    text_content = post['text_content']
                    
                    try:
                        # 1) Первый запрос: сокращение текста
                        shorten_result = await call_deepseek_api(
                            prompt=SHORTEN_PROMPT,
                            text=text_content,
                            response_schema=SHORTEN_SCHEMA,
                            model_type='deepseek-chat',
                            temperature=0.1,
                            tokens=500
                        )
                        
                        short_text = "ошибка"
                        if shorten_result and 'short_text' in shorten_result:
                            short_text = shorten_result['short_text']
                            logging.info(f"Shortener: Текст для поста ID:{post_id} успешно сокращен. "
                                       f"Длина: {len(short_text)} символов")
                        else:
                            logging.warning(f"Shortener: Не удалось получить сокращенный текст для поста ID:{post_id}")

                        # 2) Второй запрос: оценка мифичности (используем сокращенный текст)
                        myth_score = 0.0
                        if short_text != "ошибка":  # Только если первый запрос успешен
                            try:
                                myth_result = await call_deepseek_api(
                                    prompt=MYTH_PROMPT,
                                    text=short_text,  # Используем сокращенный текст
                                    response_schema=MYTH_SCHEMA,
                                    model_type='deepseek-chat',
                                    temperature=0.1,
                                    tokens=2000
                                )
                                
                                if myth_result and 'myth_score' in myth_result:
                                    myth_score = float(myth_result['myth_score'])
                                    logging.info(f"Shortener: Оценка мифичности для поста ID:{post_id}: {myth_score}")
                                else:
                                    logging.warning(f"Shortener: Не удалось получить оценку мифичности для поста ID:{post_id}")
                            
                            except Exception as e:
                                logging.error(f"Shortener: Ошибка при оценке мифичности для поста ID:{post_id}: {e}")
                                myth_score = 0.0

                        # Обновляем запись в БД
                        await conn.execute("""
                            UPDATE telegram_posts_top 
                            SET text_short = $1, myth_score = $2, myth = TRUE
                            WHERE id = $3
                        """, short_text, myth_score, post_id)
                        
                        logging.info(f"Shortener: Пост ID:{post_id} полностью обработан. "
                                   f"Сокращенный текст: {len(short_text)} символов, "
                                   f"Оценка мифичности: {myth_score}")

                    except Exception as e:
                        logging.error(f"Shortener: Ошибка обработки записи ID:{post_id}: {e}")
                        # Записываем "ошибка" в случае исключения
                        await conn.execute("""
                            UPDATE telegram_posts_top 
                            SET text_short = 'ошибка', myth_score = 0, myth = TRUE
                            WHERE id = $1
                        """, post_id)

        except Exception as e:
            logging.error(f"Shortener: Ошибка при обработке записей из telegram_posts_top: {e}")

    async def _shortener_loop(self):
        """Асинхронный цикл для регулярной обработки текстов."""
        while True:
            await self._process_shorten_texts()
            await asyncio.sleep(self.interval)

    async def run(self):
        """Инициализирует БД и запускает цикл сокращения текстов."""
        try:
            await self._setup_database()
            await self._shortener_loop()
        except Exception as e:
            logging.critical(f"Shortener: Критическая ошибка в службе сокращения текстов. Остановка: {e}")

async def main():
    """Точка входа для запуска службы сокращения текстов."""
    shortener = TextShortener()
    await shortener.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shortener: Остановка службы по запросу пользователя.")