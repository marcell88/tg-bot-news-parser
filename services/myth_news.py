import asyncio
import logging
import asyncpg
import json
from typing import List, Optional, Dict, Any, Tuple
from database.database import Database
from database.database_config import DatabaseConfig
from msg_processing.deepseek_service import call_deepseek_api
from prompts import SHORTEN_PROMPT, SHORTEN_SCHEMA, MYTH_PROMPT, MYTH_SCHEMA, LT_PROMPT, LT_SCHEMA

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

    async def _get_current_lt_data(self, conn) -> Optional[Tuple[List[Dict], List[Dict]]]:
        """Получает текущие LT-данные из таблицы state"""
        try:
            query = """
            SELECT "lt-topic", "lt-mood" 
            FROM state 
            ORDER BY id DESC 
            LIMIT 1
            """
            
            row = await conn.fetchrow(query)
            if not row:
                return None
            
            # Парсим JSON данные
            lt_topics = []
            lt_moods = []
            
            if row['lt-topic']:
                for item in row['lt-topic']:
                    try:
                        lt_topics.append(json.loads(item))
                    except:
                        continue
            
            if row['lt-mood']:
                for item in row['lt-mood']:
                    try:
                        lt_moods.append(json.loads(item))
                    except:
                        continue
            
            if lt_topics or lt_moods:
                logging.debug(f"📊 Загружены LT-данные: {len(lt_topics)} тем, {len(lt_moods)} настроений")
            
            return lt_topics, lt_moods
            
        except Exception as e:
            logging.debug(f"Ошибка при получении LT-данных: {e}")
            return None

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
            
                # Берем текущее распределение  LT тем и настроения
                lt_data = await self._get_current_lt_data(conn)

                if not lt_data:
                    logging.debug("⏳ Нет LT-данных для оценки, ждем...")
                    return
                
                lt_topics, lt_moods = lt_data

                lt_topics_str = "\n".join([f"- {item['topic']} (вес: {item['weight']:.2f})" for item in lt_topics])
                lt_moods_str = "\n".join([f"- {item['mood']} (вес: {item['weight']:.2f})" for item in lt_moods])

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

                        # 3) Третий запрос: оценка диверсификации тем и настроения
                        lt_score = 0.0
                        if short_text != "ошибка":  # Только если первый запрос успешен
                            try:
                                lt_score = await call_deepseek_api(
                                    prompt=LT_PROMPT,
                                    text=f"Текущие LT-темы с весами (частота):\n{lt_topics_str}\n\nТекущие LT-настроения с весами (частота):\n{lt_moods_str}\n\nНовое сообщение: {short_text}",
                                    response_schema=LT_SCHEMA,
                                    model_type='deepseek-chat',
                                    temperature=0.1,
                                    tokens=1000
                                )
                                
                                if lt_score and 'lt_score' in lt_score:
                                    lt_score = float(lt_score['myth_score'])
                                    logging.info(f"Shortener: Оценка мифичности для поста ID:{post_id}: {myth_score}")
                                else:
                                    logging.warning(f"Shortener: Не удалось получить оценку мифичности для поста ID:{post_id}")
                            
                            except Exception as e:
                                logging.error(f"Shortener: Ошибка при оценке мифичности для поста ID:{post_id}: {e}")
                                lt_score = 0.0

                        # Обновляем запись в БД
                        await conn.execute("""
                            UPDATE telegram_posts_top 
                            SET text_short = $1, myth_score = $2, lt_score = $3, myth = TRUE
                            WHERE id = $4
                        """, short_text, myth_score, lt_score, post_id)
                        
                        logging.info(f"Shortener: Пост ID:{post_id} полностью обработан. "
                                   f"Сокращенный текст: {len(short_text)} символов, "
                                   f"Оценка диверсификации: {lt_score} символов, "
                                   f"Оценка мифичности: {myth_score}"),                    

                    except Exception as e:
                        logging.error(f"Shortener: Ошибка обработки записи ID:{post_id}: {e}")
                        # Записываем "ошибка" в случае исключения
                        await conn.execute("""
                            UPDATE telegram_posts_top 
                            SET text_short = 'ошибка', myth_score = 0, lt_score = 0, myth = TRUE
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