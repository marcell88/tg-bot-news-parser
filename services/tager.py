import asyncio
import logging
import os
import asyncpg
from database.database import Database
from database.database_config import DatabaseConfig
from typing import Dict, Any, Optional
from msg_processing.deepseek_service import call_deepseek_api
from prompts import TAGED_PROMPT, TAGED_SCHEMA

# Настраиваем логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Конфигурация ---
class Config:
    """
    Класс для хранения конфигурации приложения.
    """
    # --- НАСТРОЙКИ БАЗЫ ДАННЫХ ---
    DB_HOST = DatabaseConfig.DB_HOST
    DB_PORT = DatabaseConfig.DB_PORT
    DB_NAME = DatabaseConfig.DB_NAME
    DB_USER = DatabaseConfig.DB_USER
    DB_PASS = DatabaseConfig.DB_PASS
    
    # --- НАСТРОЙКИ АНАЛИЗА ---
    ANALYZER_INTERVAL_SECONDS = 15
    BATCH_SIZE = 5
    # Добавляем задержку между запросами к API чтобы не превысить лимиты
    API_DELAY_SECONDS = 1

# --- Класс PostTagger ---
class PostTagger:
    """
    Класс-служба, отвечающая за: 
    1. Подключение к БД
    2. Выборку неразмеченных записей из telegram_posts_top
    3. Вызов DeepSeek API для получения тегов
    4. Обновление записей в БД
    """
    def __init__(self):
        self.db_pool = None
        self.analyze_interval = Config.ANALYZER_INTERVAL_SECONDS
        logging.info("PostTagger: Служба тегирования настроена.")

    async def _setup_database(self):
        """
        Получает общий пул подключений из Database менеджера.
        """
        logging.info("PostTagger: Получение общего пула подключений...")
        try:
            self.db_pool = await Database.get_pool()
            logging.info("PostTagger: Пул подключений получен успешно.")
        except Exception as e:
            logging.critical(f"PostTagger: Ошибка при получении пула БД: {e}")
            raise

    async def _process_untagged_posts(self):
        """
        Выбирает неразмеченные записи из БД, вызывает DeepSeek API и обновляет БД.
        """
        if not self.db_pool:
            logging.error("PostTagger: Невозможно выполнить тегирование, пул БД не инициализирован.")
            return

        posts_to_tag = []
        try:
            async with self.db_pool.acquire() as conn:
                # 1. Выборка неразмеченных записей из telegram_posts_top
                posts_to_tag = await conn.fetch("""
                    SELECT id, text_content 
                    FROM telegram_posts_top
                    WHERE taged = FALSE 
                    ORDER BY post_time ASC 
                    LIMIT $1
                """, Config.BATCH_SIZE)
            
                if not posts_to_tag:
                    logging.debug("PostTagger: Неразмеченных постов для тегирования не найдено.")
                    return

                logging.info(f"PostTagger: Найдено {len(posts_to_tag)} постов для тегирования.")
                
                # 2. Обработка каждой записи
                for post in posts_to_tag:
                    post_id = post['id']
                    text = post['text_content']
                    
                    logging.info(f"PostTagger: Обрабатываем пост {post_id}...")
                    
                    # 3. Вызов DeepSeek API
                    tags = await self._get_tags_from_deepseek(text)
                    
                    if tags:
                        # Обрабатываем возможные None значения и ограничиваем длину
                        tag1 = self._sanitize_tag(tags.get('subject'))
                        tag2 = self._sanitize_tag(tags.get('action'))
                        tag3 = self._sanitize_tag(tags.get('time_place'))
                        tag4 = self._sanitize_tag(tags.get('reason'))
                        tag5 = self._sanitize_tag(tags.get('source'))
                        
                        # 4. Обновление БД с полученными тегами
                        await conn.execute("""
                            UPDATE telegram_posts_top 
                            SET 
                                tag1 = $1,
                                tag2 = $2,
                                tag3 = $3,
                                tag4 = $4,
                                tag5 = $5,
                                taged = TRUE
                            WHERE id = $6
                        """, 
                        tag1, tag2, tag3, tag4, tag5, post_id)
                        
                        logging.info(f"PostTagger: Пост {post_id} успешно размечен. Теги: {tag1}, {tag2}, {tag3}, {tag4}, {tag5}")
                    else:
                        # Если не удалось получить теги, помечаем как обработанное чтобы не зацикливаться
                        await conn.execute("""
                            UPDATE telegram_posts_top 
                            SET taged = TRUE
                            WHERE id = $1
                        """, post_id)
                        logging.error(f"PostTagger: Не удалось получить теги для поста {post_id}. Помечен как обработанный.")
                    
                    # Задержка между запросами к API
                    await asyncio.sleep(Config.API_DELAY_SECONDS)

        except Exception as e:
            logging.error(f"PostTagger: Ошибка при обработке или выборке из БД: {e}")

    def _sanitize_tag(self, tag: Optional[str]) -> Optional[str]:
        """
        Обрабатывает тег: проверяет на None и ограничивает длину.
        
        Args:
            tag: Исходный тег
            
        Returns:
            Обработанный тег или None
        """
        if tag is None:
            return None
        # Ограничиваем длину тега (например, 100 символов)
        return tag.strip()[:100] if tag else None

    async def _get_tags_from_deepseek(self, text: str) -> Optional[Dict[str, str]]:
        """
        Вызывает DeepSeek API для получения тегов.
        
        Args:
            text: Текст для анализа
            
        Returns:
            Словарь с тегами или None в случае ошибки
        """
        try:
            
            # Вызов DeepSeek API
            result = await call_deepseek_api(
                prompt=TAGED_PROMPT,
                text=text,
                response_schema=TAGED_SCHEMA,
                model_type="deepseek-chat",
                temperature=0.2,
                tokens=500,
                verify_ssl=True
            )
            
            return result
            
        except Exception as e:
            logging.error(f"PostTagger: Ошибка при вызове DeepSeek API: {e}")
            return None

    async def _tagging_loop(self):
        """Асинхронный цикл для регулярного запуска тегирования."""
        while True:
            await self._process_untagged_posts()
            await asyncio.sleep(self.analyze_interval)

    async def run(self):
        """Инициализирует БД и запускает цикл тегирования."""
        try:
            await self._setup_database()
            await self._tagging_loop()
        except Exception as e:
            logging.critical(f"PostTagger: Критическая ошибка в службе тегирования. Остановка: {e}")

async def main():
    """Точка входа для запуска службы тегирования."""
    tagger = PostTagger()
    await tagger.run()

if __name__ == "__main__":
    # Запуск службы тегирования
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("PostTagger: Остановка службы по запросу пользователя.")