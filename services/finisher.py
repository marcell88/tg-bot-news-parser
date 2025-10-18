import asyncio
import logging
import os
import asyncpg
import aiohttp
from datetime import datetime
from database import Database

# Настраиваем логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Config:
    """
    Класс для хранения конфигурации приложения.
    """
    # Настройки базы данных
    DB_HOST = os.getenv('DB_HOST', 'tg-parsed-db-2-marcell88.db-msk0.amvera.tech')
    DB_PORT = int(os.getenv('DB_PORT', 5432))
    DB_NAME = os.getenv('DB_NAME', 'tg-parsed-db-2')
    DB_USER = os.getenv('DB_USER', 'marcell')
    DB_PASS = os.getenv('DB_PASS', '12345')
    
    # Настройки Telegram Bot API
    TG_BOT_API_KEY = os.getenv('TG_BOT_API_KEY', '')
    TG_PRIVATE_GROUP = os.getenv('TG_PRIVATE_GROUP', '')
    
    # Настройки финишера
    FINISHER_INTERVAL_SECONDS = 10
    BATCH_SIZE = 10

class MessageFinisher:
    """
    Служба для финальной обработки проанализированных сообщений.
    """
    def __init__(self):
        self.db_pool = None
        self.interval = Config.FINISHER_INTERVAL_SECONDS
        logging.info("Finisher: Служба финализации сообщений инициализирована.")

    async def _setup_database(self):
        logging.info("Finisher: Получение общего пула подключений...")
        try:
            self.db_pool = await Database.get_pool()  # <-- ИЗМЕНИТЬ
            logging.info("Finisher: Пул подключений получен успешно.")
        except Exception as e:
            logging.critical(f"Finisher: Ошибка при настройке базы данных: {e}")
            raise

    async def _send_telegram_message(self, chat_id: str, message: str) -> bool:
        """
        Отправляет сообщение в Telegram через бота.
        """
        if not Config.TG_BOT_API_KEY:
            logging.error("Finisher: TG_BOT_API_KEY не установлен")
            return False
        
        url = f"https://api.telegram.org/bot{Config.TG_BOT_API_KEY}/sendMessage"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json={
                    'chat_id': chat_id,
                    'text': message,
                    'parse_mode': 'HTML'
                }) as response:
                    if response.status == 200:
                        logging.info(f"Finisher: Сообщение отправлено в чат {chat_id}")
                        return True
                    else:
                        error_text = await response.text()
                        logging.error(f"Finisher: Ошибка отправки в Telegram: {error_text}")
                        return False
        except Exception as e:
            logging.error(f"Finisher: Ошибка при отправке в Telegram: {e}")
            return False

    async def _process_finished_posts(self):
        """
        Обрабатывает проанализированные записи и отправляет подходящие в Telegram.
        """
        if not self.db_pool:
            logging.error("Finisher: Невозможно выполнить обработку, пул БД не инициализирован.")
            return

        posts_to_finish = []
        try:
            async with self.db_pool.acquire() as conn:
                # Выборка записей для финализации
                posts_to_finish = await conn.fetch("""
                    SELECT id, text_content, filter_initial, context, essence, message_link, filter_initial_explain, context_explain, essence_explain, essence_score
                    FROM telegram_posts 
                    WHERE finished = FALSE AND analyzed = TRUE
                    ORDER BY post_time ASC 
                    LIMIT $1
                """, Config.BATCH_SIZE)
            
                if not posts_to_finish:
                    logging.debug("Finisher: Не найдено записей для финализации.")
                    return

                logging.info(f"Finisher: Найдено {len(posts_to_finish)} записей для обработки.")
                
                for post in posts_to_finish:
                    post_id = post['id']
                    filter_initial = post['filter_initial']
                    context = post['context']
                    essence = post['essence']
                    text = post['text_content']
                    link = post['message_link']
                    explain = str(post['essence_score']) + '\n\n' + post['essence_explain']
                    
                    # Если не прошли фильтры - просто помечаем как finished
                    if not filter_initial or not context or not essence:
                        await conn.execute("""
                            UPDATE telegram_posts 
                            SET finished = TRUE 
                            WHERE id = $1
                        """, post_id)
                        logging.info(f"Finisher: Пост ID:{post_id} отклонен фильтрами. Помечен как finished.")
                    
                    # Если прошли все фильтры - отправляем в Telegram
                    else:
                        # Отправляем сообщение в Telegram
                        message_sent = await self._send_telegram_message(
                            Config.TG_PRIVATE_GROUP,
                            text + '\n\n' + '1111' + '\n\n' + link + '\n\n' + '1111' + '\n\n' + explain
                        )
                        
                        # Обновляем запись в БД
                        await conn.execute("""
                            UPDATE telegram_posts 
                            SET finished = TRUE
                            WHERE id = $1
                        """, post_id)
                        
                        if message_sent:
                            logging.info(f"Finisher: Пост ID:{post_id} успешно отправлен в Telegram.")
                        else:
                            logging.error(f"Finisher: Ошибка отправки поста ID:{post_id} в Telegram.")

        except Exception as e:
            logging.error(f"Finisher: Ошибка при обработке записей: {e}")

    async def _finisher_loop(self):
        """Асинхронный цикл для регулярной проверки записей."""
        while True:
            await self._process_finished_posts()
            await asyncio.sleep(self.interval)

    async def run(self):
        """Инициализирует БД и запускает цикл финализации."""
        try:
            await self._setup_database()
            await self._finisher_loop()
        except Exception as e:
            logging.critical(f"Finisher: Критическая ошибка в службе финализации. Остановка: {e}")

async def main():
    """Точка входа для запуска службы финализации."""
    finisher = MessageFinisher()
    await finisher.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Finisher: Остановка службы по запросу пользователя.")