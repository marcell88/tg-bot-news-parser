import asyncio
import logging
import os
import asyncpg
import aiohttp
from datetime import datetime
from database.database import Database
from database.database_config import DatabaseConfig
from prompts import FINAL_SCORE_THRESHOLD

# Настраиваем логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Config:
    """
    Класс для хранения конфигурации приложения.
    """
    # Настройки базы данных
    DB_HOST = DatabaseConfig.DB_HOST
    DB_PORT = DatabaseConfig.DB_PORT
    DB_NAME = DatabaseConfig.DB_NAME
    DB_USER = DatabaseConfig.DB_USER
    DB_PASS = DatabaseConfig.DB_PASS
    
    # Настройки Telegram Bot API
    TG_BOT_API_KEY = os.getenv('TG_BOT_API_KEY', '')
    TG_PRIVATE_GROUP = os.getenv('TG_PRIVATE_GROUP', '')
    
    # Настройки финишера
    FINISHER_INTERVAL_SECONDS = 10
    BATCH_SIZE = 10
    
    # Константы для расчета final_score
    MAX_FEE = 1.0

class MessageFinisher:
    """
    Служба для финальной обработки проанализированных сообщений.
    """
    def __init__(self):
        self.db_pool = None
        self.interval = Config.FINISHER_INTERVAL_SECONDS
        self.session = None
        logging.info("Finisher: Служба финализации сообщений инициализирована.")

    async def _setup_database(self):
        logging.info("Finisher: Получение общего пула подключений...")
        try:
            self.db_pool = await Database.get_pool()
            logging.info("Finisher: Пул подключений получен успешно.")
        except Exception as e:
            logging.critical(f"Finisher: Ошибка при настройке базы данных: {e}")
            raise

    async def _setup_http_session(self):
        """Настраивает HTTP сессию для отправки сообщений в Telegram."""
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def _send_telegram_message(self, chat_id: str, text: str) -> bool:
        """
        Отправляет сообщение в Telegram группу.
        
        Возвращает True если сообщение отправлено успешно, False в случае ошибки.
        """
        if not Config.TG_BOT_API_KEY:
            logging.error("Finisher: TG_BOT_API_KEY не настроен, отправка сообщения невозможна")
            return False
            
        if not chat_id:
            logging.error("Finisher: chat_id не указан, отправка сообщения невозможна")
            return False
            
        try:
            await self._setup_http_session()
            
            url = f"https://api.telegram.org/bot{Config.TG_BOT_API_KEY}/sendMessage"
            payload = {
                'chat_id': chat_id,
                'text': text,
                'parse_mode': 'HTML'
            }
            
            async with self.session.post(url, json=payload) as response:
                if response.status == 200:
                    logging.info(f"Finisher: Сообщение успешно отправлено в группу {chat_id}")
                    return True
                else:
                    error_text = await response.text()
                    logging.error(f"Finisher: Ошибка отправки сообщения. Status: {response.status}, Error: {error_text}")
                    return False
                    
        except Exception as e:
            logging.error(f"Finisher: Исключение при отправке сообщения в Telegram: {e}")
            return False

    async def _process_top_posts(self):
        """
        Обрабатывает записи из telegram_posts_top.
        
        1) служба смотрит таблицу telegram_posts_top
        2) те строки где analyzed = TRUE, а finished = FALSE
        3) обрабатываем строку и отправляем в Telegram если final = TRUE
        """
        if not self.db_pool:
            logging.error("Finisher: Невозможно выполнить обработку, пул БД не инициализирован.")
            return

        try:
            async with self.db_pool.acquire() as conn:
                # 1-2) Выборка записей из telegram_posts_top где analyzed = TRUE, а finished = FALSE
                posts_to_process = await conn.fetch("""
                    SELECT id, text_content, message_link, essence, coincide_24hr
                    FROM telegram_posts_top 
                    WHERE analyzed = TRUE AND finished = FALSE
                    ORDER BY id ASC 
                    LIMIT $1
                """, Config.BATCH_SIZE)
            
                if not posts_to_process:
                    logging.debug("Finisher: Не найдено записей для обработки в telegram_posts_top.")
                    return

                logging.info(f"Finisher: Найдено {len(posts_to_process)} записей для обработки в telegram_posts_top.")
                
                for post in posts_to_process:
                    post_id = post['id']
                    
                    try:
                        # 3) Обрабатываем строку:
                        essence = post['essence'] or 0.0  # ИСПРАВЛЕНИЕ: essence вместо essence_score
                        coincide_24hr = post['coincide_24hr'] or 0.0
                        
                        # final_score = essence - max_fee*coincide_24hr
                        final_score = essence - (Config.MAX_FEE * coincide_24hr)
                        
                        # Если где-то null или ошибка - ставим 0
                        if essence is None or coincide_24hr is None:
                            final_score = 0.0
                        
                        # final = final_score >= FINAL_SCORE_THRESHOLD
                        final = final_score >= FINAL_SCORE_THRESHOLD
                        
                        # Обновляем запись в БД
                        await conn.execute("""
                            UPDATE telegram_posts_top 
                            SET 
                                final_score = $1,
                                final = $2,
                                finished = TRUE
                            WHERE id = $3
                        """, final_score, final, post_id)
                        
                        logging.info(f"Finisher: Пост ID:{post_id} обработан. "
                                   f"essence: {essence:.3f}, "  # ИСПРАВЛЕНИЕ: essence вместо essence_score
                                   f"coincide_24hr: {coincide_24hr:.3f}, "
                                   f"final_score: {final_score:.3f}, "
                                   f"final: {final}")
                        
                        # 4) Если final = TRUE то пересылка в приватную тг-группу
                        if final:
                            text_content = post['text_content'] or "Нет текста"
                            message_link = post['message_link'] or "Нет ссылки"
                            
                            # Формируем сообщение для Telegram
                            message_text = (
                                f"{text_content}\n\n"
                                f"1111\n\n"
                                f"{message_link}\n\n"
                                f"1111\n\n"
                                f"{final_score:.3f}"
                            )
                            
                            # Отправляем сообщение в Telegram
                            message_sent = await self._send_telegram_message(
                                Config.TG_PRIVATE_GROUP,
                                message_text
                            )
                            
                            if message_sent:
                                logging.info(f"Finisher: Сообщение ID:{post_id} успешно отправлено в Telegram")
                            else:
                                logging.error(f"Finisher: Не удалось отправить сообщение ID:{post_id} в Telegram")
                        
                    except Exception as e:
                        logging.error(f"Finisher: Ошибка обработки записи ID:{post_id}: {e}")
                        # Помечаем запись как finished даже в случае ошибки, чтобы не зацикливаться
                        await conn.execute("""
                            UPDATE telegram_posts_top 
                            SET finished = TRUE 
                            WHERE id = $1
                        """, post_id)

        except Exception as e:
            logging.error(f"Finisher: Ошибка при обработке записей из telegram_posts_top: {e}")

    async def _process_finished_posts(self):
        """
        Обрабатывает проанализированные записи и добавляет подходящие в telegram_posts_top.
        (Оригинальная логика для основной таблицы telegram_posts)
        """
        if not self.db_pool:
            logging.error("Finisher: Невозможно выполнить обработку, пул БД не инициализирован.")
            return

        posts_to_finish = []
        try:
            async with self.db_pool.acquire() as conn:
                # Выборка записей для финализации из основной таблицы
                posts_to_finish = await conn.fetch("""
                    SELECT id, post_time, text_content, filter_initial, context, essence, 
                           message_link, filter_initial_explain, context_explain, 
                           essence_explain, essence_score
                    FROM telegram_posts 
                    WHERE finished = FALSE AND analyzed = TRUE
                    ORDER BY post_time ASC 
                    LIMIT $1
                """, Config.BATCH_SIZE)
            
                if not posts_to_finish:
                    logging.debug("Finisher: Не найдено записей для финализации в telegram_posts.")
                    return

                logging.info(f"Finisher: Найдено {len(posts_to_finish)} записей для обработки в telegram_posts.")
                
                for post in posts_to_finish:
                    post_id = post['id']
                    filter_initial = post['filter_initial']
                    context = post['context']
                    essence = post['essence']
                    
                    # Если не прошли фильтры - просто помечаем как finished
                    if not filter_initial or not context or not essence:
                        await conn.execute("""
                            UPDATE telegram_posts 
                            SET finished = TRUE 
                            WHERE id = $1
                        """, post_id)
                        logging.info(f"Finisher: Пост ID:{post_id} отклонен фильтрами. Помечен как finished.")
                    
                    # Если прошли все фильтры - добавляем в telegram_posts_top
                    else:
                        # Подготавливаем данные для добавления в telegram_posts_top
                        top_post_data = {
                            'id': post['id'],
                            'post_time': post['post_time'],
                            'text_content': post['text_content'],
                            'message_link': post['message_link'],
                            'essence': post['essence_score']  # ИСПРАВЛЕНИЕ: essence_score из telegram_posts -> essence в telegram_posts_top
                        }
                        
                        # Добавляем в таблицу telegram_posts_top
                        await self._add_to_top_posts(conn, top_post_data)
                        
                        # Обновляем запись в основной таблице
                        await conn.execute("""
                            UPDATE telegram_posts 
                            SET finished = TRUE
                            WHERE id = $1
                        """, post_id)
                        
                        logging.info(f"Finisher: Пост ID:{post_id} успешно добавлен в telegram_posts_top.")

        except Exception as e:
            logging.error(f"Finisher: Ошибка при обработке записей из telegram_posts: {e}")

    async def _add_to_top_posts(self, conn, post_data: dict):
        """
        Добавляет сообщение в таблицу telegram_posts_top.
        """
        try:
            await conn.execute("""
                INSERT INTO telegram_posts_top (
                    id, post_time, text_content, message_link, essence,  -- ИСПРАВЛЕНИЕ: essence вместо essence_score
                    tag1, tag2, tag3, tag4, tag5, vector1, vector2, vector3, vector4, vector5, taged,
                    finished, analyzed, coincide_24hr, 
                    final_score, final,
                    subject, action, time_place, reason, source
                ) VALUES (
                    $1, $2, $3, $4, $5,
                    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, FALSE,
                    FALSE, FALSE, NULL,
                    NULL, FALSE,
                    NULL, NULL, NULL, NULL, NULL
                )
            """, 
            post_data['id'],
            post_data['post_time'], 
            post_data['text_content'],
            post_data['message_link'],
            post_data['essence'])  # ИСПРАВЛЕНИЕ: essence вместо essence_score
            
            logging.info(f"Finisher: Сообщение ID:{post_data['id']} добавлено в telegram_posts_top")
            
        except Exception as e:
            logging.error(f"Finisher: Ошибка при добавлении в telegram_posts_top: {e}")

    async def _finisher_loop(self):
        """Асинхронный цикл для регулярной проверки записей."""
        while True:
            # Обрабатываем обе таблицы
            await self._process_top_posts()  # Новая логика для telegram_posts_top
            await self._process_finished_posts()  # Оригинальная логика для telegram_posts
            await asyncio.sleep(self.interval)

    async def run(self):
        """Инициализирует БД и запускает цикл финализации."""
        try:
            await self._setup_database()
            await self._finisher_loop()
        except Exception as e:
            logging.critical(f"Finisher: Критическая ошибка в службе финализации. Остановка: {e}")
        finally:
            if self.session:
                await self.session.close()

async def main():
    """Точка входа для запуска службы финализации."""
    finisher = MessageFinisher()
    await finisher.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Finisher: Остановка службы по запросу пользователя.")