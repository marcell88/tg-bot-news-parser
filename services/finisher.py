import asyncio
import logging
import os
import asyncpg
import aiohttp
from datetime import datetime
from database.database import Database
from database.database_config import DatabaseConfig
from prompts import FINAL_SCORE_THRESHOLD, ADJ_THRESHOLD, MIN_SCORE_THRESHOLD
import math  # Добавляем для математических операций


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
    
    # Настройки Telegram Bot API (оставляем для возможного будущего использования)
    TG_BOT_API_KEY = os.getenv('TG_BOT_API_KEY', '')
    TG_PRIVATE_GROUP = os.getenv('TG_PRIVATE_GROUP', '')
    
    # Новый токен для бота-редактора
    EDITOR_BOT_API_KEY = os.getenv('EDITOR_BOT_API_KEY', '')
    EDITOR_BOT_CHAT_ID = '508481456'
    
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

    async def _send_telegram_message(self, chat_id: str, text: str, bot_token: str = None) -> bool:
        """
        Отправляет сообщение в Telegram группу.
        
        Возвращает True если сообщение отправлено успешно, False в случае ошибки.
        """
        token_to_use = bot_token or Config.TG_BOT_API_KEY
        if not token_to_use:
            logging.error("Finisher: Токен бота не настроен, отправка сообщения невозможна")
            return False
            
        if not chat_id:
            logging.error("Finisher: chat_id не указан, отправка сообщения невозможна")
            return False
            
        try:
            await self._setup_http_session()
            
            url = f"https://api.telegram.org/bot{token_to_use}/sendMessage"
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

    def _format_message_for_editor(self, post_data: dict) -> str:
        """
        Форматирует сообщение для отправки боту-редактору.
        """
        try:
            text_short = post_data.get('text_short', '') or ''
            link = post_data.get('message_link', '') or 'нет ссылки'
            author_best = post_data.get('author_best', '') or 'НЕИЗВЕСТНЫЙ АВТОР'
            comment_best = post_data.get('comment_best', '') or ''
            news_final_score = post_data.get('news_final_score', 0) or 0
            comment_score_best = post_data.get('comment_score_best', 0) or 0
            total_score = post_data.get('total_score', 0) or 0
            
            # Определяем формат сообщения в зависимости от comment_score_best
            if comment_score_best >= MIN_SCORE_THRESHOLD:
                # Полный формат с комментарием
                message_parts = [
                    text_short,
                    "\n\n1111\n\n",
                    link,
                    "\n\n1111\n\n",
                    "[как бы] " + author_best.upper(),
                    "\n\n1111\n",
                    comment_best,
                    "\n\n1111\n",
                    f"{news_final_score}",
                    f"{comment_score_best}",
                    f"{total_score}"
                ]
            else:
                # Сокращенный формат без комментария
                message_parts = [
                    text_short,
                    "\n\n1111\n\n",
                    link,
                    "\n\n1111\n",
                    f"{news_final_score}",
                    f"{comment_score_best}",
                    f"{total_score}"
                ]
            
            return "\n".join(message_parts)
            
        except Exception as e:
            logging.error(f"Finisher: Ошибка при форматировании сообщения для редактора: {e}")
            return ""

    async def _send_to_editor_bot(self, post_data: dict) -> bool:
        """
        Отправляет сообщение боту-редактору.
        """
        comment_score_best = post_data.get('comment_score_best', 0) or 0
        total_score = post_data.get('total_score', 0) or 0
                
        if not Config.EDITOR_BOT_API_KEY:
            logging.error("Finisher: EDITOR_BOT_API_KEY не настроен, отправка редактору невозможна")
            return False
                
        # Форматируем сообщение в зависимости от comment_score_best
        message_text = self._format_message_for_editor(post_data)
        if not message_text:
            logging.error(f"Finisher: Не удалось сформировать сообщение для редактора для поста ID:{post_data['id']}")
            return False
                
        # Используем EDITOR_BOT_CHAT_ID если указан, иначе отправляем самому боту
        chat_id = Config.EDITOR_BOT_CHAT_ID or Config.EDITOR_BOT_API_KEY.split(':')[0]
        
        logging.info(f"Finisher: Отправка поста ID:{post_data['id']} боту-редактору (total_score: {total_score}, comment_score_best: {comment_score_best})")
        return await self._send_telegram_message(chat_id, message_text, Config.EDITOR_BOT_API_KEY)


    def _calculate_penalty(self, coincide_24hr: float) -> float:
        """
        Вычисляет штраф для final_score на основе coincide_24hr.
        """
        if coincide_24hr < 0.5:
            penalty = -0.5
        elif coincide_24hr < 0.8:
            penalty = -0.5 + ((2 - (-0.5)) / (0.8 - 0.5)) * (coincide_24hr - 0.5)
        else:
            penalty = 2.0

        return penalty

    def _calculate_bonus(self, myth: float) -> float:
        """
        Вычисляет бонус для final_score на основе myth.
        """
        if myth < 5:
            bonus = 0
        else:
            bonus = 0 + ((2 - 0) / (10 - 5)) * (myth - 5)

        return bonus

    def _calculate_geometric_mean(self, news_final_score: float, comment_score_best: float) -> float:
        """
        Вычисляет среднегеометрическое news_final_score и comment_score_best.
        Округляет до одного знака после точки.
        
        Если один из параметров None, возвращает 0.0.
        """
        if news_final_score is None or comment_score_best is None:
            return 0.0
            
        try:
            # Вычисляем среднегеометрическое
            product = news_final_score * comment_score_best
            if product < 0:
                # Если произведение отрицательное, берем модуль и потом возвращаем знак
                geometric_mean = -math.sqrt(abs(product))
            else:
                geometric_mean = math.sqrt(product)
            
            # Округляем до одного знака после точки
            return round(geometric_mean, 1)
            
        except Exception as e:
            logging.error(f"Finisher: Ошибка при вычислении среднегеометрического: {e}")
            return 0.0

    async def _process_top_top_posts(self):
        """
        Обрабатывает записи из telegram_posts_top_top.
        """
        if not self.db_pool:
            logging.error("Finisher: Невозможно выполнить обработку, пул БД не инициализирован.")
            return

        try:
            async with self.db_pool.acquire() as conn:
                # Выборка записей из telegram_posts_top_top где analyzed = TRUE, а finished = FALSE
                posts_to_process = await conn.fetch("""
                    SELECT id, post_time, text_content, text_short, message_link,
                           news_final_score, comment_score_best, total_score,
                           comment_best, author_best
                    FROM telegram_posts_top_top 
                    WHERE analyzed = TRUE AND finished = FALSE
                    ORDER BY id ASC 
                    LIMIT $1
                """, Config.BATCH_SIZE)
            
                if not posts_to_process:
                    logging.debug("Finisher: Не найдено записей для обработки в telegram_posts_top_top.")
                    return

                logging.info(f"Finisher: Найдено {len(posts_to_process)} записей для обработки в telegram_posts_top_top.")
                
                for post in posts_to_process:
                    post_id = post['id']
                    
                    try:
                        news_final_score = post['news_final_score']
                        comment_score_best = post['comment_score_best']
                        
                        # Вычисляем total_score как среднегеометрическое
                        total_score = round(comment_score_best * 0.7 + news_final_score * 0.3, 1)
                        
                        # Обновляем запись в БД - сначала устанавливаем total_score
                        await conn.execute("""
                            UPDATE telegram_posts_top_top 
                            SET total_score = $1
                            WHERE id = $2
                        """, total_score, post_id)
                        
                        logging.info(f"Finisher: Пост ID:{post_id} обработан в telegram_posts_top_top. "
                                   f"news_final_score: {news_final_score}, "
                                   f"comment_score_best: {comment_score_best}, "
                                   f"total_score: {total_score}")
                        
                        # Подготавливаем данные для отправки редактору
                        post_data = {
                            'id': post_id,
                            'text_short': post['text_short'],
                            'message_link': post['message_link'],
                            'author_best': post['author_best'],
                            'comment_best': post['comment_best'],
                            'news_final_score': news_final_score,
                            'comment_score_best': comment_score_best,
                            'total_score': total_score
                        }
                        
                        # Отправляем боту-редактору если total_score >= порога
                        await self._send_to_editor_bot(post_data)
                        
                        # Помечаем запись как finished после обработки
                        await conn.execute("""
                            UPDATE telegram_posts_top_top 
                            SET finished = TRUE
                            WHERE id = $1
                        """, post_id)
                        
                        logging.info(f"Finisher: Пост ID:{post_id} помечен как finished в telegram_posts_top_top")
                        
                    except Exception as e:
                        logging.error(f"Finisher: Ошибка обработки записи ID:{post_id} в telegram_posts_top_top: {e}")

        except Exception as e:
            logging.error(f"Finisher: Ошибка при обработке записей из telegram_posts_top_top: {e}")

    async def _add_to_top_top_posts(self, conn, post_data: dict):
        """
        Добавляет сообщение в таблицу telegram_posts_top_top.
        """
        try:
            await conn.execute("""
                INSERT INTO telegram_posts_top_top (
                    id, post_time, text_content, text_short, message_link,
                    finished, analyzed, total_score, news_final_score,
                    comment_best, comment_score_best,
                    comment_1, comment_score_1, comment_2, comment_score_2, 
                    comment_3, comment_score_3
                ) VALUES (
                    $1, $2, $3, $4, $5,
                    $6, $7, $8, $9,
                    $10, $11,
                    $12, $13, $14, $15, $16, $17
                )
            """, 
            post_data['id'],
            post_data['post_time'], 
            post_data['text_content'],
            post_data['text_short'],
            post_data['message_link'],
            post_data['finished'],
            post_data['analyzed'],
            post_data['total_score'],
            post_data['news_final_score'],  # Используем переданное значение final_score
            post_data['comment_best'],
            post_data['comment_score_best'],
            post_data['comment_1'],
            post_data['comment_score_1'],
            post_data['comment_2'],
            post_data['comment_score_2'],
            post_data['comment_3'],
            post_data['comment_score_3'])
            
            logging.info(f"Finisher: Сообщение ID:{post_data['id']} добавлено в telegram_posts_top_top")
            
        except Exception as e:
            logging.error(f"Finisher: Ошибка при добавлении в telegram_posts_top_top: {e}")

    async def _process_top_posts(self):
        """
        Обрабатывает записи из telegram_posts_top.
        """
        if not self.db_pool:
            logging.error("Finisher: Невозможно выполнить обработку, пул БД не инициализирован.")
            return

        try:
            async with self.db_pool.acquire() as conn:
                # 1-2) Выборка записей из telegram_posts_top где analyzed = TRUE, а finished = FALSE
                posts_to_process = await conn.fetch("""
                    SELECT id, post_time, text_content, text_short, message_link, 
                           essence, coincide_24hr, myth_score
                    FROM telegram_posts_top 
                    WHERE analyzed = TRUE AND myth = TRUE AND finished = FALSE
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
                        myth = post['myth_score'] or 0.0
                        
                        # final_score = essence - max_fee*coincide_24hr
                        final_score = round(essence - self._calculate_penalty(coincide_24hr) + self._calculate_bonus(myth), 1)
                        
                        # Если где-то null или ошибка - ставим 0
                        if essence is None or coincide_24hr is None or myth is None:
                            final_score = 0.0
                        
                        final = final_score >= ADJ_THRESHOLD
                        
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
                        
                        # 4) Если final = TRUE то добавляем в таблицу telegram_posts_top_top
                        if final:
                            # Подготавливаем данные для добавления в telegram_posts_top_top
                            top_top_post_data = {
                                'id': post['id'],
                                'post_time': post['post_time'],
                                'text_content': post['text_content'],
                                'text_short': post['text_short'] or post['text_content'][:500],  # Если text_short пустой, берем начало text_content
                                'message_link': post['message_link'],
                                'finished': False,
                                'analyzed': False,
                                'total_score': None,
                                'news_final_score': final_score,  # Используем final_score из telegram_posts_top
                                'comment_best': None,
                                'comment_score_best': None,
                                'comment_1': None,
                                'comment_score_1': None,
                                'comment_2': None,
                                'comment_score_2': None,
                                'comment_3': None,
                                'comment_score_3': None
                            }
                            
                            # Добавляем в таблицу telegram_posts_top_top
                            await self._add_to_top_top_posts(conn, top_top_post_data)
                            
                            logging.info(f"Finisher: Сообщение ID:{post_id} с final_score {final_score:.3f} добавлено в telegram_posts_top_top")
                        
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
                    id, post_time, text_content, message_link, essence,
                    tag1, tag2, tag3, tag4, tag5, vector1, vector2, vector3, vector4, vector5, taged,
                    finished, analyzed, coincide_24hr, 
                    final_score, final,
                    tag1_score, tag2_score, tag3_score, tag4_score, tag5_score
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
            # Обрабатываем все три таблицы
            await self._process_top_top_posts()  # Новая логика для telegram_posts_top_top
            await self._process_top_posts()      # Логика для telegram_posts_top
            await self._process_finished_posts() # Логика для telegram_posts
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