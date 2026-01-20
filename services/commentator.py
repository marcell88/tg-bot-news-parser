import asyncio
import logging
import asyncpg
import json
import aiohttp
import os
from dotenv import load_dotenv
from database.database import Database
from database.database_config import DatabaseConfig

# Загружаем переменные окружения
load_dotenv()

# Настраиваем логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TopTopConfig:
    """
    Конфигурация для службы работы с telegram_posts_top_top.
    """
    # Настройки базы данных
    DB_HOST = DatabaseConfig.DB_HOST
    DB_PORT = DatabaseConfig.DB_PORT
    DB_NAME = DatabaseConfig.DB_NAME
    DB_USER = DatabaseConfig.DB_USER
    DB_PASS = DatabaseConfig.DB_PASS
    
    # Настройки службы
    TOP_TOP_INTERVAL_SECONDS = 10
    BATCH_SIZE = 3
    
    # URL из .env
    URL_AUTHOR = os.getenv('URL_AUTHOR')
    URL_APPROACH = os.getenv('URL_APPROACH') 
    URL_WRITE = os.getenv('URL_WRITE')
    URL_ADD_TO_TABLE = os.getenv('URL_ADD_TO_TABLE')
    URL_ASSESS = os.getenv('URL_ASSESS')  # Добавлен URL для оценки
    
    API_HEADERS = {
        "Content-Type": "application/json"
    }

class TopTopProcessor:
    """
    Служба для обработки записей в telegram_posts_top_top.
    """
    def __init__(self):
        self.db_pool = None
        self.interval = TopTopConfig.TOP_TOP_INTERVAL_SECONDS
        self.session = None
        logging.info("TopTopProcessor: Служба обработки топ-топ записей инициализирована.")

    async def _setup_database(self):
        """Настройка отдельного подключения к базе данных."""
        logging.info("TopTopProcessor: Получение отдельного пула подключений...")
        try:
            self.db_pool = await Database.get_embedder_pool()
            logging.info("TopTopProcessor: Отдельный пул подключений получен успешно.")
        except Exception as e:
            logging.critical(f"TopTopProcessor: Ошибка при настройке базы данных: {e}")
            raise

    async def _setup_http_session(self):
        """Настраивает HTTP сессию для API запросов."""
        if not self.session:
            self.session = aiohttp.ClientSession()

    def _prepare_text_for_json(self, text: str) -> str:
        """
        Обрабатывает текст для передачи в JSON.
        """
        if not text:
            return ""
        
        # Обрезаем текст до 4000 символов чтобы избежать слишком больших запросов
        processed_text = text[:4000]
        
        # Возвращаем как есть - aiohttp сам правильно сериализует в JSON
        return processed_text

    async def _make_api_request(self, url: str, payload: dict, step_name: str):
        """
        Делает запрос к API и возвращает результат.
        """
        try:
            await self._setup_http_session()
            
            # Логируем payload для отладки
            logging.info(f"\n\n════════════════════════════════════════")
            logging.info(f"TopTopProcessor: ОТПРАВКА ЗАПРОСА {step_name}")
            logging.info(f"URL: {url}")
            logging.info(f"Payload keys: {list(payload.keys())}")
            
            # Логируем типы данных в payload
            for key, value in payload.items():
                logging.info(f"   {key}: type={type(value).__name__}, len={len(str(value)) if hasattr(value, '__len__') else 'N/A'}")
            
            logging.info(f"════════════════════════════════════════\n")
            
            async with self.session.post(
                url,
                headers=TopTopConfig.API_HEADERS,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=300)
            ) as response:
                
                response_text = await response.text()
                
                logging.info(f"\n\n────────────────────────────────────────")
                logging.info(f"TopTopProcessor: ОТВЕТ {step_name}")
                logging.info(f"Status: {response.status}")
                
                if response.status == 200:
                    try:
                        result = json.loads(response_text)
                        logging.info(f"Response type: {type(result)}")
                        
                        if isinstance(result, list):
                            logging.info(f"List length: {len(result)}")
                            if len(result) > 0:
                                logging.info(f"First item: {result[0]}")
                        else:
                            logging.info(f"Response: {result}")
                        
                        logging.info(f"✅ Запрос {step_name} успешен")
                        return result
                        
                    except json.JSONDecodeError:
                        logging.error(f"❌ Ошибка парсинга JSON")
                        logging.error(f"Raw response: {response_text}")
                        return None
                else:
                    logging.error(f"❌ Ошибка API. Status: {response.status}")
                    logging.error(f"Error response: {response_text}")
                    return None
                
                logging.info(f"────────────────────────────────────────\n\n")
                    
        except Exception as e:
            logging.error(f"❌ Исключение при API запросе {step_name}: {e}")
            logging.error(f"   URL: {url}")
            logging.error(f"   Payload: {payload}")
            logging.error(f"   Step: {step_name}")
            return None

    async def _execute_four_step_request(self, text_content: str, request_number: int) -> dict:
        """
        Выполняет четыре последовательных запроса для одного комментария.
        Теперь: AUTHOR -> APPROACH -> WRITE -> ASSESS
        """
        # Сохраняем исходный текст
        original_text = text_content
        
        # Шаг 1: URL_AUTHOR - получаем только автора
        author_payload = {
            "text": self._prepare_text_for_json(original_text)
        }
        
        author_result = await self._make_api_request(
            TopTopConfig.URL_AUTHOR, 
            author_payload, 
            f"AUTHOR #{request_number}"
        )
        
        # Обрабатываем ответ как список с одним объектом
        if not author_result:
            logging.warning(f"❌ TopTopProcessor: Ошибка на шаге AUTHOR #{request_number}")
            return {'author': 'нет', 'comment': 'нет', 'score': 0.0}
        
        # Извлекаем первого элемента из списка
        if isinstance(author_result, list) and len(author_result) > 0:
            author_data = author_result[0]
            if 'author' in author_data:
                author_name = str(author_data['author'])  # Преобразуем в строку
                logging.info(f"✅ AUTHOR #{request_number}: получен автор '{author_name}'")
            else:
                logging.warning(f"❌ TopTopProcessor: Ошибка на шаге AUTHOR #{request_number}")
                logging.warning(f"   Ожидалось поле: 'author'")
                logging.warning(f"   Получено: {author_data}")
                return {'author': 'нет', 'comment': 'нет', 'score': 0.0}
        else:
            logging.warning(f"❌ TopTopProcessor: Ошибка на шаге AUTHOR #{request_number}")
            logging.warning(f"   Ожидался список, получено: {type(author_result)}")
            return {'author': 'нет', 'comment': 'нет', 'score': 0.0}
        
        # Шаг 2: URL_APPROACH - передаем исходный текст + автора, получаем device, structure, goal, idea
        approach_payload = {
            "text": str(original_text),  # Преобразуем в строку
            "author": str(author_name)   # Преобразуем в строку
        }
        
        approach_result = await self._make_api_request(
            TopTopConfig.URL_APPROACH,
            approach_payload,
            f"APPROACH #{request_number}"
        )
        
        if not approach_result:
            logging.warning(f"❌ TopTopProcessor: Ошибка на шаге APPROACH #{request_number}")
            return {'author': 'нет', 'comment': 'нет', 'score': 0.0}
        
        # Извлекаем первый элемент из списка
        if isinstance(approach_result, list) and len(approach_result) > 0:
            approach_data = approach_result[0]
            logging.info(f"✅ APPROACH #{request_number}: получены device, structure, goal, idea")
        else:
            logging.warning(f"❌ TopTopProcessor: Ошибка на шаге APPROACH #{request_number}")
            return {'author': 'нет', 'comment': 'нет', 'score': 0.0}
        
        # Шаг 3: URL_WRITE - передаем исходный текст, автора + данные от APPROACH
        # Убеждаемся, что все значения являются строками
        write_payload = {
            "text": str(original_text),  # Исходный текст
            "author": str(author_name),  # Полученный автор
            "device": str(approach_data.get('device', '')),
            "structure": str(approach_data.get('structure', '')),
            "goal": str(approach_data.get('goal', '')),
            "idea": str(approach_data.get('idea', ''))
        }
        
        write_result = await self._make_api_request(
            TopTopConfig.URL_WRITE,
            write_payload,
            f"WRITE #{request_number}"
        )
        
        if not write_result:
            logging.warning(f"❌ TopTopProcessor: Ошибка на шаге WRITE #{request_number}")
            return {'author': 'нет', 'comment': 'нет', 'score': 0.0}
        
        # Извлекаем первый элемент из списка
        if isinstance(write_result, list) and len(write_result) > 0:
            write_data = write_result[0]
            if 'comment' in write_data and 'author' in write_data:
                write_text = str(write_data['comment'])  # Преобразуем в строку
                write_author = str(write_data['author'])  # Преобразуем в строку
                logging.info(f"✅ WRITE #{request_number}: получен rewrite текст")
                logging.info(f"   Author: {write_author}")
                logging.info(f"   Text length: {len(write_text)}")
            else:
                logging.warning(f"❌ TopTopProcessor: Ошибка на шаге WRITE #{request_number}")
                logging.warning(f"   Ожидались поля: 'comment', 'author'")
                logging.warning(f"   Получено: {write_data}")
                return {'author': 'нет', 'comment': 'нет', 'score': 0.0}
        else:
            logging.warning(f"❌ TopTopProcessor: Ошибка на шаге WRITE #{request_number}")
            return {'author': 'нет', 'comment': 'нет', 'score': 0.0}
        
        # Шаг 4: URL_ASSESS - оценка rewrite текста
        # Убеждаемся, что все значения являются строками
        assess_payload = {
            "text": str(original_text),   # Исходный текст
            "rewrite": str(write_text)    # Текст полученный от WRITE
        }
        
        # Логируем типы данных перед отправкой ASSESS
        logging.info(f"🔍 Проверка типов данных для ASSESS #{request_number}:")
        logging.info(f"   text type: {type(assess_payload['text']).__name__}")
        logging.info(f"   rewrite type: {type(assess_payload['rewrite']).__name__}")
        
        assess_result = await self._make_api_request(
            TopTopConfig.URL_ASSESS,
            assess_payload,
            f"ASSESS #{request_number}"
        )
        
        if not assess_result:
            logging.warning(f"❌ TopTopProcessor: Ошибка на шаге ASSESS #{request_number}")
            return {'author': 'нет', 'comment': 'нет', 'score': 0.0}
        
        # Извлекаем первый элемент из списка
        if isinstance(assess_result, list) and len(assess_result) > 0:
            assess_data = assess_result[0]
            if 'score' in assess_data:
                try:
                    score = float(assess_data['score'])
                    logging.info(f"✅ ASSESS #{request_number}: получен score: {score}")
                    
                    logging.info(f"✅ TopTopProcessor: Четверной запрос #{request_number} УСПЕШНО завершен")
                    logging.info(f"   Author: {write_author}")
                    logging.info(f"   Score: {score}")
                    
                    return {
                        'author': write_author,
                        'comment': write_text,
                        'score': score
                    }
                except (ValueError, TypeError) as e:
                    logging.error(f"❌ TopTopProcessor: Ошибка преобразования score: {e}")
                    logging.error(f"   Score value: {assess_data['score']}")
                    return {'author': 'нет', 'comment': 'нет', 'score': 0.0}
            else:
                logging.warning(f"❌ TopTopProcessor: Ошибка на шаге ASSESS #{request_number}")
                logging.warning(f"   Ожидалось поле: 'score'")
                logging.warning(f"   Получено: {assess_data}")
                return {'author': 'нет', 'comment': 'нет', 'score': 0.0}
        else:
            logging.warning(f"❌ TopTopProcessor: Ошибка на шаге ASSESS #{request_number}")
            return {'author': 'нет', 'comment': 'нет', 'score': 0.0}

    async def _process_single_post(self, post_id: int, text_content: str, conn):
        """
        Обрабатывает одну запись: делает три четверных запроса с ожиданием.
        """
        # Логируем исходный текст из базы
        logging.info(f"\n📖 TopTopProcessor: Исходный текст из БД для поста ID:{post_id}")
        logging.info(f"   Длина: {len(text_content)} символов")
        logging.info(f"   Тип: {type(text_content).__name__}\n")
        
        comments_data = []
        
        # Делаем три четверных запроса (каждый состоит из author->approach->write->assess)
        for i in range(3):
            logging.info(f"🎯 TopTopProcessor: НАЧАЛО четверного запроса #{i+1} для поста ID:{post_id}")
            
            comment_result = await self._execute_four_step_request(text_content, i+1)
            comments_data.append(comment_result)
            
            logging.info(f"🏁 TopTopProcessor: Четверной запрос #{i+1} завершен. Score: {comment_result['score']}\n")
        
        # Находим лучший комментарий (с наибольшим score)
        best_comment = max(comments_data, key=lambda x: x['score'])
        
        # Обновляем запись в БД
        await conn.execute("""
            UPDATE telegram_posts_top_top 
            SET 
                author_1 = $1, comment_1 = $2, comment_score_1 = $3,
                author_2 = $4, comment_2 = $5, comment_score_2 = $6,
                author_3 = $7, comment_3 = $8, comment_score_3 = $9,
                author_best = $10, comment_best = $11, comment_score_best = $12,
                analyzed = TRUE
            WHERE id = $13
        """,
        str(comments_data[0]['author']), str(comments_data[0]['comment']), float(comments_data[0]['score']),
        str(comments_data[1]['author']), str(comments_data[1]['comment']), float(comments_data[1]['score']),
        str(comments_data[2]['author']), str(comments_data[2]['comment']), float(comments_data[2]['score']),
        str(best_comment['author']), str(best_comment['comment']), float(best_comment['score']),
        post_id)
        
        logging.info(f"\n🎉 TopTopProcessor: Пост ID:{post_id} успешно обработан!")
        logging.info(f"   Лучший комментарий: score {best_comment['score']}")
        logging.info(f"   Автор: {best_comment['author']}\n")
        
        # Шаг 5: Отправляем лучшего автора в URL_ADD_TO_TABLE
        await self._send_best_author_to_table(str(best_comment['author']), post_id)

    async def _send_best_author_to_table(self, best_author: str, post_id: int):
        """
        Отправляет лучшего автора в URL_ADD_TO_TABLE.
        """
        if not TopTopConfig.URL_ADD_TO_TABLE:
            logging.info(f"ℹ️  URL_ADD_TO_TABLE не настроен, пропускаем отправку для поста ID:{post_id}")
            return
        
        try:
            payload = {
                'author': str(best_author)  # Преобразуем в строку
            }
            
            logging.info(f"\n📤 TopTopProcessor: Отправка лучшего автора в URL_ADD_TO_TABLE")
            logging.info(f"   Post ID: {post_id}")
            logging.info(f"   Author: {best_author}")
            logging.info(f"   Author type: {type(best_author).__name__}")
            
            result = await self._make_api_request(
                TopTopConfig.URL_ADD_TO_TABLE,
                payload,
                f"ADD_TO_TABLE для поста {post_id}"
            )
            
            if result:
                logging.info(f"✅ TopTopProcessor: Автор '{best_author}' успешно добавлен в таблицу для поста ID:{post_id}")
            else:
                logging.warning(f"⚠️  TopTopProcessor: Не удалось добавить автора в таблицу для поста ID:{post_id}")
                
        except Exception as e:
            logging.error(f"❌ TopTopProcessor: Ошибка при отправке автора в таблицу для поста ID:{post_id}: {e}")

    async def _process_top_top_posts(self):
        """
        Обрабатывает записи из telegram_posts_top_top где analyzed = FALSE.
        """
        if not self.db_pool:
            logging.error("TopTopProcessor: Невозможно выполнить обработку, пул БД не инициализирован.")
            return

        try:
            async with self.db_pool.acquire() as conn:
                # Выборка записей где analyzed = FALSE
                posts_to_process = await conn.fetch("""
                    SELECT id, text_content
                    FROM telegram_posts_top_top 
                    WHERE analyzed = FALSE
                    ORDER BY id ASC 
                    LIMIT $1
                """, TopTopConfig.BATCH_SIZE)
            
                if not posts_to_process:
                    logging.debug("TopTopProcessor: Не найдено записей для обработки.")
                    return

                logging.info(f"\n📊 TopTopProcessor: Найдено {len(posts_to_process)} записей для обработки.\n")
                
                # Обрабатываем каждую запись последовательно
                for post in posts_to_process:
                    post_id = post['id']
                    text_content = post['text_content']
                    
                    # Убеждаемся, что text_content является строкой
                    if not isinstance(text_content, str):
                        logging.warning(f"⚠️  TopTopProcessor: text_content для поста ID:{post_id} не является строкой. Тип: {type(text_content)}")
                        text_content = str(text_content)
                    
                    try:
                        await self._process_single_post(post_id, text_content, conn)
                        
                    except Exception as e:
                        logging.error(f"\n💥 TopTopProcessor: Ошибка обработки записи ID:{post_id}: {e}\n")
                        # Помечаем запись как analyzed даже в случае ошибки
                        await conn.execute("""
                            UPDATE telegram_posts_top_top 
                            SET analyzed = TRUE 
                            WHERE id = $1
                        """, post_id)

        except Exception as e:
            logging.error(f"TopTopProcessor: Ошибка при обработке записей из telegram_posts_top_top: {e}")

    async def _processor_loop(self):
        """Асинхронный цикл для регулярной обработки текстов."""
        while True:
            await self._process_top_top_posts()
            await asyncio.sleep(self.interval)

    async def run(self):
        """Инициализирует БД и запускает цикл обработки."""
        try:
            await self._setup_database()
            await self._processor_loop()
        except Exception as e:
            logging.critical(f"TopTopProcessor: Критическая ошибка в службе. Остановка: {e}")
        finally:
            if self.session:
                await self.session.close()

async def main():
    """Точка входа для запуска службы."""
    processor = TopTopProcessor()
    await processor.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("TopTopProcessor: Остановка службы по запросу пользователя.")