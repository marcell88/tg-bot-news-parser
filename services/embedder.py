# services/embedder.py
import asyncio
import logging
import numpy as np
from database.database import Database
from database.database_config import DatabaseConfig

# Настраиваем логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Config:
    """
    Конфигурация для службы эмбеддингов.
    """
    # Настройки базы данных
    DB_HOST = DatabaseConfig.DB_HOST
    DB_PORT = DatabaseConfig.DB_PORT
    DB_NAME = DatabaseConfig.DB_NAME
    DB_USER = DatabaseConfig.DB_USER
    DB_PASS = DatabaseConfig.DB_PASS
    
    # Настройки embedder
    EMBEDDER_INTERVAL_SECONDS = 30
    BATCH_SIZE = 5
    
    # Метрика сравнения: 'cosine' или 'euclidean'
    SIMILARITY_METRIC = 'euclidean'  # Меняйте здесь для тестирования

class EmbedderService:
    """
    Служба для создания эмбеддингов тегов и расчета сходства.
    Поддерживает косинусное сходство и евклидово расстояние.
    """
    def __init__(self):
        self.db_pool = None
        self.interval = Config.EMBEDDER_INTERVAL_SECONDS
        self.model = None
        self.is_running = False
        self.similarity_metric = Config.SIMILARITY_METRIC
        logging.info(f"Embedder: Служба эмбеддингов инициализирована. Метрика: {self.similarity_metric}")

    async def _setup_database(self):
        """Подключается к базе данных через отдельный пул для embedder."""
        logging.info("Embedder: Получение отдельного пула подключений для embedder...")
        try:
            # Используем отдельный пул для embedder
            self.db_pool = await Database.get_embedder_pool()
            logging.info("Embedder: Отдельный пул подключений получен успешно.")
        except Exception as e:
            logging.critical(f"Embedder: Ошибка при настройке базы данных: {e}")
            raise

    async def _load_model(self):
        """Загрузка модели для эмбеддингов."""
        try:
            # Импортируем внутри функции чтобы избежать ошибок если библиотека не установлена
            from sentence_transformers import SentenceTransformer
            
            # Используем мультиязычную модель
            model_name = 'sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2'
            
            logging.info(f"Embedder: Загрузка модели '{model_name}'")
            self.model = SentenceTransformer(model_name)
            
            # Тестируем модель
            test_embedding = self.model.encode(["тестовый текст"]).tolist()
            embedding_dim = len(test_embedding[0])
            
            logging.info(f"Embedder: Модель успешно загружена, размерность: {embedding_dim}")
            
        except ImportError as e:
            logging.error(f"Embedder: Библиотека sentence-transformers не установлена: {e}")
            self.model = "stub"
        except Exception as e:
            logging.error(f"Embedder: Ошибка загрузки модели: {e}")
            self.model = "stub"

    def _get_fallback_embedding(self, text: str) -> list:
        """Заглушечный эмбеддинг для случаев ошибок."""
        if not text:
            return [0.0] * 384
            
        np.random.seed(hash(text) % 2**32)
        return np.random.normal(0, 0.1, 384).tolist()

    async def _get_text_embedding(self, text: str) -> list:
        """
        Генерирует эмбеддинг для текста.
        """
        # Проверка пустого текста
        if not text or not text.strip():
            logging.debug("Embedder: Получен пустой текст, возвращаем нулевой вектор")
            return [0.0] * 384

        try:
            if self.model is None:
                await self._load_model()
            
            if self.model == "stub":
                embedding = self._get_fallback_embedding(text)
                logging.debug("Embedder: Использован заглушечный эмбеддинг")
            else:
                embedding = self.model.encode(text).tolist()
                logging.debug(f"Embedder: Сгенерирован эмбеддинг, размерность: {len(embedding)}")
            
            return embedding
            
        except Exception as e:
            logging.error(f"Embedder: Ошибка генерации эмбеддинга для текста '{text[:50]}...': {e}")
            return self._get_fallback_embedding(text)

    def _cosine_similarity(self, vec1: list, vec2: list) -> float:
        """
        Вычисляет косинусное сходство между двумя векторами.
        Возвращает значение от 0 до 1.
        """
        try:
            if len(vec1) != len(vec2):
                logging.warning(f"Embedder: Разная размерность векторов: {len(vec1)} vs {len(vec2)}")
                # Обрезаем до минимальной длины
                min_len = min(len(vec1), len(vec2))
                vec1 = vec1[:min_len]
                vec2 = vec2[:min_len]
            
            v1 = np.array(vec1)
            v2 = np.array(vec2)
            
            norm_v1 = np.linalg.norm(v1)
            norm_v2 = np.linalg.norm(v2)
            
            if norm_v1 == 0 or norm_v2 == 0:
                return 0.0
                
            cosine_sim = np.dot(v1, v2) / (norm_v1 * norm_v2)
            return float(cosine_sim)
            
        except Exception as e:
            logging.error(f"Embedder: Ошибка вычисления косинусного сходства: {e}")
            return 0.0

    def _euclidean_similarity(self, vec1: list, vec2: list) -> float:
        """
        Вычисляет сходство на основе евклидова расстояния.
        Преобразует расстояние в сходство от 0 до 1.
        
        Чем ближе векторы, тем больше сходство (ближе к 1).
        Чем дальше векторы, тем меньше сходство (ближе к 0).
        """
        try:
            if len(vec1) != len(vec2):
                logging.warning(f"Embedder: Разная размерность векторов: {len(vec1)} vs {len(vec2)}")
                # Обрезаем до минимальной длины
                min_len = min(len(vec1), len(vec2))
                vec1 = vec1[:min_len]
                vec2 = vec2[:min_len]
            
            v1 = np.array(vec1)
            v2 = np.array(vec2)
            
            # Вычисляем евклидово расстояние
            distance = np.linalg.norm(v1 - v2)
            
            # Нормализуем расстояние до диапазона [0, 1]
            # Для эмбеддингов обычно максимальное расстояние ~2-3 (для нормализованных векторов)
            # Используем сигмоиду для преобразования расстояния в сходство
            similarity = 1.0 / (1.0 + distance)
            
            return float(similarity)
            
        except Exception as e:
            logging.error(f"Embedder: Ошибка вычисления евклидова сходства: {e}")
            return 0.0

    def _calculate_similarity(self, vec1: list, vec2: list) -> float:
        """
        Универсальная функция для расчета сходства в зависимости от выбранной метрики.
        """
        if self.similarity_metric == 'euclidean':
            return self._euclidean_similarity(vec1, vec2)
        else:  # по умолчанию косинусное
            return self._cosine_similarity(vec1, vec2)

    def _parse_embedding_string(self, embedding_str: str) -> list:
        """
        Парсит строку эмбеддинга в список чисел.
        """
        try:
            if embedding_str is None:
                return [0.0] * 384
                
            if isinstance(embedding_str, list):
                return embedding_str
                
            if embedding_str.startswith('[') and embedding_str.endswith(']'):
                # Убираем скобки и разбиваем по запятым
                parts = embedding_str[1:-1].split(',')
                return [float(x.strip()) for x in parts if x.strip()]
            else:
                # Пробуем literal_eval для других форматов
                import ast
                try:
                    result = ast.literal_eval(embedding_str)
                    if isinstance(result, list):
                        return result
                    else:
                        return [0.0] * 384
                except:
                    return [0.0] * 384
                    
        except Exception as e:
            logging.error(f"Embedder: Ошибка парсинга эмбеддинга '{embedding_str[:100] if embedding_str else 'None'}...': {e}")
            return [0.0] * 384

    async def _calculate_similarities(self, conn, current_post_id: int, current_embeddings: list) -> dict:
        """
        Вычисляет максимальное сходство для каждого тега с предыдущими записями.
        """
        try:
            logging.info(f"Embedder: Поиск предыдущих записей для ID:{current_post_id}")
            
            # Получаем все предыдущие записи (id < current_post_id) с векторами
            previous_records = await conn.fetch("""
                SELECT id, vector1, vector2, vector3, vector4, vector5
                FROM telegram_posts_top 
                WHERE id < $1 
                AND vector1 IS NOT NULL 
                AND vector2 IS NOT NULL 
                AND vector3 IS NOT NULL 
                AND vector4 IS NOT NULL 
                AND vector5 IS NOT NULL
                ORDER BY id DESC
                LIMIT 100  -- Ограничиваем для производительности
            """, current_post_id)
            
            if not previous_records:
                logging.debug(f"Embedder: Нет предыдущих записей для сравнения с ID:{current_post_id}")
                return {
                    'subject': 0.0,
                    'action': 0.0,
                    'time_place': 0.0,
                    'reason': 0.0,
                    'source': 0.0
                }
            
            logging.info(f"Embedder: Найдено {len(previous_records)} предыдущих записей для сравнения")
            
            # Инициализируем максимальные значения сходства
            max_similarities = {
                'subject': 0.0,
                'action': 0.0,
                'time_place': 0.0,
                'reason': 0.0,
                'source': 0.0
            }
            
            processed_count = 0
            # Для каждой предыдущей записи вычисляем сходство по каждому тегу
            for record in previous_records:
                try:
                    # Парсим вектора предыдущей записи
                    prev_vectors = []
                    for i in range(1, 6):
                        vector_field = f'vector{i}'
                        vector_str = record[vector_field]
                        prev_vectors.append(self._parse_embedding_string(vector_str))
                    
                    # Вычисляем сходство для каждого тега
                    for i, tag_name in enumerate(['subject', 'action', 'time_place', 'reason', 'source']):
                        if i < len(current_embeddings) and i < len(prev_vectors):
                            similarity = self._calculate_similarity(current_embeddings[i], prev_vectors[i])
                            if similarity > max_similarities[tag_name]:
                                max_similarities[tag_name] = similarity
                    
                    processed_count += 1
                            
                except Exception as e:
                    logging.warning(f"Embedder: Ошибка обработки записи {record['id']}: {e}")
                    continue
            
            logging.info(f"Embedder: Обработано {processed_count} записей, максимальное сходство для ID:{current_post_id}: {max_similarities}")
            return max_similarities
            
        except Exception as e:
            logging.error(f"Embedder: Ошибка расчета сходства для ID:{current_post_id}: {e}")
            return {
                'subject': 0.0,
                'action': 0.0,
                'time_place': 0.0,
                'reason': 0.0,
                'source': 0.0
            }

    async def _process_tag_embeddings(self):
        """
        Обрабатывает записи и создает эмбеддинги для тегов + рассчитывает сходство.
        Обрабатывает ВСЕ записи с analyzed = FALSE и taged = TRUE.
        """
        if not self.db_pool:
            logging.error("Embedder: Невозможно выполнить обработку, пул БД не инициализирован.")
            return

        try:
            async with self.db_pool.acquire() as conn:
                # Выборка ВСЕХ записей с analyzed = FALSE и taged = TRUE
                records_to_process = await conn.fetch("""
                    SELECT id, tag1, tag2, tag3, tag4, tag5
                    FROM telegram_posts_top 
                    WHERE taged = TRUE 
                    AND analyzed = FALSE
                    ORDER BY id ASC 
                    LIMIT $1
                """, Config.BATCH_SIZE)
            
                if not records_to_process:
                    logging.debug("Embedder: Не найдено записей для создания эмбеддингов тегов.")
                    return

                logging.info(f"Embedder: Найдено {len(records_to_process)} записей для обработки эмбеддингов.")
                
                for record in records_to_process:
                    post_id = record['id']
                    
                    try:
                        logging.info(f"Embedder: Начинаем обработку записи ID:{post_id}")
                        
                        # Генерируем эмбеддинги для каждого тега
                        embeddings = []
                        for i in range(1, 6):
                            tag_field = f'tag{i}'
                            tag_text = record[tag_field]
                            
                            if tag_text:
                                logging.info(f"Embedder: Генерация эмбеддинга для {tag_field}: '{tag_text}'")
                                embedding = await self._get_text_embedding(tag_text)
                            else:
                                logging.info(f"Embedder: Тег {tag_field} пустой, используем нулевой вектор")
                                embedding = [0.0] * 384  # Пустой вектор для пустого тега
                            
                            embeddings.append(embedding)
                        
                        logging.info(f"Embedder: Все эмбеддинги сгенерированы для ID:{post_id}")
                        
                        # Вычисляем сходство с предыдущими записями
                        similarities = await self._calculate_similarities(conn, post_id, embeddings)
                        
                        # ПРИМЕНЯЕМ ДИСКРЕТИЗАЦИЮ значений сходства
                        discrete_similarities = self._discretize_similarities(similarities)
                        
                        # ВЫЧИСЛЯЕМ СРЕДНЕЕ АРИФМЕТИЧЕСКОЕ для coincide_24hr
                        coincide_24hr = self._calculate_coincide_24hr(discrete_similarities)
                        
                        # Обновляем запись в БД с векторами и сходством
                        await conn.execute("""
                            UPDATE telegram_posts_top 
                            SET 
                                vector1 = $1,
                                vector2 = $2,
                                vector3 = $3, 
                                vector4 = $4,
                                vector5 = $5,
                                subject = $6,
                                action = $7,
                                time_place = $8,
                                reason = $9,
                                source = $10,
                                coincide_24hr = $11,  -- ДОБАВЛЯЕМ новое поле
                                analyzed = TRUE
                            WHERE id = $12
                        """, 
                        str(embeddings[0]),              # vector1
                        str(embeddings[1]),              # vector2  
                        str(embeddings[2]),              # vector3
                        str(embeddings[3]),              # vector4
                        str(embeddings[4]),              # vector5
                        discrete_similarities['subject'], # subject (дискретизированное)
                        discrete_similarities['action'],  # action (дискретизированное)
                        discrete_similarities['time_place'],  # time_place (дискретизированное)
                        discrete_similarities['reason'],    # reason (дискретизированное)
                        discrete_similarities['source'],    # source (дискретизированное)
                        coincide_24hr,                    # coincide_24hr (среднее арифметическое)
                        post_id)
                        
                        logging.info(f"Embedder: Запись ID:{post_id} успешно обработана. "
                                f"Сходство: subject={discrete_similarities['subject']:.1f}, "
                                f"action={discrete_similarities['action']:.1f}, "
                                f"time_place={discrete_similarities['time_place']:.1f}, "
                                f"reason={discrete_similarities['reason']:.1f}, "
                                f"source={discrete_similarities['source']:.1f}, "
                                f"coincide_24hr={coincide_24hr:.3f}")
                        
                    except Exception as e:
                        logging.error(f"Embedder: Критическая ошибка обработки записи ID:{post_id}: {e}")
                        # Продолжаем обработку следующих записей

        except Exception as e:
            logging.error(f"Embedder: Ошибка при обработке батча: {e}")

    def _discretize_similarity(self, similarity: float) -> float:
        """
        Применяет дискретизацию к значению сходства по заданным правилам.
        
        Правила:
        - 0.7-1.0 → 1.0
        - 0.4-0.7 → 0.7  
        - 0.2-0.4 → 0.3
        - 0.0-0.2 → 0.0
        """
        if similarity >= 0.7:
            return 1.0
        elif similarity >= 0.4:
            return 0.7
        elif similarity >= 0.2:
            return 0.3
        else:
            return 0.0

    def _discretize_similarities(self, similarities: dict) -> dict:
        """
        Применяет дискретизацию ко всем значениям сходства.
        """
        return {
            'subject': self._discretize_similarity(similarities['subject']),
            'action': self._discretize_similarity(similarities['action']),
            'time_place': self._discretize_similarity(similarities['time_place']),
            'reason': self._discretize_similarity(similarities['reason']),
            'source': self._discretize_similarity(similarities['source'])
        }

    def _calculate_coincide_24hr(self, discrete_similarities: dict) -> float:
        """
        Вычисляет среднее арифметическое всех дискретизированных значений сходства.
        """
        values = [
            discrete_similarities['subject'],
            discrete_similarities['action'], 
            discrete_similarities['time_place'],
            discrete_similarities['reason'],
            discrete_similarities['source']
        ]
        
        return sum(values) / len(values)

    async def _embedder_loop(self):
        """Асинхронный цикл для регулярной обработки эмбеддингов."""
        while self.is_running:
            try:
                await self._process_tag_embeddings()
            except Exception as e:
                logging.error(f"Embedder: Ошибка в основном цикле: {e}")
            await asyncio.sleep(self.interval)
    async def run(self):
        """Основной метод запуска службы."""
        try:
            logging.info("Embedder: Запуск службы...")
            await self._setup_database()
            self.is_running = True
            logging.info("Embedder: Служба запущена, начинаем цикл обработки")
            await self._embedder_loop()
        except Exception as e:
            logging.critical(f"Embedder: Критическая ошибка в службе эмбеддингов. Остановка: {e}")
        finally:
            self.is_running = False
            logging.info("Embedder: Служба остановлена")

async def main():
    """Точка входа для запуска службы эмбеддингов."""
    embedder = EmbedderService()
    await embedder.run()

if __name__ == "__main__":
    try:
        logging.info("=== ЗАПУСК EMBEDDER СЛУЖБЫ ===")
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Embedder: Остановка службы по запросу пользователя.")
    except Exception as e:
        logging.critical(f"Embedder: Непредвиденная ошибка: {e}")