# services/embedder.py
import asyncio
import logging
import numpy as np
from sentence_transformers import SentenceTransformer
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
    EMBEDDER_INTERVAL_SECONDS = 5
    BATCH_SIZE = 1
    
    # Используем комбинированную метрику вместо отдельных
    SIMILARITY_METRIC = 'combined'  # 'cosine', 'euclidean', или 'combined'
    
    # Модель для настоящих семантических эмбеддингов
    EMBEDDING_MODEL = 'sentence-transformers/paraphrase-multilingual-mpnet-base-v2'
    EMBEDDING_DIMENSION = 768

class EmbedderService:
    """
    Служба для создания эмбеддингов тегов и расчета сходства.
    Использует настоящие семантические эмбеддинги через sentence-transformers.
    Вся остальная логика остается без изменений.
    """
    def __init__(self):
        self.db_pool = None
        self.interval = Config.EMBEDDER_INTERVAL_SECONDS
        self.is_running = False
        self.similarity_metric = Config.SIMILARITY_METRIC
        self.model = None
        self.embedding_dim = Config.EMBEDDING_DIMENSION
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

    def _load_model(self):
        """Загружает модель для создания эмбеддингов (вызывается при первом использовании)."""
        if self.model is None:
            try:
                logging.info(f"Embedder: Загрузка модели {Config.EMBEDDING_MODEL}...")
                self.model = SentenceTransformer(Config.EMBEDDING_MODEL)
                logging.info("Embedder: Модель загружена успешно.")
            except Exception as e:
                logging.critical(f"Embedder: Ошибка загрузки модели: {e}")
                raise

    def _get_text_embedding(self, text: str) -> list:
        """
        Генерирует настоящий семантический эмбеддинг для текста.
        Заменяет старую хэш-версию на настоящую модель.
        """
        # Проверка пустого текста
        if not text or not text.strip():
            logging.debug("Embedder: Получен пустой текст, возвращаем нулевой вектор")
            return [0.0] * self.embedding_dim

        try:
            # Загружаем модель при первом использовании
            self._load_model()
            
            # Создаем настоящий семантический эмбеддинг
            embedding = self.model.encode([text])[0].tolist()
            
            logging.debug(f"Embedder: Сгенерирован семантический эмбеддинг, размерность: {len(embedding)}")
            return embedding
            
        except Exception as e:
            logging.error(f"Embedder: Ошибка генерации эмбеддинга для текста '{text[:50]}...': {e}")
            return [0.0] * self.embedding_dim

    # ВСЕ ОСТАЛЬНЫЕ МЕТОДЫ ОСТАЮТСЯ БЕЗ ИЗМЕНЕНИЙ
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
            similarity = 1.0 / (1.0 + distance)
            
            return float(similarity)
            
        except Exception as e:
            logging.error(f"Embedder: Ошибка вычисления евклидова сходства: {e}")
            return 0.0

    def _normalized_euclidean_similarity(self, vec1: list, vec2: list) -> float:
        """
        Нормализованное евклидово сходство с предварительной нормализацией векторов.
        Более устойчивая версия евклидовой метрики.
        """
        try:
            if len(vec1) != len(vec2):
                logging.warning(f"Embedder: Разная размерность векторов: {len(vec1)} vs {len(vec2)}")
                min_len = min(len(vec1), len(vec2))
                vec1 = vec1[:min_len]
                vec2 = vec2[:min_len]
            
            v1 = np.array(vec1)
            v2 = np.array(vec2)
            
            # Нормализуем вектора
            norm_v1 = np.linalg.norm(v1)
            norm_v2 = np.linalg.norm(v2)
            
            if norm_v1 == 0 or norm_v2 == 0:
                return 0.0
                
            v1_norm = v1 / norm_v1
            v2_norm = v2 / norm_v2
            
            # Вычисляем расстояние между нормализованными векторами
            distance = np.linalg.norm(v1_norm - v2_norm)
            
            # Преобразуем в схожесть (максимальное расстояние между нормализованными векторами = 2)
            similarity = 1.0 - (distance / 2.0)
            
            return max(0.0, float(similarity))
            
        except Exception as e:
            logging.error(f"Embedder: Ошибка вычисления нормализованного евклидова сходства: {e}")
            return 0.0

    def _adjusted_cosine_similarity(self, vec1: list, vec2: list) -> float:
        """
        Скорректированное косинусное сходство для избежания завышения оценок.
        """
        try:
            cosine_sim = self._cosine_similarity(vec1, vec2)
            
            # Сжимаем верхний диапазон чтобы избежать неадекватно высоких значений
            if cosine_sim > 0.9:
                return 0.85 + (cosine_sim - 0.9) * 0.5
            elif cosine_sim > 0.95:
                return 0.9  # Максимальное ограничение
                
            return cosine_sim
            
        except Exception as e:
            logging.error(f"Embedder: Ошибка вычисления скорректированного косинусного сходства: {e}")
            return 0.0

    def _combined_similarity(self, vec1: list, vec2: list) -> float:
        """
        КОМБИНИРОВАННАЯ МЕТРИКА - основная для использования.
        Сочетает преимущества косинусного и евклидова сходства.
        """
        try:
            # Вычисляем обе метрики
            cosine_sim = self._adjusted_cosine_similarity(vec1, vec2)
            euclidean_sim = self._normalized_euclidean_similarity(vec1, vec2)
            
            # Взвешенная комбинация (можно настроить веса)
            # 0.4 * cosine + 0.6 * euclidean - баланс между семантикой и геометрией
            combined_sim = 0.4 * cosine_sim + 0.6 * euclidean_sim
            
            logging.debug(f"Embedder: Комбинированная метрика - косинус: {cosine_sim:.4f}, "
                         f"евклид: {euclidean_sim:.4f}, результат: {combined_sim:.4f}")
            
            return combined_sim
            
        except Exception as e:
            logging.error(f"Embedder: Ошибка вычисления комбинированного сходства: {e}")
            return 0.0

    def _calculate_similarity(self, vec1: list, vec2: list) -> float:
        """
        Универсальная функция для расчета сходства в зависимости от выбранной метрики.
        ВНИМАНИЕ: Эта функция НЕ проверяет нулевые вектора!
        """
        if self.similarity_metric == 'euclidean':
            return self._euclidean_similarity(vec1, vec2)
        elif self.similarity_metric == 'cosine':
            return self._cosine_similarity(vec1, vec2)
        elif self.similarity_metric == 'combined':
            return self._combined_similarity(vec1, vec2)
        else:  # по умолчанию комбинированная
            return self._combined_similarity(vec1, vec2)

    def _parse_embedding_string(self, embedding_str: str) -> list:
        """
        Парсит строку эмбеддинга в список чисел.
        """
        try:
            if embedding_str is None:
                return [0.0] * self.embedding_dim
                
            # Если уже список - возвращаем как есть (но убедимся, что это float)
            if isinstance(embedding_str, list):
                try:
                    return [float(x) for x in embedding_str]
                except (ValueError, TypeError):
                    logging.warning("Embedder: Ошибка преобразования списка к float, возвращаем нулевой вектор")
                    return [0.0] * self.embedding_dim
            
            # Если это строка (основной случай из БД)
            if isinstance(embedding_str, str):
                # Убираем лишние пробелы и проверяем формат
                embedding_str = embedding_str.strip()
                
                if embedding_str.startswith('[') and embedding_str.endswith(']'):
                    try:
                        # Убираем скобки и разбиваем по запятым
                        content = embedding_str[1:-1].strip()
                        if not content:  # Пустой список []
                            return [0.0] * self.embedding_dim
                            
                        parts = content.split(',')
                        result = []
                        
                        for part in parts:
                            part = part.strip()
                            if part:  # Пропускаем пустые элементы
                                try:
                                    result.append(float(part))
                                except ValueError as e:
                                    logging.warning(f"Embedder: Ошибка преобразования '{part}' в float: {e}")
                                    result.append(0.0)
                        
                        # ПРОВЕРКА РАЗМЕРНОСТИ
                        if len(result) != self.embedding_dim:
                            logging.warning(f"Embedder: Парсинг создал вектор размерности {len(result)}")
                            if len(result) > self.embedding_dim:
                                result = result[:self.embedding_dim]
                            else:
                                result = result + [0.0] * (self.embedding_dim - len(result))
                        
                        # ОТЛАДКА: логируем успешный парсинг
                        if len(result) == self.embedding_dim:
                            logging.debug(f"Embedder: Успешно распарсен вектор")
                            logging.debug(f"Embedder: Первые 3 элемента: {result[:3]}")
                        
                        return result
                        
                    except Exception as e:
                        logging.error(f"Embedder: Ошибка парсинга строки эмбеддинга: {e}")
                        return [0.0] * self.embedding_dim
                else:
                    # Пробуем literal_eval для других форматов
                    import ast
                    try:
                        result = ast.literal_eval(embedding_str)
                        if isinstance(result, list):
                            return [float(x) for x in result]
                        else:
                            return [0.0] * self.embedding_dim
                    except:
                        logging.warning(f"Embedder: Непонятный формат эмбеддинга: {embedding_str[:100]}...")
                        return [0.0] * self.embedding_dim
            else:
                logging.warning(f"Embedder: Неподдерживаемый тип эмбеддинга: {type(embedding_str)}")
                return [0.0] * self.embedding_dim
                    
        except Exception as e:
            logging.error(f"Embedder: Критическая ошибка парсинга эмбеддинга: {e}")
            return [0.0] * self.embedding_dim

    def _is_zero_vector(self, vector: list) -> bool:
        """
        Проверяет, является ли вектор нулевым.
        """
        if not vector:
            return True
        try:
            norm = np.linalg.norm(vector)
            return norm < 0.001
        except:
            return True

    def _calculate_vector_similarity(self, vec1: list, vec2: list) -> float:
        """
        Вычисляет сходство между двумя векторами с проверкой на нулевые векторы.
        Возвращает -1 если хотя бы один вектор нулевой.
        """
        vec1_zero = self._is_zero_vector(vec1)
        vec2_zero = self._is_zero_vector(vec2)
        
        # Если хотя бы один вектор нулевой - возвращаем -1
        if vec1_zero or vec2_zero:
            return -1.0
        
        # Оба вектора ненулевые - вычисляем сходство
        return self._calculate_similarity(vec1, vec2)

    async def _calculate_similarities(self, conn, current_post_id: int, current_embeddings: list) -> dict:
        """
        Вычисляет сходство для каждого тега с наиболее близкой предыдущей записью.
        Находит запись с максимальным средним сходством по всем тегам и берет значения от нее.
        ТОЛЬКО с записями где id < current_post_id И final = TRUE.
        """
        try:
            logging.info(f"Embedder: Поиск предыдущих записей для ID:{current_post_id} (только final = TRUE)")
            
            # Получаем все предыдущие записи (id < current_post_id И final = TRUE) с векторами
            previous_records = await conn.fetch("""
                SELECT id, vector1, vector2, vector3, vector4, vector5
                FROM telegram_posts_top 
                WHERE id < $1 
                AND final = TRUE
                AND vector1 IS NOT NULL 
                AND vector2 IS NOT NULL 
                AND vector3 IS NOT NULL 
                AND vector4 IS NOT NULL 
                AND vector5 IS NOT NULL
                ORDER BY id DESC
                LIMIT 100  -- Ограничиваем для производительности
            """, current_post_id)
            
            if not previous_records:
                logging.debug(f"Embedder: Нет предыдущих записей с final=TRUE для сравнения с ID:{current_post_id}")
                return {
                    'tag1_score': -1.0,
                    'tag2_score': -1.0,
                    'tag3_score': -1.0,
                    'tag4_score': -1.0,
                    'tag5_score': -1.0
                }
            
            logging.info(f"Embedder: Найдено {len(previous_records)} предыдущих записей с final=TRUE для сравнения")
            
            best_record_similarities = None
            best_average_similarity = -1.0
            
            # Для каждой предыдущей записи вычисляем сходство по всем тегам
            for record in previous_records:
                try:
                    # Парсим вектора предыдущей записи ИЗ БД
                    prev_vectors = []
                    for i in range(1, 6):
                        vector_field = f'vector{i}'
                        vector_str = record[vector_field]
                        prev_vector = self._parse_embedding_string(vector_str)
                        prev_vectors.append(prev_vector)
                    
                    # Вычисляем сходство для каждого тега с текущей записью
                    record_similarities = {}
                    valid_similarities = []
                    
                    for i, tag_name in enumerate(['tag1_score', 'tag2_score', 'tag3_score', 'tag4_score', 'tag5_score']):
                        if i < len(current_embeddings) and i < len(prev_vectors):
                            similarity = self._calculate_vector_similarity(current_embeddings[i], prev_vectors[i])
                            record_similarities[tag_name] = similarity
                            
                            # Собираем только валидные (не отрицательные) значения для среднего
                            if similarity != -1.0:
                                valid_similarities.append(similarity)
                    
                    # Вычисляем среднее сходство для этой записи (только по валидным значениям)
                    if valid_similarities:
                        current_average = sum(valid_similarities) / len(valid_similarities)
                    else:
                        current_average = 0.0
                    
                    # Если нашли запись с лучшим средним сходством, сохраняем ее значения
                    if current_average > best_average_similarity:
                        best_average_similarity = current_average
                        best_record_similarities = record_similarities
                        logging.debug(f"Embedder: Найдена более близкая запись ID:{record['id']} со средним сходством: {current_average:.4f}")
                        
                except Exception as e:
                    logging.warning(f"Embedder: Ошибка обработки записи {record['id']}: {e}")
                    continue
            
            # Если не нашли ни одной подходящей записи, возвращаем значения по умолчанию
            if best_record_similarities is None:
                logging.info(f"Embedder: Не найдено подходящих записей с валидными сходствами для ID:{current_post_id}")
                return {
                    'tag1_score': -1.0,
                    'tag2_score': -1.0,
                    'tag3_score': -1.0,
                    'tag4_score': -1.0,
                    'tag5_score': -1.0
                }
            
            logging.info(f"Embedder: Наиболее близкая запись найдена со средним сходством: {best_average_similarity:.4f}")
            logging.info(f"Embedder: Значения сходства для ID:{current_post_id}: {best_record_similarities}")
            
            return best_record_similarities
            
        except Exception as e:
            logging.error(f"Embedder: Ошибка расчета сходства для ID:{current_post_id}: {e}")
            return {
                'tag1_score': -1.0,
                'tag2_score': -1.0,
                'tag3_score': -1.0,
                'tag4_score': -1.0,
                'tag5_score': -1.0
            }

    async def _check_existing_vectors_quality(self, conn, post_id: int) -> bool:
        """
        Проверяет качество существующих векторов.
        Возвращает True если вектора нужно пересчитать.
        """
        try:
            # Получаем существующие вектора
            record = await conn.fetchrow("""
                SELECT vector1, vector2, vector3, vector4, vector5
                FROM telegram_posts_top 
                WHERE id = $1
            """, post_id)
            
            if not record:
                return True
                
            # Проверяем каждый вектор на качество
            for i in range(1, 6):
                vector_field = f'vector{i}'
                vector_str = record[vector_field]
                
                if vector_str is None:
                    logging.info(f"Embedder: Вектор {vector_field} пустой, требуется пересчет")
                    return True
                    
                # Парсим вектор и проверяем его характеристики
                vector = self._parse_embedding_string(vector_str)
                
                # Проверяем размерность
                if len(vector) != self.embedding_dim:
                    logging.info(f"Embedder: Вектор {vector_field} имеет неправильную размерность {len(vector)}, требуется пересчет")
                    return True
                
                # Проверяем на нулевой вектор
                if self._is_zero_vector(vector):
                    logging.info(f"Embedder: Вектор {vector_field} нулевой, требуется пересчет")
                    return True
            
            logging.info(f"Embedder: Существующие вектора для ID:{post_id} в порядке, но все равно пересчитываем")
            return True  # Всегда пересчитываем при analyzed = FALSE
            
        except Exception as e:
            logging.warning(f"Embedder: Ошибка проверки существующих векторов для ID:{post_id}: {e}")
            return True  # При ошибке тоже пересчитываем

    async def _process_tag_embeddings(self):
        """
        Обрабатывает записи и создает эмбеддинги для тегов + рассчитывает сходство.
        ПЕРЕСЧИТЫВАЕТ ВЕКТОРА КАЖДЫЙ РАЗ при analyzed = FALSE, даже если они уже существуют.
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
                        
                        # Проверяем, нужно ли пересчитывать вектора
                        need_recalculation = await self._check_existing_vectors_quality(conn, post_id)
                        
                        if not need_recalculation:
                            logging.info(f"Embedder: Вектора для ID:{post_id} в порядке, но все равно пересчитываем по требованию")
                        
                        # Генерируем эмбеддинги для каждого тега (ВСЕГДА ПЕРЕСЧИТЫВАЕМ)
                        embeddings = []
                        for i in range(1, 6):
                            tag_field = f'tag{i}'
                            tag_text = record[tag_field]
                            
                            # Если тег "нет информации" или пустой - используем нулевой вектор
                            if not tag_text or tag_text.lower() in ['нет информации', 'нет данных', '']:
                                logging.info(f"Embedder: Тег {tag_field} содержит 'нет информации', используем нулевой вектор")
                                embedding = [0.0] * self.embedding_dim
                            else:
                                logging.info(f"Embedder: Генерация семантического эмбеддинга для {tag_field}: '{tag_text}'")
                                embedding = self._get_text_embedding(tag_text)
                                logging.info(f"Embedder: Семантический эмбеддинг для {tag_field} сгенерирован, размерность: {len(embedding)}")
                            
                            embeddings.append(embedding)
                        
                        logging.info(f"Embedder: Все семантические эмбеддинги пересчитаны для ID:{post_id}")
                        
                        # Вычисляем сходство с предыдущими записями (ТОЛЬКО final = TRUE)
                        similarities = await self._calculate_similarities(conn, post_id, embeddings)
                        
                        # ВЫЧИСЛЯЕМ СРЕДНЕЕ АРИФМЕТИЧЕСКОЕ для coincide_24hr (только положительные значения)
                        coincide_24hr = self._calculate_coincide_24hr(similarities)
                        
                        # Обновляем запись в БД с ПЕРЕСЧИТАННЫМИ векторами и сходством
                        await conn.execute("""
                            UPDATE telegram_posts_top 
                            SET 
                                vector1 = $1,
                                vector2 = $2,
                                vector3 = $3, 
                                vector4 = $4,
                                vector5 = $5,
                                tag1_score = $6,
                                tag2_score = $7,
                                tag3_score = $8,
                                tag4_score = $9,
                                tag5_score = $10,
                                coincide_24hr = $11,
                                analyzed = TRUE
                            WHERE id = $12
                        """, 
                        str(embeddings[0]),              # vector1 (ПЕРЕСЧИТАН)
                        str(embeddings[1]),              # vector2 (ПЕРЕСЧИТАН)
                        str(embeddings[2]),              # vector3 (ПЕРЕСЧИТАН)
                        str(embeddings[3]),              # vector4 (ПЕРЕСЧИТАН)
                        str(embeddings[4]),              # vector5 (ПЕРЕСЧИТАН)
                        similarities['tag1_score'],
                        similarities['tag2_score'],
                        similarities['tag3_score'],
                        similarities['tag4_score'],
                        similarities['tag5_score'],
                        coincide_24hr,
                        post_id)
                        
                        logging.info(f"Embedder: Запись ID:{post_id} успешно обработана с ПЕРЕСЧЕТОМ векторов. "
                                f"Сходство: tag1={similarities['tag1_score']:.3f}, "
                                f"tag2={similarities['tag2_score']:.3f}, "
                                f"tag3={similarities['tag3_score']:.3f}, "
                                f"tag4={similarities['tag4_score']:.3f}, "
                                f"tag5={similarities['tag5_score']:.3f}, "
                                f"coincide_24hr={coincide_24hr:.3f}")
                        
                    except Exception as e:
                        logging.error(f"Embedder: Критическая ошибка обработки записи ID:{post_id}: {e}")
                        # Продолжаем обработку следующих записей

        except Exception as e:
            logging.error(f"Embedder: Ошибка при обработке батча: {e}")

    def _calculate_coincide_24hr(self, similarities: dict) -> float:
        """
        Вычисляет среднее арифметическое всех значений сходства.
        Учитывает только положительные или нулевые значения.
        Если все значения отрицательные, возвращает 0.
        """
        values = [
            similarities['tag1_score'],
            similarities['tag2_score'], 
            similarities['tag3_score'],
            similarities['tag4_score'],
            similarities['tag5_score']
        ]
        
        # Фильтруем только положительные или нулевые значения
        positive_values = [v for v in values if v >= 0]
        
        if not positive_values:
            return 0.0
        
        return sum(positive_values) / len(positive_values)

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
        logging.info("=== ЗАПУСК EMBEDDER СЛУЖБЫ (СЕМАНТИЧЕСКИЙ РЕЖИМ) ===")
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Embedder: Остановка службы по запросу пользователя.")
    except Exception as e:
        logging.critical(f"Embedder: Непредвиденная ошибка: {e}")