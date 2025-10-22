# database/database.py
import asyncpg
import logging
from typing import Optional
from database.database_config import DatabaseConfig

class Database:
    """
    Единый менеджер подключений к БД для всех служб.
    С отдельным пулом для embedder для избежания блокировок.
    """
    _pool: Optional[asyncpg.Pool] = None
    _embedder_pool: Optional[asyncpg.Pool] = None  # <-- ОТДЕЛЬНЫЙ ПУЛ ДЛЯ EMBEDDER
    _initialized = False
    
    @classmethod
    async def get_pool(cls) -> asyncpg.Pool:
        """
        Возвращает общий пул подключений для большинства служб.
        """
        if cls._pool is None:
            logging.info("Создание общего пула подключений к БД...")
            try:
                cls._pool = await asyncpg.create_pool(
                    user=DatabaseConfig.DB_USER,
                    password=DatabaseConfig.DB_PASS,
                    database=DatabaseConfig.DB_NAME,
                    host=DatabaseConfig.DB_HOST,
                    port=DatabaseConfig.DB_PORT,
                    ssl='require',
                    min_size=2,
                    max_size=8,
                    max_inactive_connection_lifetime=60
                )
                logging.info("Общий пул подключений к БД создан успешно")
            except Exception as e:
                logging.critical(f"Ошибка создания пула БД: {e}")
                raise
        
        return cls._pool
    
    @classmethod
    async def get_embedder_pool(cls) -> asyncpg.Pool:
        """
        Возвращает отдельный пул подключений для службы embedder.
        Это предотвращает блокировки при длительных операциях с векторами.
        """
        if cls._embedder_pool is None:
            logging.info("Создание отдельного пула подключений для embedder...")
            try:
                cls._embedder_pool = await asyncpg.create_pool(
                    user=DatabaseConfig.DB_USER,
                    password=DatabaseConfig.DB_PASS,
                    database=DatabaseConfig.DB_NAME,
                    host=DatabaseConfig.DB_HOST,
                    port=DatabaseConfig.DB_PORT,
                    ssl='require',
                    min_size=2,           # Минимальное количество соединений
                    max_size=4,           # Максимальное - меньше чем у основного пула
                    max_inactive_connection_lifetime=120,  # Больше время жизни
                    command_timeout=300,  # Увеличиваем таймаут для долгих операций
                    max_queries=50000     # Больше запросов в соединении
                )
                logging.info("Отдельный пул подключений для embedder создан успешно")
            except Exception as e:
                logging.critical(f"Ошибка создания пула embedder БД: {e}")
                # В случае ошибки возвращаем основной пул
                return await cls.get_pool()
        
        return cls._embedder_pool
    
    @classmethod
    async def initialize_database(cls):
        """
        Инициализация БД: создание таблиц и проверка структуры.
        Должна вызываться ОДИН раз при запуске приложения.
        """
        if cls._initialized:
            return
            
        pool = await cls.get_pool()
        
        async with pool.acquire() as conn:
            # Создаем основную таблицу, если не существует
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS telegram_posts (
                    id BIGSERIAL PRIMARY KEY,
                    post_time TIMESTAMP WITH TIME ZONE NOT NULL, 
                    text_content TEXT NOT NULL,
                    message_link TEXT,
                    finished BOOLEAN DEFAULT FALSE,
                    analyzed BOOLEAN DEFAULT FALSE,
                    filter_initial BOOLEAN,
                    filter_initial_explain TEXT,
                    context BOOLEAN,
                    context_score REAL,
                    context_explain TEXT,
                    essence BOOLEAN,
                    essence_score REAL,
                    essence_explain TEXT                    
                );
            """)
            logging.info("✅ Таблица 'telegram_posts' создана/проверена")
            
            # Проверяем наличие всех столбцов в основной таблице
            await cls._check_table_columns(conn, 'telegram_posts', cls._get_main_table_columns())
            
            # Создаем таблицу для топовых сообщений с новой структурой
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS telegram_posts_top (
                    id BIGSERIAL PRIMARY KEY,
                    post_time TIMESTAMP WITH TIME ZONE NOT NULL, 
                    text_content TEXT NOT NULL,
                    message_link TEXT,
                    
                    tag1 TEXT,
                    tag2 TEXT,
                    tag3 TEXT,
                    tag4 TEXT,
                    tag5 TEXT,
                    
                    vector1 vector(384),
                    vector2 vector(384),
                    vector3 vector(384),
                    vector4 vector(384),
                    vector5 vector(384),
                    
                    taged BOOLEAN DEFAULT FALSE,
                    analyzed BOOLEAN DEFAULT FALSE,
                    coincide_24hr REAL,
                    essence REAL,
                    final_score REAL,
                    final BOOLEAN DEFAULT FALSE,
                    finished BOOLEAN DEFAULT FALSE,

                    subject REAL,
                    action REAL,
                    time_place REAL,
                    reason REAL,
                    source REAL
                );
            """)
            logging.info("✅ Таблица 'telegram_posts_top' создана/проверена")
            
            # Проверяем наличие всех столбцов в таблице топовых сообщений
            await cls._check_table_columns(conn, 'telegram_posts_top', cls._get_top_table_columns())
            
        cls._initialized = True
        logging.info("✅ Инициализация БД завершена")
    
    @classmethod
    def _get_main_table_columns(cls):
        """Возвращает структуру столбцов для основной таблицы."""
        return {
            'id': 'BIGSERIAL PRIMARY KEY',
            'post_time': 'TIMESTAMP WITH TIME ZONE NOT NULL',
            'text_content': 'TEXT NOT NULL',
            'message_link': 'TEXT',
            'finished': 'BOOLEAN DEFAULT FALSE',
            'analyzed': 'BOOLEAN DEFAULT FALSE',
            'filter_initial': 'BOOLEAN',
            'filter_initial_explain': 'TEXT',
            'context': 'BOOLEAN',
            'context_score': 'REAL',
            'context_explain': 'TEXT',
            'essence': 'BOOLEAN',
            'essence_score': 'REAL',
            'essence_explain': 'TEXT'
        }
    
    @classmethod
    def _get_top_table_columns(cls):
        """Возвращает структуру столбцов для таблицы топовых сообщений."""
        return {
            'id': 'BIGSERIAL PRIMARY KEY',
            'post_time': 'TIMESTAMP WITH TIME ZONE NOT NULL',
            'text_content': 'TEXT NOT NULL',
            'message_link': 'TEXT',
            
            'tag1': 'TEXT',
            'tag2': 'TEXT',
            'tag3': 'TEXT',
            'tag4': 'TEXT',
            'tag5': 'TEXT',
            
            'vector1': 'vector(384)',
            'vector2': 'vector(384)',
            'vector3': 'vector(384)',
            'vector4': 'vector(384)',
            'vector5': 'vector(384)',
            
            'taged': 'BOOLEAN DEFAULT FALSE',
            'analyzed': 'BOOLEAN DEFAULT FALSE',
            'coincide_24hr': 'REAL',
            'essence': 'REAL',
            'final_score': 'REAL',
            'final': 'BOOLEAN DEFAULT FALSE',
            'finished': 'BOOLEAN DEFAULT FALSE',

            'subject': 'REAL',
            'action': 'REAL',
            'time_place': 'REAL',
            'reason': 'REAL',
            'source': 'REAL',
        }
    
    @classmethod
    async def _check_table_columns(cls, conn, table_name: str, required_columns: dict):
        """
        Проверяет наличие всех необходимых столбцов в указанной таблице.
        """
        # Получаем информацию о столбцах таблицы
        columns = await conn.fetch("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = $1
        """, table_name)
        
        existing_columns = {row['column_name'] for row in columns}
        missing_columns = set(required_columns.keys()) - existing_columns
        
        if missing_columns:
            logging.warning(f"❌ В таблице '{table_name}' отсутствуют столбцы: {missing_columns}")
            # АВТОМАТИЧЕСКИ ДОБАВЛЯЕМ отсутствующие столбцы
            await cls._add_missing_columns(conn, table_name, missing_columns, required_columns)
        else:
            logging.info(f"✅ Все необходимые столбцы присутствуют в таблице '{table_name}'")
    
    @classmethod
    async def _add_missing_columns(cls, conn, table_name: str, missing_columns: set, column_definitions: dict):
        """
        Добавляет отсутствующие столбцы в указанную таблицу.
        """
        added_columns = []
        
        for column in missing_columns:
            if column in column_definitions:
                try:
                    # Для столбца 'id' не добавляем, так как он PRIMARY KEY
                    if column == 'id':
                        continue
                        
                    await conn.execute(f"""
                        ALTER TABLE {table_name} 
                        ADD COLUMN {column} {column_definitions[column]}
                    """)
                    added_columns.append(column)
                    logging.info(f"✅ Добавлен столбец '{column}' в таблицу '{table_name}'")
                except Exception as e:
                    logging.error(f"❌ Ошибка добавления столбца '{column}' в таблицу '{table_name}': {e}")
        
        if added_columns:
            logging.info(f"✅ Успешно добавлены столбцы в таблицу '{table_name}': {added_columns}")
        else:
            logging.info(f"ℹ️  Не было добавлено новых столбцов в таблицу '{table_name}'")
    
    @classmethod
    async def close(cls):
        """
        Закрывает все пулы подключений.
        Вызывается при завершении приложения.
        """
        if cls._pool:
            await cls._pool.close()
            cls._pool = None
            logging.info("Общий пул подключений к БД закрыт")
            
        if cls._embedder_pool:
            await cls._embedder_pool.close()
            cls._embedder_pool = None
            logging.info("Пул подключений embedder закрыт")
            
        cls._initialized = False