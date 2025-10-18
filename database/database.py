# database/database.py
import asyncpg
import logging
from typing import Optional
from database.database_config import DatabaseConfig

class Database:
    """
    Единый менеджер подключений к БД для всех служб.
    Все службы используют ОДИН общий пул подключений.
    """
    _pool: Optional[asyncpg.Pool] = None
    _initialized = False
    
    @classmethod
    async def get_pool(cls) -> asyncpg.Pool:
        """
        Возвращает общий пул подключений.
        Если пул не создан - создает его.
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
            
            # Проверяем наличие всех столбцов
            await cls._check_table_columns(conn)
            
        cls._initialized = True
        logging.info("✅ Инициализация БД завершена")
    
    @classmethod
    async def _check_table_columns(cls, conn):
        """
        Проверяет наличие всех необходимых столбцов в таблице.
        """
        required_columns = {
            'id', 'post_time', 'text_content', 'message_link', 'finished', 
            'analyzed', 'filter_initial', 'filter_initial_explain', 'context', 
            'context_score', 'context_explain', 'essence', 'essence_score', 'essence_explain'
        }
        
        # Получаем информацию о столбцах таблицы
        columns = await conn.fetch("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'telegram_posts'
        """)
        
        existing_columns = {row['column_name'] for row in columns}
        missing_columns = required_columns - existing_columns
        
        if missing_columns:
            logging.warning(f"❌ Отсутствующие столбцы: {missing_columns}")
            # АВТОМАТИЧЕСКИ ДОБАВЛЯЕМ отсутствующие столбцы
            await cls._add_missing_columns(conn, missing_columns)
        else:
            logging.info("✅ Все необходимые столбцы присутствуют в таблице")
    
    @classmethod
    async def _add_missing_columns(cls, conn, missing_columns):
        """
        Добавляет отсутствующие столбцы в таблицу.
        """
        column_definitions = {
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
        
        added_columns = []
        
        for column in missing_columns:
            if column in column_definitions:
                try:
                    # Для столбца 'id' не добавляем, так как он PRIMARY KEY
                    if column == 'id':
                        continue
                        
                    await conn.execute(f"""
                        ALTER TABLE telegram_posts 
                        ADD COLUMN {column} {column_definitions[column]}
                    """)
                    added_columns.append(column)
                    logging.info(f"✅ Добавлен столбец: {column}")
                except Exception as e:
                    logging.error(f"❌ Ошибка добавления столбца {column}: {e}")
        
        if added_columns:
            logging.info(f"✅ Успешно добавлены столбцы: {added_columns}")
        else:
            logging.info("ℹ️  Не было добавлено новых столбцов")
    
    @classmethod
    async def close(cls):
        """
        Закрывает общий пул подключений.
        Вызывается при завершении приложения.
        """
        if cls._pool:
            await cls._pool.close()
            cls._pool = None
            cls._initialized = False
            logging.info("Общий пул подключений к БД закрыт")