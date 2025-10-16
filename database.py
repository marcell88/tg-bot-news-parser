import asyncpg
import os
import logging
from typing import Optional

class Database:
    """
    Единый менеджер подключений к БД для всех служб.
    Все службы используют ОДИН общий пул подключений.
    """
    _pool: Optional[asyncpg.Pool] = None
    
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
                    user=os.getenv('DB_USER', 'marcell'),
                    password=os.getenv('DB_PASS', '12345'),
                    database=os.getenv('DB_NAME', 'tg-parsed-db'),
                    host=os.getenv('DB_HOST', 'telegram-parsed-db-marcell88.db-msk0.amvera.tech'),
                    port=int(os.getenv('DB_PORT', 5432)),
                    ssl='require',
                    min_size=2,      # Минимальное количество подключений
                    max_size=8,      # МАКСИМУМ 8 подключений для ВСЕХ служб
                    max_inactive_connection_lifetime=60  # Закрывать неактивные через 60 сек
                )
                logging.info("Общий пул подключений к БД создан успешно")
            except Exception as e:
                logging.critical(f"Ошибка создания пула БД: {e}")
                raise
        
        return cls._pool
    
    @classmethod
    async def close(cls):
        """
        Закрывает общий пул подключений.
        Вызывается при завершении приложения.
        """
        if cls._pool:
            await cls._pool.close()
            cls._pool = None
            logging.info("Общий пул подключений к БД закрыт")