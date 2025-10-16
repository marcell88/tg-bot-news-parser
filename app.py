import asyncio
import logging
# Импортируем функцию main из нового пакета services.
from services.listener import main as run_listener
from services.cleaner import main as run_cleaner
from services.analyzer import main as run_analyzer
from services.finisher import main as run_finisher
from database import Database

# Настраиваем логирование для точки входа.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def main_services():
    """
    Запускает все асинхронные службы (Listener, Cleaner и Analyzer) одновременно.
    """
    logging.info("Основная асинхронная задача запущена. Ожидание завершения всех служб...")
    
    # Используем asyncio.gather для одновременного запуска всех трех независимых служб.
    try:
        await asyncio.gather(
            run_listener(), # Мониторинг Telegram и запись в БД
            run_cleaner(),  # Очистка старых, обработанных записей
            run_analyzer(),  # Анализ новых записей и обновление полей
            run_finisher(),    # Финальная обработка и отправка в Telegram
            return_exceptions=True
        )
    except Exception as e:
        logging.critical(f"Критическая ошибка в main_services: {e}")
    finally:
        # ВСЕГДА закрываем общий пул при завершении
        await Database.close()  # <-- ДОБАВИТЬ ЭТУ СТРОКУ

def start_application():
    """
    Основная функция для запуска всех служб приложения.
    """
    logging.info("Приложение запущено. Запуск служб Telegram Listener, DB Cleaner и Text Analyzer...")
    
    # Запускаем основной асинхронный процесс, который управляет всеми службами.
    asyncio.run(main_services())

if __name__ == '__main__':
    try:
        start_application()
    except KeyboardInterrupt:
        logging.info("Приложение остановлено пользователем (Ctrl+C).")
    except Exception as e:
        logging.critical(f"Непредвиденная ошибка в app.py: {e}")
