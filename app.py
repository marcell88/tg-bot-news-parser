import asyncio
import logging
# Импортируем функцию main из нового пакета services.
from services.listener import main as run_listener
from services.cleaner import main as run_cleaner
from services.analyzer import main as run_analyzer
from services.finisher import main as run_finisher
from services.stats import main as run_stats  # <-- ДОБАВИТЬ
from database import Database

# Настраиваем логирование для точки входа.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def main_services():
    """
    Запускает все асинхронные службы (Listener, Cleaner, Analyzer, Finisher и Stats) одновременно.
    """
    logging.info("Основная асинхронная задача запущена. Ожидание завершения всех служб...")
    
    # Используем asyncio.gather для одновременного запуска всех пяти независимых служб.
    await asyncio.gather(
        run_listener(),   # Мониторинг Telegram и запись в БД
        run_cleaner(),    # Очистка старых, обработанных записей
        run_analyzer(),   # Анализ новых записей и обновление полей
        run_finisher(),   # Финальная обработка и отправка в Telegram
        run_stats(),      # Бот статистики и управления  # <-- ДОБАВИТЬ
        return_exceptions=True
    )


def start_application():
    """
    Основная функция для запуска всех служб приложения.
    """
    logging.info("Приложение запущено. Запуск служб: Listener, Cleaner, Analyzer, Finisher, Stats...")
    
    # Запускаем основной асинхронный процесс, который управляет всеми службами.
    asyncio.run(main_services())

if __name__ == '__main__':
    try:
        start_application()
    except KeyboardInterrupt:
        logging.info("Приложение остановлено пользователем (Ctrl+C).")
    except Exception as e:
        logging.critical(f"Непредвиденная ошибка в app.py: {e}")