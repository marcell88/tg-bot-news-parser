import asyncio
import logging
import signal
from services.listener import main as run_listener
from services.cleaner import main as run_cleaner
from services.analyzer import main as run_analyzer
from services.finisher import main as run_finisher
from services.stats import main as run_stats
from database import Database

# Настраиваем логирование для точки входа.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ServiceManager:
    """Менеджер для управления всеми службами."""
    
    def __init__(self):
        self.tasks = []
        self.is_running = True
        
        # Обработка сигналов остановки
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Обработчик сигналов остановки."""
        logging.info(f"Получен сигнал остановки {signum}")
        self.is_running = False

    async def start_services(self):
        """Запускает все службы."""
        services = [
            ("Listener", run_listener),
            ("Cleaner", run_cleaner),
            ("Analyzer", run_analyzer),
            ("Finisher", run_finisher),
            ("Stats", run_stats),
        ]
        
        for name, service_func in services:
            task = asyncio.create_task(self._run_service(name, service_func))
            self.tasks.append(task)
            await asyncio.sleep(1)  # Небольшая задержка между запусками

    async def _run_service(self, name: str, service_func):
        """Запускает одну службу с обработкой ошибок."""
        try:
            logging.info(f"🚀 Запуск службы {name}...")
            await service_func()
        except asyncio.CancelledError:
            logging.info(f"Служба {name} остановлена")
        except Exception as e:
            logging.error(f"❌ Ошибка в службе {name}: {e}")

    async def stop_services(self):
        """Останавливает все службы."""
        logging.info("Остановка всех служб...")
        for task in self.tasks:
            if not task.done():
                task.cancel()
        
        # Ждем завершения задач
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # Закрываем соединения с БД
        await Database.close()

    async def run(self):
        """Основной цикл работы."""
        try:
            await self.start_services()
            
            # Держим приложение активным
            while self.is_running:
                await asyncio.sleep(1)
                
        except KeyboardInterrupt:
            logging.info("Получен KeyboardInterrupt")
        finally:
            await self.stop_services()

async def main_services():
    """Запускает все службы через менеджер."""
    manager = ServiceManager()
    await manager.run()

def start_application():
    """Основная функция для запуска приложения."""
    logging.info("🚀 Запуск приложения...")
    
    try:
        asyncio.run(main_services())
    except KeyboardInterrupt:
        logging.info("Приложение остановлено пользователем (Ctrl+C).")
    except Exception as e:
        logging.critical(f"Непредвиденная ошибка в app.py: {e}")
    finally:
        logging.info("Приложение завершило работу.")

if __name__ == '__main__':
    start_application()