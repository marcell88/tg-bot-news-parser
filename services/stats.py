# services/stats.py
import asyncio
import logging
import os
import asyncpg
import json
import re
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
from database.database import Database
from database.database_config import DatabaseConfig

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Config:
    """
    Конфигурация для службы статистики.
    """
    # Настройки базы данных
    DB_HOST = DatabaseConfig.DB_HOST
    DB_PORT = DatabaseConfig.DB_PORT
    DB_NAME = DatabaseConfig.DB_NAME
    DB_USER = DatabaseConfig.DB_USER
    DB_PASS = DatabaseConfig.DB_PASS
    
    # Настройки Telegram Bot API
    STATS_BOT_API_KEY = os.getenv('STATS_BOT_API_KEY', '')
    
    # Файл с каналами
    CHANNELS_FILE = 'monitored_channels.json'

class StatsService:
    """
    Служба для сбора статистики и управления ботом.
    """
    def __init__(self):
        self.db_pool = None
        self.bot_app = None
        self.is_running = False
        logging.info("StatsService: Служба статистики инициализирована.")

    async def _setup_database(self):
        """Подключается к базе данных."""
        logging.info("StatsService: Подключение к БД...")
        try:
            self.db_pool = await Database.get_pool()
            logging.info("StatsService: Пул подключений к БД получен успешно.")
        except Exception as e:
            logging.critical(f"StatsService: Ошибка при подключении к БД: {e}")
            raise

    async def _setup_bot(self):
        """Настраивает Telegram бота."""
        if not Config.STATS_BOT_API_KEY:
            logging.error("StatsService: STATS_BOT_API_KEY не установлен")
            return False

        try:
            self.bot_app = Application.builder().token(Config.STATS_BOT_API_KEY).build()
            
            # Добавляем обработчики команд
            self.bot_app.add_handler(CommandHandler("start", self._start_command))
            self.bot_app.add_handler(CommandHandler("stats", self._stats_command))
            self.bot_app.add_handler(CommandHandler("channels", self._channels_command))
            self.bot_app.add_handler(CommandHandler("distr", self._distr_command))
            
            logging.info("StatsService: Бот настроен успешно.")
            return True
        except Exception as e:
            logging.error(f"StatsService: Ошибка при настройке бота: {e}")
            return False

    async def _start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start."""
        welcome_text = """
🤖 Бот статистики системы мониторинга

Доступные команды:

/stats - Показать общую статистику обработки
/distr - Статистика распределения по essence_score (например /distr 5.0)
/channels - Список мониторящихся каналов

Система автоматически собирает новости и анализирует их с помощью AI.
        """
        await update.message.reply_text(welcome_text)

    async def _channels_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /channels."""
        try:
            if not os.path.exists(Config.CHANNELS_FILE):
                await update.message.reply_text("📋 Файл с каналами не найден.")
                return

            with open(Config.CHANNELS_FILE, 'r', encoding='utf-8') as f:
                channels_data = json.load(f)

            if not channels_data:
                await update.message.reply_text("📋 Нет каналов для мониторинга.")
                return

            # Формируем сообщение с каналами
            message = "📋 Мониторящиеся каналы:\n\n"
            for i, channel in enumerate(channels_data[:50], 1):  # Ограничиваем 50 каналами
                username = f"@{channel['username']}" if channel['username'] else "без username"
                message += f"{i}. {channel['title']}\n   ID: {channel['id']} • {username}\n\n"

            if len(channels_data) > 50:
                message += f"\n... и еще {len(channels_data) - 50} каналов"

            await update.message.reply_text(message)

        except Exception as e:
            logging.error(f"Ошибка при обработке /channels: {e}")
            await update.message.reply_text("❌ Ошибка при получении списка каналов.")

    async def _get_database_size(self) -> str:
        """Получает размер базы данных в читаемом формате."""
        try:
            async with self.db_pool.acquire() as conn:
                # Запрос для получения размера БД
                size_bytes = await conn.fetchval("SELECT pg_database_size($1)", Config.DB_NAME)
                
                # Конвертируем в читаемый формат
                if size_bytes >= 1024**3:  # GB
                    return f"{size_bytes / (1024**3):.2f} GB"
                elif size_bytes >= 1024**2:  # MB
                    return f"{size_bytes / (1024**2):.2f} MB"
                elif size_bytes >= 1024:  # KB
                    return f"{size_bytes / 1024:.2f} KB"
                else:
                    return f"{size_bytes} bytes"
                    
        except Exception as e:
            logging.error(f"Ошибка при получении размера БД: {e}")
            return "неизвестно"

    async def _distr_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /distr для статистики по essence_score."""
        try:
            if not self.db_pool:
                await update.message.reply_text("❌ База данных не подключена.")
                return

            # Проверяем, передан ли параметр
            if not context.args:
                await update.message.reply_text(
                    "❌ Укажите пороговое значение для essence_score\n"
                    "Например: /distr 5.0 или /distr 7.5"
                )
                return

            # Получаем число из аргументов команды
            score_text = context.args[0]
            
            try:
                min_score = float(score_text)
                if min_score < 0 or min_score > 10:
                    await update.message.reply_text("❌ Число должно быть от 0 до 10")
                    return
            except ValueError:
                await update.message.reply_text("❌ Неверный формат числа. Используйте: /distr 5.0 или /distr 6.1")
                return

            async with self.db_pool.acquire() as conn:
                # Общее количество записей
                total_count = await conn.fetchval("SELECT COUNT(*) FROM telegram_posts")
                
                # Количество записей с essence_score >= указанного значения
                score_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM telegram_posts WHERE essence_score >= $1", 
                    min_score
                )
                
                # Рассчитываем процент
                score_percent = (score_count / total_count * 100) if total_count > 0 else 0

            # Формируем сообщение
            score_message = f"""
🎯 Статистика распределения по essence_score

Запрос: сообщения с оценкой >= {min_score}

Результат:
• Больше {min_score} баллов: {score_percent:.1f}% ({score_count}/{total_count})
• Всего сообщений в базе: {total_count}
            """.strip()

            await update.message.reply_text(score_message)

        except Exception as e:
            logging.error(f"Ошибка при обработке /distr {score_text}: {e}")
            await update.message.reply_text("❌ Ошибка при получении статистики по оценкам.")

    async def _stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /stats."""
        try:
            if not self.db_pool:
                await update.message.reply_text("❌ База данных не подключена.")
                return

            async with self.db_pool.acquire() as conn:
                # Общее количество записей
                total_count = await conn.fetchval("SELECT COUNT(*) FROM telegram_posts")
                last_id = await conn.fetchval("SELECT MAX(id) FROM telegram_posts")
                
                # За последнюю неделю
                week_ago = datetime.now() - timedelta(days=7)
                week_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM telegram_posts WHERE post_time >= $1", 
                    week_ago
                )
                
                # За последние 24 часа
                day_ago = datetime.now() - timedelta(days=1)
                day_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM telegram_posts WHERE post_time >= $1", 
                    day_ago
                )
                
                # Статистика фильтров (общая)
                filter1_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM telegram_posts WHERE filter_initial = true"
                )
                filter2_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM telegram_posts WHERE filter_initial = true AND context = true"
                )
                filter3_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM telegram_posts WHERE filter_initial = true AND context = true AND essence = true"
                )
                
                # Статистика фильтров за 24 часа
                day_filter1_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM telegram_posts WHERE filter_initial = true AND post_time >= $1", 
                    day_ago
                )
                day_filter2_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM telegram_posts WHERE filter_initial = true AND context = true AND post_time >= $1", 
                    day_ago
                )
                day_filter3_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM telegram_posts WHERE filter_initial = true AND context = true AND essence = true AND post_time >= $1", 
                    day_ago
                )
                
                # Рассчитываем проценты (общие)
                filter1_percent = (filter1_count / total_count * 100) if total_count > 0 else 0
                filter2_percent = (filter2_count / total_count * 100) if total_count > 0 else 0
                filter3_percent = (filter3_count / total_count * 100) if total_count > 0 else 0
                
                # Рассчитываем проценты за 24 часа
                day_filter1_percent = (day_filter1_count / day_count * 100) if day_count > 0 else 0
                day_filter2_percent = (day_filter2_count / day_count * 100) if day_count > 0 else 0
                day_filter3_percent = (day_filter3_count / day_count * 100) if day_count > 0 else 0

            # Получаем размер БД
            db_size = await self._get_database_size()

            # Формируем сообщение со статистикой
            stats_message = f"""
📊 Статистика системы мониторинга

Обработано сообщений:
• Всего: {last_id or 0}
• За неделю: {week_count}
• За 24 часа: {day_count}
• Размер БД: {db_size}

Эффективность фильтров (все время):
✅ Фильтр 1 (допустимый контент): {filter1_percent:.1f}% ({filter1_count}/{total_count})
✅ Фильтр 2 (есть контекст): {filter2_percent:.1f}% ({filter2_count}/{total_count})
✅ Фильтр 3 (качественное содержание): {filter3_percent:.1f}% ({filter3_count}/{total_count})

За последние 24 часа:
🕐 Все фильтры: {day_filter3_percent:.1f}% ({day_filter3_count}/{day_count})

Дополнительные команды:
/distr 5.0 - статистика распределения по essence_score
            """.strip()

            await update.message.reply_text(stats_message)

        except Exception as e:
            logging.error(f"Ошибка при обработке /stats: {e}")
            await update.message.reply_text("❌ Ошибка при получении статистики.")

    async def run_bot(self):
        """Запускает бота с упрощенной логикой."""
        if not self.bot_app:
            logging.error("StatsService: Бот не настроен, запуск невозможен.")
            return

        try:
            logging.info("StatsService: Запуск бота статистики...")
            self.is_running = True
            
            # Простой запуск без сложного управления жизненным циклом
            await self.bot_app.initialize()
            await self.bot_app.start()
            await self.bot_app.updater.start_polling()
            
            # Держим бота активным
            while self.is_running:
                await asyncio.sleep(1)
                
        except asyncio.CancelledError:
            logging.info("StatsService: Получен сигнал остановки бота")
        except Exception as e:
            if "Conflict" in str(e):
                logging.warning("StatsService: Бот уже запущен в другом процессе. Пропускаем запуск.")
            else:
                logging.error(f"StatsService: Ошибка при работе бота: {e}")
        finally:
            # Корректно останавливаем бота
            try:
                if hasattr(self, 'bot_app') and self.bot_app:
                    if hasattr(self.bot_app, 'updater') and self.bot_app.updater:
                        await self.bot_app.updater.stop()
                    await self.bot_app.stop()
                    await self.bot_app.shutdown()
            except Exception as e:
                logging.error(f"StatsService: Ошибка при остановке бота: {e}")
            
            logging.info("StatsService: Бот остановлен")

    async def stop(self):
        """Останавливает службу."""
        self.is_running = False
        logging.info("StatsService: Остановка службы...")

    async def run(self):
        """Основной метод запуска службы."""
        try:
            await self._setup_database()
            if await self._setup_bot():
                await self.run_bot()
            else:
                logging.error("StatsService: Не удалось настроить бота, служба остановлена.")
        except Exception as e:
            logging.critical(f"StatsService: Критическая ошибка: {e}")
        finally:
            await self.stop()

async def main():
    """Точка входа для службы статистики."""
    stats_service = StatsService()
    await stats_service.run()

if __name__ == "__main__":
    asyncio.run(main())