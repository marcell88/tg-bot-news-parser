import asyncio
import logging
import os
import asyncpg
import json
from datetime import datetime, timedelta
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, ContextTypes
from database import Database

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Config:
    """
    Конфигурация для службы статистики.
    """
    # Настройки базы данных
    DB_HOST = os.getenv('DB_HOST', 'telegram-parsed-db-marcell88.db-msk0.amvera.tech')
    DB_PORT = int(os.getenv('DB_PORT', 5432))
    DB_NAME = os.getenv('DB_NAME', 'tg-parsed-db')
    DB_USER = os.getenv('DB_USER', 'marcell')
    DB_PASS = os.getenv('DB_PASS', '12345')
    
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
            
            logging.info("StatsService: Бот настроен успешно.")
            return True
        except Exception as e:
            logging.error(f"StatsService: Ошибка при настройке бота: {e}")
            return False

    async def _start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start."""
        welcome_text = """
🤖 *Бот статистики системы мониторинга*

Доступные команды:

/stats - Показать статистику обработки
/channels - Список мониторящихся каналов

Система автоматически собирает новости и анализирует их с помощью AI.
        """
        await update.message.reply_text(welcome_text, parse_mode='Markdown')

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
            message = "📋 *Мониторящиеся каналы:*\n\n"
            for i, channel in enumerate(channels_data[:50], 1):  # Ограничиваем 50 каналами
                username = f"@{channel['username']}" if channel['username'] else "без username"
                message += f"{i}. {channel['title']}\n   ID: `{channel['id']}` • {username}\n\n"

            if len(channels_data) > 50:
                message += f"\n... и еще {len(channels_data) - 50} каналов"

            await update.message.reply_text(message, parse_mode='Markdown')

        except Exception as e:
            logging.error(f"Ошибка при обработке /channels: {e}")
            await update.message.reply_text("❌ Ошибка при получении списка каналов.")

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
                
                # Статистика фильтров
                filter1_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM telegram_posts WHERE filter_initial = true"
                )
                filter2_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM telegram_posts WHERE filter_initial = true AND context = true"
                )
                filter3_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM telegram_posts WHERE filter_initial = true AND context = true AND essence = true"
                )
                
                # Рассчитываем проценты
                filter1_percent = (filter1_count / total_count * 100) if total_count > 0 else 0
                filter2_percent = (filter2_count / total_count * 100) if total_count > 0 else 0
                filter3_percent = (filter3_count / total_count * 100) if total_count > 0 else 0

            # Формируем сообщение со статистикой
            stats_message = f"""
📊 *Статистика системы мониторинга*

*Обработано сообщений:*
• Всего: `{total_count}`
• За неделю: `{week_count}`
• За 24 часа: `{day_count}`
• Последний ID: `{last_id or 0}`

*Эффективность фильтров:*
✅ Фильтр 1 (допустимый контент): `{filter1_percent:.1f}%` ({filter1_count}/{total_count})
✅ Фильтр 2 (есть контекст): `{filter2_percent:.1f}%` ({filter2_count}/{total_count})
✅ Фильтр 3 (качественное содержание): `{filter3_percent:.1f}%` ({filter3_count}/{total_count})

*Прохождение контента:*
🟢 Прошли все фильтры: `{filter3_count} сообщений`
            """.strip()

            await update.message.reply_text(stats_message, parse_mode='Markdown')

        except Exception as e:
            logging.error(f"Ошибка при обработке /stats: {e}")
            await update.message.reply_text("❌ Ошибка при получении статистики.")

    async def run_bot(self):
        """Запускает бота."""
        if not self.bot_app:
            logging.error("StatsService: Бот не настроен, запуск невозможен.")
            return

        try:
            logging.info("StatsService: Запуск бота статистики...")
            await self.bot_app.run_polling()
        except Exception as e:
            logging.error(f"StatsService: Ошибка при работе бота: {e}")

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

async def main():
    """Точка входа для службы статистики."""
    stats_service = StatsService()
    await stats_service.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("StatsService: Остановка по запросу пользователя.")