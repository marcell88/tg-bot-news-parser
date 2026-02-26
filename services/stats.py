# services/stats.py
"""
Служба статистики для Telegram бота.
Предоставляет команды для получения различных метрик из базы данных.
"""

import asyncio
import logging
import os
import json
import ast
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional
from collections import Counter

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes

from database.database import Database
from database.database_config import DatabaseConfig

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Config:
    """Конфигурация для службы статистики."""
    DB_HOST = DatabaseConfig.DB_HOST
    DB_PORT = DatabaseConfig.DB_PORT
    DB_NAME = DatabaseConfig.DB_NAME
    DB_USER = DatabaseConfig.DB_USER
    DB_PASS = DatabaseConfig.DB_PASS
    STATS_BOT_API_KEY = os.getenv('STATS_BOT_API_KEY', '')
    CHANNELS_FILE = 'monitored_channels.json'


class StatsService:
    """Служба для сбора статистики и управления ботом."""

    def __init__(self):
        self.db_pool = None
        self.bot_app = None
        self.is_running = False
        logging.info("StatsService: Служба статистики инициализирована.")

    async def _setup_database(self) -> bool:
        """Подключается к базе данных."""
        logging.info("StatsService: Подключение к БД...")
        try:
            self.db_pool = await Database.get_pool()
            logging.info("StatsService: Пул подключений к БД получен успешно.")
            return True
        except Exception as e:
            logging.critical(f"StatsService: Ошибка при подключении к БД: {e}")
            return False

    async def _setup_bot(self) -> bool:
        """Настраивает Telegram бота."""
        if not Config.STATS_BOT_API_KEY:
            logging.error("StatsService: STATS_BOT_API_KEY не установлен")
            return False

        try:
            self.bot_app = Application.builder().token(Config.STATS_BOT_API_KEY).build()

            # Регистрация обработчиков команд
            self.bot_app.add_handler(CommandHandler("start", self._start_command))
            self.bot_app.add_handler(CommandHandler("help", self._help_command))
            self.bot_app.add_handler(CommandHandler("input", self._input_command))
            self.bot_app.add_handler(CommandHandler("editor", self._editor_command))
            self.bot_app.add_handler(CommandHandler("longterm", self._longterm_command))
            self.bot_app.add_handler(CommandHandler("authors", self._authors_command))
            self.bot_app.add_handler(CommandHandler("channels", self._channels_command))

            logging.info("StatsService: Бот настроен успешно.")
            return True
        except Exception as e:
            logging.error(f"StatsService: Ошибка при настройке бота: {e}")
            return False

    def _get_commands_keyboard(self) -> InlineKeyboardMarkup:
        """Создает клавиатуру со списком команд."""
        keyboard = [
            [
                InlineKeyboardButton("/input", callback_data="cmd_input"),
                InlineKeyboardButton("/editor", callback_data="cmd_editor"),
            ],
            [
                InlineKeyboardButton("/longterm", callback_data="cmd_longterm"),
                InlineKeyboardButton("/authors", callback_data="cmd_authors"),
            ],
            [
                InlineKeyboardButton("/channels", callback_data="cmd_channels"),
                InlineKeyboardButton("/help", callback_data="cmd_help"),
            ]
        ]
        return InlineKeyboardMarkup(keyboard)

    async def _start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start."""
        welcome_text = (
            "Бот статистики системы мониторинга\n"
            "\n"
            "Доступные команды:\n"
            "/input - статистика поступления сообщений (пример: /input)\n"
            "/editor - количество записей в редакторе\n"
            "/longterm - долгосрочная статистика тем и настроений\n"
            "/authors - статистика авторов\n"
            "/channels - список мониторящихся каналов\n"
            "/help - справка\n"
        )
        await update.message.reply_text(
            welcome_text,
            reply_markup=self._get_commands_keyboard()
        )

    async def _help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /help."""
        help_text = (
            "/input - статистика поступления сообщений за 24 часа\n"
            "  Пример: /input - покажет количество записей в telegram_posts, telegram_posts_top, telegram_posts_top_top\n"
            "\n"
            "/editor - количество записей в таблице editor\n"
            "  Пример: /editor\n"
            "\n"
            "/longterm - долгосрочная статистика тем и настроений из таблицы state\n"
            "  Пример: /longterm\n"
            "\n"
            "/authors - статистика авторов из последних 50 записей таблицы to_publish\n"
            "  Пример: /authors\n"
            "\n"
            "/channels - список мониторящихся каналов\n"
            "  Пример: /channels\n"
        )
        await update.message.reply_text(help_text, reply_markup=self._get_commands_keyboard())

    async def _input_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /input - статистика поступления сообщений."""
        if not self.db_pool:
            await update.message.reply_text("Ошибка: База данных не подключена")
            return

        try:
            # Текущее время UTC
            now_utc = datetime.utcnow()
            day_ago_utc = now_utc - timedelta(days=1)

            async with self.db_pool.acquire() as conn:
                # telegram_posts за последние 24 часа
                posts_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM telegram_posts WHERE post_time >= $1",
                    day_ago_utc
                ) or 0

                # telegram_posts_top за последние 24 часа
                posts_top_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM telegram_posts_top WHERE post_time >= $1",
                    day_ago_utc
                ) or 0

                # telegram_posts_top_top за последние 24 часа
                posts_top_top_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM telegram_posts_top_top WHERE post_time >= $1",
                    day_ago_utc
                ) or 0

            response = (
                f"/input\n"
                f"{posts_count} > {posts_top_count} > {posts_top_count}\n"
                f"\n"
                f"telegram_posts: {posts_count}\n"
                f"telegram_posts_top: {posts_top_count}\n"
                f"telegram_posts_top_top: {posts_top_top_count}"
            )
            await update.message.reply_text(response, reply_markup=self._get_commands_keyboard())

        except Exception as e:
            logging.error(f"Ошибка при обработке /input: {e}")
            await update.message.reply_text(f"Ошибка при получении статистики: {e}")

    async def _editor_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /editor - количество записей в таблице editor."""
        if not self.db_pool:
            await update.message.reply_text("Ошибка: База данных не подключена")
            return

        try:
            async with self.db_pool.acquire() as conn:
                count = await conn.fetchval("SELECT COUNT(*) FROM editor") or 0

            response = f"/editor\n{count}\n\nЗаписей в редакторе: {count}"
            await update.message.reply_text(response, reply_markup=self._get_commands_keyboard())

        except Exception as e:
            logging.error(f"Ошибка при обработке /editor: {e}")
            await update.message.reply_text(f"Ошибка при получении статистики: {e}")

    async def _longterm_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /longterm - статистика тем и настроений."""
        if not self.db_pool:
            await update.message.reply_text("Ошибка: База данных не подключена")
            return

        try:
            async with self.db_pool.acquire() as conn:
                # Получаем данные из таблицы state
                row = await conn.fetchrow("SELECT lt_topic, lt_mood FROM state LIMIT 1")

            if not row or not row['lt_topic'] or not row['lt_mood']:
                await update.message.reply_text("Нет данных в таблице state")
                return

            # Парсим темы
            topics_text = self._parse_longterm_data(row['lt_topic'], "ТЕМЫ")
            moods_text = self._parse_longterm_data(row['lt_mood'], "НАСТРОЕНИЕ")

            response = f"/longterm\n\n{topics_text}\n\n{moods_text}"
            await update.message.reply_text(response, reply_markup=self._get_commands_keyboard())

        except Exception as e:
            logging.error(f"Ошибка при обработке /longterm: {e}")
            await update.message.reply_text(f"Ошибка при получении статистики: {e}")

    def _parse_longterm_data(self, data: str, title: str) -> str:
        """Парсит данные долгосрочной статистики."""
        try:
            # Очищаем строку от лишних кавычек и пробелов
            cleaned = data.strip('"{}').replace('""', '"')
            
            # Разбиваем на отдельные объекты
            items = []
            buffer = ""
            bracket_count = 0
            
            for char in cleaned:
                buffer += char
                if char == '{':
                    bracket_count += 1
                elif char == '}':
                    bracket_count -= 1
                    if bracket_count == 0:
                        # Завершенный объект
                        try:
                            # Пробуем распарсить JSON
                            obj_str = buffer.strip().replace('\\"', '"')
                            if obj_str.startswith('"') and obj_str.endswith('"'):
                                obj_str = obj_str[1:-1]
                            
                            obj = json.loads(obj_str)
                            if isinstance(obj, dict) and 'topic' in obj:
                                items.append((obj['topic'], obj['weight'] * 100))
                            elif isinstance(obj, dict) and 'mood' in obj:
                                items.append((obj['mood'], obj['weight'] * 100))
                        except:
                            # Если не JSON, пробуем извлечь регулярным выражением
                            import re
                            name_match = re.search(r'"(?:topic|mood)":\s*"([^"]+)"', obj_str)
                            weight_match = re.search(r'"weight":\s*([0-9.]+)', obj_str)
                            
                            if name_match and weight_match:
                                name = name_match.group(1)
                                weight = float(weight_match.group(1)) * 100
                                items.append((name, weight))
                        
                        buffer = ""

            # Формируем результат
            if items:
                result = [f"{title}"]
                for name, weight in items:
                    # Ограничиваем длину строки
                    short_name = name if len(name) <= 50 else name[:47] + "..."
                    result.append(f"{short_name} = {weight:.0f}%")
                return "\n".join(result)
            else:
                return f"{title}\nНет данных"

        except Exception as e:
            logging.error(f"Ошибка парсинга {title}: {e}")
            return f"{title}\nОшибка парсинга данных"

    async def _authors_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /authors - статистика авторов."""
        if not self.db_pool:
            await update.message.reply_text("Ошибка: База данных не подключена")
            return

        try:
            async with self.db_pool.acquire() as conn:
                # Получаем последние 50 записей с авторами
                rows = await conn.fetch(
                    "SELECT author FROM to_publish WHERE author IS NOT NULL AND author != '' ORDER BY id DESC LIMIT 50"
                )

            if not rows:
                await update.message.reply_text("Нет данных об авторах")
                return

            # Считаем частоту авторов
            authors = [row['author'] for row in rows if row['author']]
            author_counts = Counter(authors)
            total = len(authors)

            if total == 0:
                await update.message.reply_text("Нет данных об авторах")
                return

            # Формируем статистику (топ-10)
            sorted_authors = sorted(author_counts.items(), key=lambda x: x[1], reverse=True)[:10]
            
            response_lines = ["/authors"]
            for author, count in sorted_authors:
                percentage = (count / total) * 100
                response_lines.append(f"{author.upper()} = {percentage:.0f}%")

            response = "\n".join(response_lines)
            await update.message.reply_text(response, reply_markup=self._get_commands_keyboard())

        except Exception as e:
            logging.error(f"Ошибка при обработке /authors: {e}")
            await update.message.reply_text(f"Ошибка при получении статистики: {e}")

    async def _channels_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /channels - список мониторящихся каналов."""
        try:
            if not os.path.exists(Config.CHANNELS_FILE):
                await update.message.reply_text("Файл с каналами не найден")
                return

            with open(Config.CHANNELS_FILE, 'r', encoding='utf-8') as f:
                channels_data = json.load(f)

            if not channels_data:
                await update.message.reply_text("Нет каналов для мониторинга")
                return

            # Формируем сообщение с каналами
            message_lines = ["/channels\n\nМониторящиеся каналы:\n"]
            for i, channel in enumerate(channels_data[:20], 1):
                username = f"@{channel['username']}" if channel.get('username') else "без username"
                title = channel.get('title', 'Без названия')
                message_lines.append(f"{i}. {title}")
                message_lines.append(f"   ID: {channel['id']} • {username}\n")

            if len(channels_data) > 20:
                message_lines.append(f"... и еще {len(channels_data) - 20} каналов")

            await update.message.reply_text(
                "\n".join(message_lines),
                reply_markup=self._get_commands_keyboard()
            )

        except Exception as e:
            logging.error(f"Ошибка при обработке /channels: {e}")
            await update.message.reply_text(f"Ошибка при получении списка каналов: {e}")

    async def run_bot(self):
        """Запускает бота."""
        if not self.bot_app:
            logging.error("StatsService: Бот не настроен, запуск невозможен.")
            return

        try:
            logging.info("StatsService: Запуск бота статистики...")
            self.is_running = True

            await self.bot_app.initialize()
            await self.bot_app.start()
            await self.bot_app.updater.start_polling()

            logging.info("StatsService: Бот статистики запущен и готов к работе")

            while self.is_running:
                await asyncio.sleep(1)

        except asyncio.CancelledError:
            logging.info("StatsService: Получен сигнал остановки бота")
        except Exception as e:
            if "Conflict" in str(e):
                logging.warning("StatsService: Бот уже запущен в другом процессе")
            else:
                logging.error(f"StatsService: Ошибка при работе бота: {e}")
        finally:
            await self._stop_bot()
            logging.info("StatsService: Бот остановлен")

    async def _stop_bot(self):
        """Корректно останавливает бота."""
        try:
            if self.bot_app:
                if hasattr(self.bot_app, 'updater') and self.bot_app.updater:
                    await self.bot_app.updater.stop()
                await self.bot_app.stop()
                await self.bot_app.shutdown()
        except Exception as e:
            logging.error(f"StatsService: Ошибка при остановке бота: {e}")

    async def stop(self):
        """Останавливает службу."""
        self.is_running = False
        logging.info("StatsService: Остановка службы...")

    async def run(self):
        """Основной метод запуска службы."""
        try:
            if not await self._setup_database():
                logging.error("StatsService: Не удалось подключиться к БД")
                return

            if await self._setup_bot():
                await self.run_bot()
            else:
                logging.error("StatsService: Не удалось настроить бота")
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