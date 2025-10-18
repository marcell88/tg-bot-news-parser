import asyncio
# deque (double-ended queue) используется для эффективной работы с очередью (быстрое добавление и удаление с концов).
from collections import deque 
from datetime import datetime, timedelta
# TelegramClient - основной класс для взаимодействия с API Telegram.
# events - для обработки событий (например, новых сообщений).
from telethon import TelegramClient, events
# Типы данных Telegram, используемые для разбора сущностей и сообщений.
from telethon.tl.types import Message, Channel, User, PeerChannel, PeerUser
# StringSession позволяет сохранять и восстанавливать сессию в виде строки.
from telethon.sessions import StringSession
import logging
import json
import os 
# asyncpg - асинхронный драйвер PostgreSQL.
import asyncpg 
from database.database import Database
from database.database_config import DatabaseConfig

# --- Настройка логирования ---
# Настраиваем логирование, чтобы видеть сообщения (INFO, ERROR и т.д.) с датой и временем.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Конфигурация ---
class Config:
    """
    Класс для хранения конфигурации приложения. 
    Все ключевые параметры собраны здесь для удобства настройки.
    """
    # API_ID и API_HASH - учетные данные для доступа к Telegram API (получены на my.telegram.org).
    API_ID = 25491744
    API_HASH = '0643451ea49fcac6f5a8697005714e33'

    # PRIVATE_GROUP_ID - ID приватной группы, сообщения из которой будут пересылаться боту-анализатору.
    PRIVATE_GROUP_ID = -1003130125238
    # BOT_USERNAME - юзернейм бота, который должен обрабатывать сообщения из приватной группы.
    BOT_USERNAME = '@trigger_prosa_bot'

    # CHANNELS_FILE - имя файла, в котором хранится список мониторящихся каналов.
    CHANNELS_FILE = 'monitored_channels.json'

    # CHANNELS_UPDATE_INTERVAL_MINUTES - интервал обновления списка каналов (в минутах)
    CHANNELS_UPDATE_INTERVAL_MINUTES = 30  # <-- ДОБАВИТЬ

    DB_HOST = DatabaseConfig.DB_HOST
    DB_PORT = DatabaseConfig.DB_PORT
    DB_NAME = DatabaseConfig.DB_NAME
    DB_USER = DatabaseConfig.DB_USER
    DB_PASS = DatabaseConfig.DB_PASS
    
    # -----------------------------------------------------------------


# --- Класс TelegramListener ---
class TelegramListener:
    """
    Класс-контроллер, который отвечает за:
    1. Подключение к Telegram и БД.
    2. Отслеживание новых сообщений в мониторящихся каналах.
    3. Сохранение сообщений в PostgreSQL.
    """
    def __init__(self, client: TelegramClient):
            self.client = client
            self.bot_entity = None 
            self.private_group_entity = None
            self.monitored_channel_identifiers = self._load_monitored_channels() 
            self.db_pool = None 
            self.last_channels_update = None


    async def _update_monitored_channels(self):
        """
        Обновляет список мониторящихся каналов, получая текущие подписки пользователя.
        """
        try:
            logging.info("🔄 Обновление списка мониторящихся каналов...")
            
            channels_list = []
            
            # Получаем все диалоги
            async for dialog in self.client.iter_dialogs():
                # Берем только каналы (is_channel = True)
                if dialog.is_channel:
                    entity = dialog.entity
                    
                    channel_data = {
                        'id': entity.id,
                        'title': getattr(entity, 'title', 'Без названия'),
                        'username': getattr(entity, 'username', None),
                        'participants_count': getattr(entity, 'participants_count', 0),
                        'broadcast': getattr(entity, 'broadcast', False),
                        'megagroup': getattr(entity, 'megagroup', False),
                    }
                    
                    channels_list.append(channel_data)
            
            # Сохраняем в monitored_channels.json
            with open(Config.CHANNELS_FILE, 'w', encoding='utf-8') as f:
                json.dump(channels_list, f, ensure_ascii=False, indent=2)
            
            # ОБНОВЛЯЕМ ПРАВИЛЬНО - без рекурсии
            channel_ids = set()
            for channel in channels_list:
                channel_ids.add(str(channel['id']))
            
            old_count = len(self.monitored_channel_identifiers)
            self.monitored_channel_identifiers = channel_ids
            self.last_channels_update = datetime.now()
            
            # Логируем изменения
            logging.info(f"✅ Список каналов обновлен! Было: {old_count}, стало: {len(channel_ids)} каналов")
            
        except Exception as e:
            logging.error(f"❌ Ошибка при обновлении списка каналов: {e}")



    async def _channels_update_loop(self):
        """
        Цикл для регулярного обновления списка каналов.
        """
        while True:
            try:
                # Первое обновление при старте
                if self.last_channels_update is None:
                    await self._update_monitored_channels()
                
                # Ждем указанный интервал
                await asyncio.sleep(Config.CHANNELS_UPDATE_INTERVAL_MINUTES * 60)
                
                # Обновляем список каналов
                await self._update_monitored_channels()
                
            except Exception as e:
                logging.error(f"Ошибка в цикле обновления каналов: {e}")
                await asyncio.sleep(60)  # Ждем минуту при ошибке

    # --- МЕТОДЫ ДЛЯ РАБОТЫ С БАЗОЙ ДАННЫХ ---
    async def _setup_database(self):
        """
        Получает общий пул подключений из Database менеджера.
        """
        logging.info("Listener: Получение общего пула подключений...")
        try:
            # Используем общий пул вместо создания нового
            self.db_pool = await Database.get_pool()  # <-- ИЗМЕНИТЬ ЭТУ СТРОКУ
            logging.info("Listener: Пул подключений получен успешно.")
            
            # Инициализация таблицы (оставляем эту логику)
            async with self.db_pool.acquire() as conn:
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
                logging.info("Таблица 'telegram_posts' создана/готова.")
            
        except Exception as e:
            logging.critical(f"Listener: Ошибка при настройке базы данных: {e}")
            raise

    async def _save_message_to_db(self, data: dict):
        """Сохраняет сообщение в БД."""
        if not self.db_pool:
            logging.error("Не удалось сохранить сообщение: пул подключений к БД отсутствует.")
            return

        try:
            async with self.db_pool.acquire() as conn:
                # В INSERT-запросе не нужно указывать поле 'finished', так как оно автоматически 
                # получает значение DEFAULT FALSE, заданное в схеме таблицы.
                await conn.execute("""
                    INSERT INTO telegram_posts (post_time, text_content, message_link)
                    VALUES ($1, $2, $3)
                """, 
                data['post_time'], data['text'], data['link']) # <-- Используем ключ 'text'
            
            logging.info(f"Сообщение сохранено в БД из '{data['title']}' (Post Time: {data['post_time']}).")

        except Exception as e:
            logging.error(f"Ошибка при сохранении сообщения в базу данных: {e}")

    async def _resolve_entities(self): 
        """
        Получает объекты (Entity) для бота и приватной группы 
        по их ID/юзернеймам.
        """
        try:
            self.bot_entity = await self.client.get_entity(Config.BOT_USERNAME)
            logging.info(f"Успешно подключились к боту: {Config.BOT_USERNAME}")
            
            self.private_group_entity = await self.client.get_entity(Config.PRIVATE_GROUP_ID)
            logging.info(f"Успешно подключились к приватной группе: {getattr(self.private_group_entity, 'title', Config.PRIVATE_GROUP_ID)}")
            
        except Exception as e:
            logging.error(f"Ошибка доступа к одной из сущностей: {e}")
            raise

    async def _get_original_message_link(self, chat_entity, message_id: int) -> str | None:
        """
        Генерирует постоянную ссылку на сообщение.
        Для публичных каналов использует юзернейм (t.me/username/id), 
        для приватных - c/ID (t.me/c/ID/id).
        """
        if not chat_entity: return None
        username = getattr(chat_entity, 'username', None)
        if username:
            return f"https://t.me/{username}/{message_id}"
        elif getattr(chat_entity, 'id', None):
            return f"https://t.me/c/{abs(chat_entity.id)}/{message_id}"
        return None

    def _load_monitored_channels(self) -> set[str]:
        """
        Загружает список каналов для мониторинга из файла 'monitored_channels.json'.
        """
        if os.path.exists(Config.CHANNELS_FILE):
            try:
                with open(Config.CHANNELS_FILE, 'r', encoding='utf-8') as f:
                    channels_data = json.load(f)
                
                # Извлекаем ID каналов
                channel_ids = set()
                channel_info_list = []
                
                for channel in channels_data:
                    channel_id = str(channel['id'])
                    channel_ids.add(channel_id)
                    channel_info = {
                        'id': channel_id,
                        'title': channel.get('title', 'Без названия'),
                        'username': channel.get('username', 'нет username')
                    }
                    channel_info_list.append(channel_info)
                
                # Логируем информацию
                logging.info(f"📊 Загружено {len(channel_ids)} каналов для мониторинга")
                logging.info("📋 Список мониторящихся каналов:")
                for i, channel in enumerate(channel_info_list, 1):
                    username_display = f"@{channel['username']}" if channel['username'] != 'нет username' else "без username"
                    logging.info(f"    {i:2d}. {channel['title']:40} (ID: {channel['id']:15}) {username_display}")
                
                return channel_ids
                
            except Exception as e:
                logging.error(f"Ошибка при загрузке каналов: {e}") 
                return set()
        
        logging.info(f"Файл {Config.CHANNELS_FILE} не найден. Будет создан при первом обновлении.")
        return set()

    async def _private_group_message_handler(self, event: events.NewMessage.Event):
        """
        Обработчик сообщений, пришедших из приватной группы (Config.PRIVATE_GROUP_ID).
        Эти сообщения пересылаются напрямую боту-анализатору.
        """
        try:
            if event.chat_id != Config.PRIVATE_GROUP_ID: return
            message = event.message
            text_content = message.text or message.caption or ""
            if not text_content.strip(): return
            logging.info(f"Обнаружено сообщение в приватной группе: {text_content[:100]}...")
            await self.client.send_message(
                entity=self.bot_entity,
                message=text_content.strip(),
                link_preview=False
            )
            logging.info(f"Сообщение из приватной группы переслано боту {Config.BOT_USERNAME}")
        except Exception as e:
            logging.error(f"Ошибка при обработке сообщения из приватной группы: {e}", exc_info=True)

    async def _message_event_handler(self, event: events.NewMessage.Event):
        """
        Основной обработчик новых сообщений из всех чатов. 
        Фильтрует сообщения, генерирует ссылку и сохраняет в БД, если канал мониторится.
        """
        try:
            chat = getattr(event, 'chat', None)
            message = event.message
            text_content = message.text or message.caption or ""

            if not chat or not text_content.strip(): return

            chat_identifiers = set()
            chat_username = getattr(chat, 'username', '').lower()
            chat_id = getattr(chat, 'id', None)

            # Собираем все возможные идентификаторы чата (юзернейм, ID со знаком и без)
            if chat_username: chat_identifiers.add(chat_username)
            if chat_id:
                chat_identifiers.add(str(chat_id))
                chat_identifiers.add(str(abs(chat_id))) 

            # Проверка: пересекаются ли идентификаторы сообщения с отслеживаемыми каналами.
            is_monitored_channel = not self.monitored_channel_identifiers.isdisjoint(chat_identifiers)
            
            if not is_monitored_channel: return

            source_channel_title = getattr(chat, 'title', f'Channel @{chat_username}' if chat_username else f'Channel ID:{chat_id}')

            # Создание объекта для сохранения в БД
            message_data = {
                'post_time': message.date.replace(tzinfo=None) if message.date else datetime.now(), 
                'text': text_content,
                'link': None,
                'title': source_channel_title
            }

            message_link = None
            original_source_title = source_channel_title

            # Сценарий 1: Сообщение является оригинальным 
            if not message.fwd_from:
                message_link = await self._get_original_message_link(chat, message.id)

            # Сценарий 2: Сообщение было переслано (forwarded) 
            else:
                fwd_from = message.fwd_from
                if fwd_from and fwd_from.from_id and isinstance(fwd_from.from_id, PeerChannel) and fwd_from.channel_post:
                    channel_id = fwd_from.from_id.channel_id
                    message_id = fwd_from.channel_post
                    message_link = f"https://t.me/c/{channel_id}/{message_id}"
                    
                    try:
                        original_entity = await self.client.get_entity(fwd_from.from_id)
                        if getattr(original_entity, 'username', None):
                            message_link = f"https://t.me/{original_entity.username}/{message_id}"
                        if getattr(original_entity, 'title', None): 
                            original_source_title = original_entity.title
                    except Exception:
                        pass

            if message_link:
                message_data['link'] = message_link
            else:
                message_data['link'] = f"Исходный канал/пользователь: {original_source_title}"
                    
            # Сохраняем сообщение напрямую в БД
            await self._save_message_to_db(message_data)
                
        except Exception as e:
            logging.error(f"Ошибка в обработчике нового сообщения: {e}", exc_info=True)

    async def start_monitoring(self):
        """
        Главный метод запуска: 
        1. Разрешает все сущности (чаты/боты).
        2. Настраивает БД.
        3. Добавляет обработчики событий Telegram.
        4. Запускает фоновые задачи.
        """
        await self._resolve_entities() 
        await self._setup_database() 
        
        self.client.add_event_handler(self._message_event_handler, events.NewMessage())
        self.client.add_event_handler(self._private_group_message_handler, events.NewMessage(
            chats=[Config.PRIVATE_GROUP_ID]
        ))
        
        # Запускаем фоновую задачу обновления каналов
        asyncio.create_task(self._channels_update_loop())
        
        logging.info("Мониторинг каналов и подключение к БД запущены...")
        logging.info(f"📡 Автообновление списка каналов каждые {Config.CHANNELS_UPDATE_INTERVAL_MINUTES} минут")


# --- Функции для работы с сессией ---
async def create_and_save_session(session_name: str) -> str:
    """
    Создает новую сессию TelegramClient, запрашивая у пользователя авторизацию 
    (телефон, код, пароль 2FA), и сохраняет строку сессии в файл.
    """
    logging.info(f"Создание новой сессии с именем '{session_name}'.")
    
    client = TelegramClient(StringSession(), Config.API_ID, Config.API_HASH)
    
    try:
        await client.connect()
        if not await client.is_user_authorized():
            logging.info("Клиент не авторизован. Запрашиваем авторизацию...")
            user_phone = input('Пожалуйста, введите свой номер телефона (включая код страны): ')
            await client.start(phone=user_phone,
                               password=lambda: input('Пожалуйста, введите свой пароль (если установлен 2FA): '),
                               code_callback=lambda: input('Пожалуйста, введите код, присланный Telegram: '))
            logging.info("Авторизация прошла успешно.")

        session_string = client.session.save()
        logging.info("Строка сессии успешно получена.")
        
        with open(f"{session_name}.session", "w") as f:
            f.write(session_string)
        logging.info(f"Строка сессии сохранена в файл '{session_name}.session'.")
        
        return session_string
    except Exception as e:
        logging.error(f"Ошибка при создании или сохранении сессии: {e}")
        raise
    finally:
        if client.is_connected():
            await client.disconnect()

# --- Главная функция запуска, которая будет вызываться из app.py ---
async def main():
    """
    Основная точка входа в программу. 
    Отвечает за загрузку/создание сессии и запуск клиента Telegram.
    """
    session_string = None
    session_file_name = "telegram_forwarder_session"

    # 1. Попытка загрузки сессии
    try:
        with open(f"{session_file_name}.session", "r") as f:
            session_string = f.read().strip()
        logging.info("Сессия успешно загружена из файла.")
    except FileNotFoundError:
        logging.warning(f"Файл сессии '{session_file_name}.session' не найден. Необходимо создать новую сессию.")
    except Exception as e:
        logging.error(f"Ошибка при чтении файла сессии: {e}. Попробуем создать новую сессию.")

    # 2. Создание сессии, если загрузка не удалась
    if not session_string:
        session_string = await create_and_save_session(session_file_name)

    if not session_string:
        logging.error("Не удалось получить строковую сессию. Выход.")
        return

    # 3. Инициализация клиента Telegram с загруженной сессией
    client = TelegramClient(StringSession(session_string), Config.API_ID, Config.API_HASH)
    
    try:
        logging.info("Подключение к Telegram с использованием сессии...")
        await client.start()
        
        if not await client.is_user_authorized():
            logging.error("Клиент не авторизован после загрузки сессии.")
            return
        logging.info("Клиент Telegram успешно подключен.")

        # 4. Инициализация и запуск основного класса
        listener = TelegramListener(client) 
        await listener.start_monitoring()
        
        logging.info("Приложение запущено. Нажмите Ctrl+C для остановки.")
        await client.run_until_disconnected()
    except Exception as e:
        logging.critical(f"Критическая ошибка приложения: {e}")
    finally:
        # 5. Очистка ресурсов
        if client.is_connected():
            logging.info("Отключение клиента Telegram...")
            await client.disconnect()
        logging.info("Приложение завершило работу.")

if __name__ == '__main__':
    # Если файл запущен напрямую (для тестирования), запускаем main().
    asyncio.run(main())