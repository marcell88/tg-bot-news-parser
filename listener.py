import asyncio
from collections import deque
from datetime import datetime, timedelta
from telethon import TelegramClient, events
from telethon.tl.types import Message, Channel, User, PeerChannel, PeerUser
from telethon.sessions import StringSession
import logging
import json
import os 

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Конфигурация ---
class Config:
    """Класс для хранения конфигурации приложения."""
    API_ID = 25491744
    API_HASH = '0643451ea49fcac6f5a8697005714e33'

    TARGET_CHAT_ID = '@queue_news_bot'  # ID целевой группы, сообщения из тг-каналов пересылаются сюда

    PRIVATE_GROUP_ID = -1003081584455    # ID приватной группы для мониторинга (ДОБАВЛЕНО) - если появляется сообщение в приватное. то оно пересылается в trigger
    BOT_USERNAME = '@trigger_prosa_bot'  # Имя бота для пересылки (ДОБАВЛЕНО)

    MESSAGE_SEND_INTERVAL_SECONDS = 10 # Интервал отправки сообщений в секундах
    CHANNELS_FILE = 'monitored_channels.json' # Файл для сохранения списка каналов
    AUTHORIZED_USER_ID = None # ID пользователя, который может управлять ботом (будет установлен после авторизации)


# --- Класс TelegramForwarder ---
class TelegramForwarder:
    """
    Класс для пересылки сообщений из заданных каналов в целевую группу.
    Поддерживает динамическое управление списком каналов через команды.
    """
    def __init__(self, client: TelegramClient, target_chat_id: int):
            self.client = client
            self.target_chat_id = target_chat_id # Используем target_chat_id
            self.target_entity = None
            self.bot_entity = None  # ДОБАВЛЕНО: сущность бота для пересылки
            self.private_group_entity = None  # ДОБАВЛЕНО: сущность приватной группы

            self.message_queue = deque()
            self.last_send_time = datetime.min
            self.send_interval = timedelta(seconds=Config.MESSAGE_SEND_INTERVAL_SECONDS)

            self.monitored_channel_identifiers = self._load_monitored_channels() 

    async def _resolve_entities(self): 
        """Пытается получить сущности целевого чата, бота и приватной группы."""
        try:
            # Получаем сущность целевого чата
            self.target_entity = await self.client.get_entity(self.target_chat_id)
            if isinstance(self.target_entity, User):
                logging.info(f"Успешно подключились к целевому чату (пользователю/боту): {getattr(self.target_entity, 'first_name', self.target_chat_id)}")
            elif isinstance(self.target_entity, Channel):
                logging.info(f"Успешно подключились к целевой группе/каналу: {getattr(self.target_entity, 'title', self.target_chat_id)}")
            else:
                logging.info(f"Успешно подключились к целевой сущности: {self.target_chat_id}")
            
            # ДОБАВЛЕНО: Получаем сущность бота
            self.bot_entity = await self.client.get_entity(Config.BOT_USERNAME)
            logging.info(f"Успешно подключились к боту: {Config.BOT_USERNAME}")
            
            # ДОБАВЛЕНО: Получаем сущность приватной группы
            self.private_group_entity = await self.client.get_entity(Config.PRIVATE_GROUP_ID)
            logging.info(f"Успешно подключились к приватной группе: {getattr(self.private_group_entity, 'title', Config.PRIVATE_GROUP_ID)}")
            
        except Exception as e:
            logging.error(f"Ошибка доступа к одной из сущностей: {e}")
            raise

    async def _get_original_message_link(self, chat_entity, message_id: int) -> str | None:
        """
        Генерирует ссылку на сообщение, используя entity чата.
        """
        if not chat_entity:
            return None
        
        username = getattr(chat_entity, 'username', None)
        
        if username:
            return f"https://t.me/{username}/{message_id}"
        elif getattr(chat_entity, 'id', None):
            # Для каналов без username, используем ссылку по ID.
            # abs() для корректного URL приватных групп/каналов
            return f"https://t.me/c/{abs(chat_entity.id)}/{message_id}"
        return None

    async def _process_message_queue(self):
        """Обрабатывает очередь сообщений, отправляя их с задержкой."""
        while True:
            try:
                if self.message_queue and datetime.now() - self.last_send_time >= self.send_interval:
                    # message_data теперь словарь: {'text': ..., 'link': ..., 'title': ...}
                    message_data = self.message_queue.popleft()
                    
                    text_to_send = message_data['text']
                    link_to_send = message_data['link']
                    source_title = message_data['title']
                    
                    if text_to_send.strip():
                        msg = f"{text_to_send.strip()}"
                        if link_to_send:
                            msg += f"\n\n1111\n\n{link_to_send}" # Изменили текст ссылки
                        
                        await self.client.send_message(
                            entity=self.target_entity,
                            message=msg,
                            link_preview=False # Отключаем предпросмотр ссылок для чистоты
                        )
                        self.last_send_time = datetime.now()
                        logging.info(f"Отправлено сообщение из '{source_title}' в целевой чат.") 
            
            except Exception as e:
                logging.error(f"Ошибка при обработке очереди сообщений: {e}")
                await asyncio.sleep(5)
            
            await asyncio.sleep(0.5)

    def _load_monitored_channels(self) -> set[str]:
        """Загружает список каналов из файла."""
        if os.path.exists(Config.CHANNELS_FILE):
            try:
                with open(Config.CHANNELS_FILE, 'r', encoding='utf-8') as f:
                    channels = json.load(f)
                logging.info(f"Загружено {len(channels)} каналов из {Config.CHANNELS_FILE}")
                return set(channels)
            except json.JSONDecodeError as e:
                logging.error(f"Ошибка при чтении JSON из {Config.CHANNELS_FILE}: {e}")
                return set()
            except Exception as e:
                logging.error(f"Неизвестная ошибка при загрузке каналов: {e}")
                return set()
        logging.info("Файл с каналами не найден, начинается с пустого списка.")
        return set()

    def _save_monitored_channels(self):
        """Сохраняет текущий список каналов в файл."""
        try:
            with open(Config.CHANNELS_FILE, 'w', encoding='utf-8') as f:
                json.dump(list(self.monitored_channel_identifiers), f, ensure_ascii=False, indent=4)
            logging.info(f"Список каналов сохранен в {Config.CHANNELS_FILE}")
        except Exception as e:
            logging.error(f"Ошибка при сохранении списка каналов: {e}")

    async def add_channel_to_monitor_runtime(self, channel_identifier: str) -> bool:
        """Добавляет канал в список мониторинга во время выполнения и сохраняет."""
        normalized_identifier = str(channel_identifier).lower().replace('@', '')
        if normalized_identifier not in self.monitored_channel_identifiers:
            try:
                # Попробуем получить entity, чтобы убедиться, что канал существует и доступен
                entity = await self.client.get_entity(channel_identifier)
                self.monitored_channel_identifiers.add(normalized_identifier)
                self._save_monitored_channels()
                logging.info(f"Канал '{channel_identifier}' добавлен для мониторинга.")
                return True
            except Exception as e:
                logging.warning(f"Не удалось добавить канал '{channel_identifier}': {e}. Возможно, канал не существует или недоступен.")
                return False
        logging.info(f"Канал '{channel_identifier}' уже находится в списке мониторинга.")
        return False

    async def remove_channel_from_monitor_runtime(self, channel_identifier: str) -> bool:
        """Удаляет канал из списка мониторинга во время выполнения и сохраняет."""
        normalized_identifier = str(channel_identifier).lower().replace('@', '')
        if normalized_identifier in self.monitored_channel_identifiers:
            self.monitored_channel_identifiers.remove(normalized_identifier)
            self._save_monitored_channels()
            logging.info(f"Канал '{channel_identifier}' удален из мониторинга.")
            return True
        logging.info(f"Канал '{channel_identifier}' не найден в списке мониторинга.")
        return False

    def list_monitored_channels(self) -> list[str]:
        """Возвращает текущий список мониторящихся каналов."""
        return sorted(list(self.monitored_channel_identifiers))

    async def _private_group_message_handler(self, event: events.NewMessage.Event):
        """
        ДОБАВЛЕНО: Обработчик сообщений из приватной группы.
        Пересылает сообщения из приватной группы боту.
        """
        try:
            # Проверяем, что сообщение пришло из нужной приватной группы
            if event.chat_id != Config.PRIVATE_GROUP_ID:
                return
            
            message = event.message
            text_content = message.text or message.caption or ""
            
            if not text_content.strip():
                logging.debug("Получено сообщение без текста из приватной группы.")
                return
            
            logging.info(f"Обнаружено сообщение в приватной группе: {text_content[:100]}...")
            
            # Пересылаем сообщение боту
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
        Обработчик новых сообщений с усовершенствованной логикой пересылки.
        1. Если сообщение оригинальное и пришло из мониторящегося канала:
           Пересылаем текст со ссылкой на это сообщение в этом канале.
        2. Если сообщение переслано (forwarded) в мониторящийся канал:
           Пересылаем текст со ссылкой на оригинальное сообщение (если доступно).
        """
        try:
            chat = getattr(event, 'chat', None)
            message = event.message
            text_content = message.text or message.caption or ""

            if not chat or not text_content.strip():
                if not chat:
                    logging.debug("Получено событие без объекта чата (возможно, системное сообщение).")
                if not text_content.strip():
                    logging.debug(f"Получено сообщение без текста/подписи в чате {getattr(chat, 'title', chat.id)}.")
                return

            chat_username = getattr(chat, 'username', '').lower()
            chat_id_str = str(getattr(chat, 'id', ''))
            
            is_monitored_channel = (chat_username and chat_username in self.monitored_channel_identifiers) or \
                                   (chat_id_str and chat_id_str in self.monitored_channel_identifiers)
            
            if not is_monitored_channel:
                logging.debug(f"Сообщение из немониторящегося канала: {getattr(chat, 'title', chat_id_str)}")
                return

            source_channel_title = getattr(chat, 'title', f'Channel @{chat_username}' if chat_username else f'Channel ID:{chat_id_str}')

            message_to_queue = {
                'text': text_content,
                'link': None,
                'title': source_channel_title
            }

            # Сценарий 1: Сообщение является оригинальным (НЕ пересланным)
            if not message.fwd_from:
                logging.info(f"Обнаружено оригинальное сообщение из '{source_channel_title}'.")
                message_link = await self._get_original_message_link(chat, message.id)
                if message_link:
                    message_to_queue['link'] = message_link
                else:
                    logging.warning(f"Не удалось получить ссылку для оригинального сообщения в {source_channel_title}.")
                
                self.message_queue.append(message_to_queue)
                logging.info(f"Добавлено оригинальное сообщение в очередь из '{source_channel_title}'.")

            # Сценарий 2: Сообщение было переслано (forwarded) - ИЗМЕНЕННАЯ ЛОГИКА
            else:
                logging.info(f"Обнаружено пересланное сообщение в '{source_channel_title}'.")
                original_source_title = "Неизвестный источник"
                original_message_link = None
                fwd_from = message.fwd_from

                if fwd_from:
                    # Шаг 1: Получаем базовую информацию напрямую из fwd_from
                    if fwd_from.from_name:
                        original_source_title = fwd_from.from_name
                    
                    if fwd_from.from_id and isinstance(fwd_from.from_id, PeerChannel) and fwd_from.channel_post:
                        channel_id = fwd_from.from_id.channel_id
                        message_id = fwd_from.channel_post
                        # Шаг 2: Создаем "универсальную" ссылку, которая почти всегда доступна
                        original_message_link = f"https://t.me/c/{channel_id}/{message_id}"

                        # Шаг 3: Пытаемся "улучшить" ссылку, получив публичный username
                        try:
                            original_entity = await self.client.get_entity(fwd_from.from_id)
                            if getattr(original_entity, 'username', None):
                                original_message_link = f"https://t.me/{original_entity.username}/{message_id}"
                            if getattr(original_entity, 'title', None): # Обновляем название, если оно более точное
                                original_source_title = original_entity.title
                        except Exception:
                            # Если не удалось получить entity - не страшно, у нас уже есть базовая ссылка и название
                            logging.info(f"Не удалось получить entity для канала {channel_id}. Используется базовая ссылка.")

                # Добавляем информацию в очередь
                if original_message_link:
                    message_to_queue['link'] = original_message_link
                else:
                    # Этот блок теперь сработает только в редких случаях (например, пересылка от пользователя)
                    message_to_queue['link'] = f"Исходный канал/пользователь: {original_source_title}"
                    
                self.message_queue.append(message_to_queue)
                logging.info(f"Добавлено пересланное сообщение в очередь из '{source_channel_title}' (оригинал: '{original_source_title}').")
                
        except Exception as e:
            logging.error(f"Ошибка в обработчике нового сообщения: {e}", exc_info=True)





    async def _command_handler(self, event: events.NewMessage.Event):
        """Обработчик команд от авторизованного пользователя."""
        if event.sender_id != Config.AUTHORIZED_USER_ID:
            logging.warning(f"Неавторизованная команда от пользователя ID: {event.sender_id}")
            return

        text = event.message.text.lower()
        
        if text.startswith('/addchannel '):
            channel_identifier = text.replace('/addchannel ', '').strip()
            if channel_identifier:
                success = await self.add_channel_to_monitor_runtime(channel_identifier)
                if success:
                    await event.reply(f"Канал `{channel_identifier}` добавлен для мониторинга.")
                else:
                    await event.reply(f"Не удалось добавить канал `{channel_identifier}`. Проверьте правильность ID/username.")
            else:
                await event.reply("Используйте: `/addchannel <username_или_id>`")

        elif text.startswith('/removechannel '):
            channel_identifier = text.replace('/removechannel ', '').strip()
            if channel_identifier:
                success = await self.remove_channel_from_monitor_runtime(channel_identifier)
                if success:
                    await event.reply(f"Канал `{channel_identifier}` удален из мониторинга.")
                else:
                    await event.reply(f"Канал `{channel_identifier}` не найден в списке мониторинга.")
            else:
                await event.reply("Используйте: `/removechannel <username_или_id>`")

        elif text == '/listchannels':
            channels = self.list_monitored_channels()
            if channels:
                response = "Мониторятся следующие каналы:\n" + "\n".join([f"- `{ch}`" for ch in channels])
            else:
                response = "Список мониторящихся каналов пуст."
            await event.reply(response)
        
        elif text == '/start':
            await event.reply("Я запущен и мониторю каналы. Используйте:\n"
                              "`/addchannel <username_или_id>`\n"
                              "`/removechannel <username_или_id>`\n"
                              "`/listchannels`")


    async def start_monitoring(self):
        """Запускает процесс мониторинга и пересылки."""
        await self._resolve_entities()  # ИЗМЕНЕНО: Используем новый метод для получения всех сущностей
        
        # Добавляем обработчики событий
        self.client.add_event_handler(self._message_event_handler, events.NewMessage())
        # ДОБАВЛЕНО: Обработчик для приватной группы
        self.client.add_event_handler(self._private_group_message_handler, events.NewMessage(
            chats=[Config.PRIVATE_GROUP_ID]
        ))
        self.client.add_event_handler(self._command_handler, events.NewMessage(
            chats=[Config.AUTHORIZED_USER_ID], # Обрабатываем команды только от авторизованного пользователя
            pattern='^/(addchannel|removechannel|listchannels|start)'
        ))
        
        # Запускаем обработчик очереди как фоновую задачу
        asyncio.create_task(self._process_message_queue())
        
        logging.info("Мониторинг каналов запущен. Ожидание новых сообщений и команд...")

# --- Функции для работы с сессией ---
async def create_and_save_session(session_name: str) -> str:
    """
    Создает TelegramClient, авторизуется и возвращает строковую сессию.
    Предлагает ввести код подтверждения, если необходимо.
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
            
            # После авторизации получаем ID пользователя и сохраняем его в конфиг
            me = await client.get_me()
            Config.AUTHORIZED_USER_ID = me.id
            logging.info(f"Авторизованный пользователь ID: {Config.AUTHORIZED_USER_ID}")

        # Если уже авторизован, просто получаем его ID (на случай, если сессия была загружена)
        if Config.AUTHORIZED_USER_ID is None:
            me = await client.get_me()
            Config.AUTHORIZED_USER_ID = me.id
            logging.info(f"Авторизованный пользователь ID (из существующей сессии): {Config.AUTHORIZED_USER_ID}")

        session_string = client.session.save()
        logging.info("Строка сессии успешно получена.")
        
        # Сохраняем сессию в файл
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

# --- Главная функция запуска ---
async def main():
    session_string = None
    session_file_name = "telegram_forwarder_session"

    try:
        with open(f"{session_file_name}.session", "r") as f:
            session_string = f.read().strip()
        logging.info("Сессия успешно загружена из файла.")
    except FileNotFoundError:
        logging.warning(f"Файл сессии '{session_file_name}.session' не найден. Необходимо создать новую сессию.")
    except Exception as e:
        logging.error(f"Ошибка при чтении файла сессии: {e}. Попробуем создать новую сессию.")

    if not session_string:
        session_string = await create_and_save_session(session_file_name)

    if not session_string:
        logging.error("Не удалось получить строковую сессию. Выход.")
        return

    client = TelegramClient(StringSession(session_string), Config.API_ID, Config.API_HASH)
    
    try:
        logging.info("Подключение к Telegram с использованием сессии...")
        await client.start()
        
        if Config.AUTHORIZED_USER_ID is None:
            me = await client.get_me()
            Config.AUTHORIZED_USER_ID = me.id
            logging.info(f"Авторизованный пользователь ID (из новой сессии): {Config.AUTHORIZED_USER_ID}")

        if not await client.is_user_authorized():
            logging.error("Клиент не авторизован после загрузки сессии. Сессия может быть недействительной. Попробуйте удалить .session файл и запустить заново.")
            return
        logging.info("Клиент Telegram успешно подключен.")

        forwarder = TelegramForwarder(client, Config.TARGET_CHAT_ID) 
        
        await forwarder.start_monitoring()
        
        logging.info("Приложение запущено. Нажмите Ctrl+C для остановки.")
        await client.run_until_disconnected()
    except Exception as e:
        logging.critical(f"Критическая ошибка приложения: {e}")
    finally:
        if client.is_connected():
            logging.info("Отключение клиента Telegram...")
            await client.disconnect()
        logging.info("Приложение завершило работу.")

if __name__ == '__main__':
    asyncio.run(main())

    