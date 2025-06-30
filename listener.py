
import asyncio
from collections import deque
from datetime import datetime, timedelta
from telethon import TelegramClient, events
from telethon.tl.types import Message

# Конфигурация
API_ID = 25491744
API_HASH = '0643451ea49fcac6f5a8697005714e33'
BOT_TOKEN = '7829367379:AAFCGozo_e_-FT0hT753fF_LmHISO-HD_CA'  # Бот для управления каналами
TARGET_GROUP_ID = -1002832515196  # ID вашей приватной группы

CHANNELS_TO_MONITOR = [
    "bbbreaking",
    "bazabazon",
    "vedomosti",
    "rbc_news",
    "rian_ru",
    "mash",
    "mashnasporte",
    "mashmoyka",
    "mash_gor",
    "mash_donbass",
    "url_mash",
    "babr_mash",
    "mash_iptash",
    "amber_mash",
    "kub_mash",
    "kras_mash",
    "mash_nimash",
    "mash_siberia",
    "don_mash",
    "mash_na_volne",
    "mash_batash",
    "amur_mash",
]  # Начальные каналы для мониторинга

class TelegramForwarder:
    def __init__(self):
        self.message_queue = deque()
        self.last_send_time = datetime.min
        self.SEND_INTERVAL = timedelta(seconds=10)
        self.monitored_channels = set(str(c) for c in CHANNELS_TO_MONITOR)  # Поддержка и username и ID
        
        self.user_client = TelegramClient('user_session', API_ID, API_HASH)
        self.bot = TelegramClient('bot_session', API_ID, API_HASH)

    async def initialize(self):
        await self.user_client.start()
        await self.bot.start(bot_token=BOT_TOKEN)
        
        try:
            self.target_entity = await self.bot.get_entity(TARGET_GROUP_ID)
            print(f"Успешно подключились к группе: {getattr(self.target_entity, 'title', TARGET_GROUP_ID)}")
        except Exception as e:
            print(f"Ошибка доступа к группе: {str(e)}")
            raise

    def get_message_link(self, message):
        if not message or not hasattr(message, 'chat'):
            return None
        
        chat = message.chat
        chat_id = getattr(chat, 'id', None)
        username = getattr(chat, 'username', None)
        
        if username:
            return f"https://t.me/{username}/{message.id}"
        elif chat_id:
            return f"https://t.me/c/{chat_id}/{message.id}"
        return None

    async def process_queue(self):
        while True:
            try:
                if self.message_queue and datetime.now() - self.last_send_time >= self.SEND_INTERVAL:
                    chat_title, message = self.message_queue.popleft()
                    text = message.text or message.caption or ""
                    
                    if text.strip():
                        link = self.get_message_link(message)
                        msg = f"{text.strip()}"
                        if link:
                            msg += f"\n\n 1111 \n\n {link}"
                        
                        await self.bot.send_message(
                            entity=self.target_entity,
                            message=msg,
                            link_preview=False
                        )
                        self.last_send_time = datetime.now()
                        print(f"Отправлено сообщение из {chat_title}")
            
            except Exception as e:
                print(f"Ошибка при обработке очереди: {str(e)}")
                await asyncio.sleep(5)
            
            await asyncio.sleep(0.5)

    async def message_handler(self, event):
        try:
            chat = getattr(event, 'chat', None)
            if not chat:
                return
                
            # Проверяем по username или ID
            identifier = None
            chat_username = getattr(chat, 'username', None)
            chat_id = str(getattr(chat, 'id', None))
            
            if chat_username and chat_username.lower() in self.monitored_channels:
                identifier = chat_username
            elif chat_id in self.monitored_channels:
                identifier = chat_id
                
            if identifier:
                channel_name = getattr(chat, 'title', f'Channel {identifier}')
                self.message_queue.append((channel_name, event.message))
                print(f"Добавлено сообщение из {'@' + identifier if '@' not in identifier else identifier} ({channel_name})")
                
        except Exception as e:
            print(f"Ошибка в обработчике: {str(e)}")
            if hasattr(event, 'message') and hasattr(event.message, 'id'):
                print(f"Ошибка при обработке сообщения ID {event.message.id}")

    async def run(self):
        await self.initialize()
        self.user_client.add_event_handler(self.message_handler, events.NewMessage())
        
        asyncio.create_task(self.process_queue())
        print("Бот запущен и начал мониторинг каналов...")
        await self.user_client.run_until_disconnected()

async def main():
    forwarder = TelegramForwarder()
    await forwarder.run()

if __name__ == '__main__':
    asyncio.run(main())