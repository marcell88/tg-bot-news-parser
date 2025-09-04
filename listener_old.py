import time
from telethon import TelegramClient, events

# Замените 'ВАШ_ID_ПРИВАТНОЙ_ГРУППЫ' на фактический числовой ID вашей приватной группы.
# Вы можете получить этот ID, переслав сообщение из группы боту @userinfobot или
# используя client.get_entity('имя_вашей_группы_или_ссылка') в отдельном скрипте.
INPUT_CHANNEL = -1234567890 # Пример: Используйте фактический числовой ID вашей приватной группы

OUTPUT_CHANNEL = "@NewsFeedProcessing_bot"

api_id = '25491744'
api_hash = '0643451ea49fcac6f5a8697005714e33'

client = TelegramClient('anon2', api_id, api_hash)

async def main():
    await client.start()
    print("Клиент запущен. Мониторинг приватной группы...")

    @client.on(events.NewMessage(INPUT_CHANNEL))
    async def handler(event):
        print(f"Обнаружено новое сообщение в приватной группе: {event.message.text[:50]}...")
        # Пересылаем сообщение в выходной канал
        await client.forward_messages(OUTPUT_CHANNEL, event.message)
        print("Сообщение переслано.")
        time.sleep(5) # Добавляем небольшую задержку

    await client.run_until_disconnected()

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())