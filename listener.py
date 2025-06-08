import time
from telethon import TelegramClient, events

INPUT_CHANNEL = [
    "@bbbreaking",
    "@mash"
]

OUTPUT_CHANNEL = "@NewsFeedProcessing_bot"

api_id = '25491744'
api_hash = '0643451ea49fcac6f5a8697005714e33'

client = TelegramClient('anon2', api_id, api_hash)
client.start()

@client.on(events.NewMessage(INPUT_CHANNEL))

async def main(event):
    await client.forward_messages(OUTPUT_CHANNEL, event.message)
    time.sleep(5)

client.run_until_disconnected()