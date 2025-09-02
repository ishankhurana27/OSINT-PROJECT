import os
import io
import schedule
import time
from dotenv import load_dotenv
from telethon import TelegramClient
from db.mongo_config import messages_collection, fs
from db.minio_config import minio_client, bucket_name

load_dotenv()

# Load from .env
api_id = int(os.getenv("API_ID"))
api_hash = os.getenv("API_HASH")
phone = os.getenv("PHONE")

# Setup Telegram client
telegram = TelegramClient("session_name", api_id, api_hash)

# Your channel IDs
chat_ids = [
    -1002654095731,
    -1001704393040,
    -1001330162895,
    -1001554189930,
    -1001401190279,
]

async def fetch_and_store():
    """Fetch new messages from multiple Telegram channels and save them."""
    await telegram.start(phone=phone)

    for chat_id in chat_ids:
        print(f"📌 Fetching messages from {chat_id}...")
        try:
            # Resolve entity (ensures Telethon recognizes the chat)
            entity = await telegram.get_entity(chat_id)

            async for message in telegram.iter_messages(entity, limit=50):
                doc = {
                    "message_id": message.id,
                    "text": message.text,
                    "date": str(message.date),
                    "sender_id": message.sender_id,
                    "chat_id": chat_id,
                }

                # --- Handle media ---
                if message.media:
                    buffer = io.BytesIO()
                    await message.download_media(file=buffer)
                    buffer.seek(0)

                    content_type = getattr(message.file, "mime_type", "application/octet-stream")

                    # 1) Save to GridFS
                    file_id = fs.put(
                        buffer,
                        filename=f"{message.id}",
                        content_type=content_type,
                    )
                    doc["gridfs_id"] = str(file_id)

                    # 2) Save to MinIO
                    buffer.seek(0)
                    minio_client.put_object(
                        bucket_name,
                        f"{message.id}",
                        buffer,
                        length=len(buffer.getvalue()),
                        content_type=content_type,
                    )
                    doc["minio_object"] = f"{message.id}"

                # --- Save metadata ---
                messages_collection.insert_one(doc)
                print(f"✅ Saved message {message.id} from {chat_id}")

        except Exception as e:
            print(f"❌ Error fetching from {chat_id}: {e}")


def job():
    """Run async fetch task inside schedule."""
    with telegram:
        telegram.loop.run_until_complete(fetch_and_store())


# Run once at startup
job()

# Schedule every 60 minutes
schedule.every(60).minutes.do(job)

print("📌 Scheduler started... Fetching every 60 mins.")

# Keep running forever
while True:
    schedule.run_pending()
    time.sleep(1)
