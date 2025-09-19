import os
import io
from dotenv import load_dotenv
from telethon import TelegramClient
from datetime import datetime
from db.minio_config import minio_client, bucket_name
from db.mongo_config import messages_collection  # ✅ Use MongoDB


# ---------------------------
# Load .env
# ---------------------------
load_dotenv()

# ---------------------------
# Telegram Config
# ---------------------------
TELEGRAM_API_ID = int(os.getenv("API_ID"))
TELEGRAM_API_HASH = os.getenv("API_HASH")
TELEGRAM_PHONE = os.getenv("PHONE")
TELEGRAM_SESSION = "telegram_session"

telegram = TelegramClient(TELEGRAM_SESSION, TELEGRAM_API_ID, TELEGRAM_API_HASH)


# ---------------------------
# Helpers
# ---------------------------
def message_exists(chat_id, message_id):
    """Check if message already exists in MongoDB."""
    return messages_collection.find_one({"chat_id": str(chat_id), "message_id": str(message_id)}) is not None


# ---------------------------
# Fetch + Store Function
# ---------------------------
async def fetch_and_store(chat_id: str, limit: int = 50):
    """
    Fetch latest messages from a given Telegram channel/group 
    and store them in MongoDB + MinIO.
    """
    print(f"📌 Fetching messages from {chat_id}...")
    success_db, skipped, fail_db = 0, 0, 0

    async for message in telegram.iter_messages(chat_id, limit=limit):
        if not (message.text or message.media):
            continue

        # Skip if already indexed in MongoDB
        if message_exists(chat_id, message.id):
            skipped += 1
            continue

        media_url = None
        if message.media:
            file_name = f"{chat_id}_{message.id}"

            try:
                # --- In-memory upload to MinIO ---
                buffer = io.BytesIO()
                await message.download_media(file=buffer)
                buffer.seek(0)
                minio_client.put_object(
                    bucket_name,
                    file_name,
                    buffer,
                    length=len(buffer.getvalue()),
                    content_type="application/octet-stream"
                )

                # ✅ Store the file URL instead of base64
                media_url = f"http://{os.getenv('MINIO_ENDPOINT')}/{bucket_name}/{file_name}"
                print(f"✅ Stored media {file_name} in MinIO")

            except Exception as e:
                print(f"⚠️ Media upload failed (msg {message.id}): {e}")

        doc = {
            "message_id": str(message.id),
            "text": message.text,
            "date": message.date.isoformat() if isinstance(message.date, datetime) else message.date,
            "sender_id": str(message.sender_id) if message.sender_id else None,
            "chat_id": str(chat_id),
            "media_url": media_url,
        }

        try:
            messages_collection.insert_one(doc)
            success_db += 1
            print(f"✅ MongoDB: Inserted message {doc['message_id']}")
        except Exception as e:
            fail_db += 1
            print(f"⚠️ MongoDB insert error (msg {doc['message_id']}): {e}")

    print("\n📊 Summary for chat:", chat_id)
    print(f"   ✅ Inserted: {success_db}, ⏭️ Skipped (already exists): {skipped}, ⚠️ Failed: {fail_db}")

    return {
        "inserted": success_db,
        "skipped": skipped,
        "failed": fail_db
    }
