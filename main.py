# import os
# import io
# import time
# import schedule
# from dotenv import load_dotenv
# from telethon import TelegramClient
# from datetime import datetime
# from db.es_config import es, index_name
# from db.minio_config import minio_client, bucket_name
# from search import app  # import FastAPI instance from search.py


# # ---------------------------
# # Load .env
# # ---------------------------
# load_dotenv()

# # ---------------------------
# # Telegram Config
# # ---------------------------
# TELEGRAM_API_ID = int(os.getenv("API_ID"))
# TELEGRAM_API_HASH = os.getenv("API_HASH")
# TELEGRAM_PHONE = os.getenv("PHONE")
# TELEGRAM_SESSION = "telegram_session"

# # Channels (IDs or usernames) → add multiple
# CHAT_IDS = [
#     "DSquadOfficial"
# ]

# telegram = TelegramClient(TELEGRAM_SESSION, TELEGRAM_API_ID, TELEGRAM_API_HASH)


# # ---------------------------
# # Helpers
# # ---------------------------
# def clean_for_indexing(doc):
#     """Convert datetime to ISO string for ES."""
#     return {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in doc.items()}


# def message_exists(chat_id, message_id):
#     """Check if message already exists in Elasticsearch."""
#     doc_id = f"{chat_id}_{message_id}"
#     return es.exists(index=index_name, id=doc_id)


# # ---------------------------
# # Main fetch
# # ---------------------------
# async def fetch_and_store(chat_id):
#     print(f"📌 Fetching messages from {chat_id}...")
#     success_es, skipped, fail_es = 0, 0, 0

#     async for message in telegram.iter_messages(chat_id, limit=50):
#         if not (message.text or message.media):
#             continue

#         # Skip if already indexed
#         if message_exists(chat_id, message.id):
#             skipped += 1
#             continue

#         media_url = None
#         if message.media:
#             file_name = f"{chat_id}_{message.id}"

#             try:
#                 # --- In-memory upload to MinIO ---
#                 buffer = io.BytesIO()
#                 await message.download_media(file=buffer)
#                 buffer.seek(0)
#                 minio_client.put_object(
#                     bucket_name,
#                     file_name,
#                     buffer,
#                     length=len(buffer.getvalue()),
#                     content_type="application/octet-stream"
#                 )

#                 media_url = f"http://{os.getenv('MINIO_ENDPOINT')}/{bucket_name}/{file_name}"
#                 print(f"✅ Stored media {file_name} in MinIO")

#             except Exception as e:
#                 print(f"⚠️ Media upload failed (msg {message.id}): {e}")

#         doc = {
#             "message_id": str(message.id),
#             "text": message.text,
#             "date": message.date,
#             "sender_id": str(message.sender_id) if message.sender_id else None,
#             "chat_id": str(chat_id),
#             "media_url": media_url,
#         }

#         clean_doc = clean_for_indexing(doc)

#         try:
#             es.index(index=index_name, id=f"{chat_id}_{doc['message_id']}", document=clean_doc)
#             success_es += 1
#             print(f"✅ ES: Inserted message {doc['message_id']}")
#         except Exception as e:
#             fail_es += 1
#             print(f"⚠️ ES index error (msg {doc['message_id']}): {e}")

#     print("\n📊 Summary for chat:", chat_id)
#     print(f"   ✅ Inserted: {success_es}, ⏭️ Skipped (already exists): {skipped}, ⚠️ Failed: {fail_es}")


# # ---------------------------
# # Job wrapper
# # ---------------------------
# def job():
#     with telegram:
#         for chat_id in CHAT_IDS:
#             try:
#                 telegram.loop.run_until_complete(fetch_and_store(chat_id))
#             except Exception as e:
#                 print(f"⚠️ Skipping {chat_id} due to error: {e}")


# # ---------------------------
# # Main loop with scheduler
# # ---------------------------
# if __name__ == "__main__":
#     # Run once at startup
#     job()

#     # Schedule every 60 minutes
#     schedule.every(60).minutes.do(job)
#     print("📌 Scheduler started... Fetching every 60 mins.")

#     while True:
#         schedule.run_pending()
#         time.sleep(1)


import os
import io
import time
import schedule
from dotenv import load_dotenv
from telethon import TelegramClient
from datetime import datetime
from db.es_config import es, index_name
from db.minio_config import minio_client, bucket_name
from search import app  # import FastAPI instance from search.py


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

# Channels (IDs or usernames) → add multiple
CHAT_IDS = [
    "DSquadOfficial"
]

telegram = TelegramClient(TELEGRAM_SESSION, TELEGRAM_API_ID, TELEGRAM_API_HASH)


# ---------------------------
# Helpers
# ---------------------------
def clean_for_indexing(doc):
    """Convert datetime to ISO string for ES."""
    return {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in doc.items()}


def message_exists(chat_id, message_id):
    """Check if message already exists in Elasticsearch."""
    doc_id = f"{chat_id}_{message_id}"
    return es.exists(index=index_name, id=doc_id)


# ---------------------------
# Main fetch
# ---------------------------
async def fetch_and_store(chat_id):
    print(f"📌 Fetching messages from {chat_id}...")
    success_es, skipped, fail_es = 0, 0, 0

    async for message in telegram.iter_messages(chat_id, limit=50):
        if not (message.text or message.media):
            continue

        # Skip if already indexed
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
            "date": message.date,
            "sender_id": str(message.sender_id) if message.sender_id else None,
            "chat_id": str(chat_id),
            # ✅ Save media_url instead of image_base64
            "media_url": media_url,
        }

        clean_doc = clean_for_indexing(doc)

        try:
            es.index(index=index_name, id=f"{chat_id}_{doc['message_id']}", document=clean_doc)
            success_es += 1
            print(f"✅ ES: Inserted message {doc['message_id']}")
        except Exception as e:
            fail_es += 1
            print(f"⚠️ ES index error (msg {doc['message_id']}): {e}")

    print("\n📊 Summary for chat:", chat_id)
    print(f"   ✅ Inserted: {success_es}, ⏭️ Skipped (already exists): {skipped}, ⚠️ Failed: {fail_es}")


# ---------------------------
# Job wrapper
# ---------------------------
def job():
    with telegram:
        for chat_id in CHAT_IDS:
            try:
                telegram.loop.run_until_complete(fetch_and_store(chat_id))
            except Exception as e:
                print(f"⚠️ Skipping {chat_id} due to error: {e}")


# ---------------------------
# Main loop with scheduler
# ---------------------------
if __name__ == "__main__":
    # Run once at startup
    job()

    # Schedule every 60 minutes
    schedule.every(60).minutes.do(job)
    print("📌 Scheduler started... Fetching every 60 mins.")

    while True:
        schedule.run_pending()
        time.sleep(1)
