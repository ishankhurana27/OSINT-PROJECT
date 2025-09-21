# from minio import Minio
# import os
# from dotenv import load_dotenv

# load_dotenv()

# minio_client = Minio(
#     os.getenv("MINIO_ENDPOINT"),
#     access_key=os.getenv("MINIO_ACCESS_KEY"),
#     secret_key=os.getenv("MINIO_SECRET_KEY"),
#     secure=False
# )

# bucket_name = os.getenv("MINIO_BUCKET", "telegram-media")

# # Ensure bucket exists
# if not minio_client.bucket_exists(bucket_name):
#     minio_client.make_bucket(bucket_name)


from minio import Minio
import os
from dotenv import load_dotenv

load_dotenv()

minio_client = Minio(
    os.getenv("MINIO_ENDPOINT"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False
)

# Telegram bucket
telegram_bucket = os.getenv("MINIO_BUCKET", "telegram-media")
if not minio_client.bucket_exists(telegram_bucket):
    minio_client.make_bucket(telegram_bucket)

# Twitter bucket
twitter_bucket = os.getenv("TWITTER_MINIO_BUCKET", "twitter-media")
if not minio_client.bucket_exists(twitter_bucket):
    minio_client.make_bucket(twitter_bucket)

# 👇 Backward compatibility for old imports
bucket_name = telegram_bucket
