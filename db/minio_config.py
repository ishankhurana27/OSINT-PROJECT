# # from minio import Minio
# # import os
# # from dotenv import load_dotenv

# # load_dotenv()

# # minio_client = Minio(
# #     os.getenv("MINIO_ENDPOINT"),
# #     access_key=os.getenv("MINIO_ACCESS_KEY"),
# #     secret_key=os.getenv("MINIO_SECRET_KEY"),
# #     secure=False
# # )

# # bucket_name = os.getenv("MINIO_BUCKET", "telegram-media")

# # # Ensure bucket exists
# # if not minio_client.bucket_exists(bucket_name):
# #     minio_client.make_bucket(bucket_name)


# from minio import Minio
# import os
# from dotenv import load_dotenv

# load_dotenv()

# minio_client = Minio(
#     os.getenv("MINIO_ENDPOINT", "localhost:9000"),
#     access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
#     secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
#     secure=False
# )

# # Telegram bucket
# telegram_bucket = os.getenv("MINIO_BUCKET", "telegram-media")
# if not minio_client.bucket_exists(telegram_bucket):
#     minio_client.make_bucket(telegram_bucket)

# # Twitter bucket
# twitter_bucket = os.getenv("TWITTER_MINIO_BUCKET", "twitter-media")
# if not minio_client.bucket_exists(twitter_bucket):
#     minio_client.make_bucket(twitter_bucket)

# # Substack bucket
# substack_bucket = os.getenv("SUBSTACK_MINIO_BUCKET", "substack-media")
# if not minio_client.bucket_exists(substack_bucket):
#     minio_client.make_bucket(substack_bucket)

# # 👇 Backward compatibility for old imports
# bucket_name = telegram_bucket

# db/minio_config.py

from minio import Minio
from minio.error import S3Error
import os
import requests
import io
from dotenv import load_dotenv

load_dotenv()

minio_client = Minio(
    os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    secure=False
)

telegram_bucket = os.getenv("MINIO_BUCKET", "telegram-media")
twitter_bucket = os.getenv("TWITTER_MINIO_BUCKET", "twitter-media")
substack_bucket = os.getenv("SUBSTACK_MINIO_BUCKET", "substack-media")

for bucket in [telegram_bucket, twitter_bucket, substack_bucket]:
    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

class MinioStore:
    def __init__(self, bucket=substack_bucket):
        self.client = minio_client
        self.bucket = bucket

    def upload_from_url(self, url, object_name):
        try:
            # Check if already exists
            try:
                self.client.stat_object(self.bucket, object_name)
                return f"{self.bucket}/{object_name}"
            except S3Error as e:
                if e.code != "NoSuchKey":
                    raise

            resp = requests.get(url, timeout=15)
            if resp.status_code == 200:
                self.client.put_object(
                    self.bucket,
                    object_name,
                    io.BytesIO(resp.content),
                    length=len(resp.content)
                )
                return f"{self.bucket}/{object_name}"
            return None
        except Exception as e:
            print(f"❌ MinIO upload failed: {e}")
            return None
# Backward compatibility
bucket_name = telegram_bucket
