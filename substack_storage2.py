
import io
import hashlib
import requests
from minio import Minio
from minio.error import S3Error
from pymongo import MongoClient
from transformers import pipeline

# Load Hugging Face sentiment analysis model once
sentiment_model = pipeline(
    "sentiment-analysis",
    model="cardiffnlp/twitter-roberta-base-sentiment-latest"
)

def analyze_sentiment(text: str) -> dict:
    """
    Run transformer sentiment analysis on text.
    Returns a dict with label and confidence score.
    """
    if not text or not text.strip():
        return {"label": "unknown", "score": 0.0}

    try:
        # Truncate text to 512 characters to avoid model overload
        result = sentiment_model(text[:512])[0]
        sentiment = result["label"].lower()   # 'positive', 'negative', 'neutral'
        confidence = round(float(result["score"]), 4)
    except Exception as e:
        print(f"⚠️ Sentiment analysis failed: {e}")
        sentiment = "unknown"
        confidence = 0.0

    return {"label": sentiment, "score": confidence}


def make_hash(value: str) -> str:
    """Generate SHA256 hash of a string (used for deduplication in MongoDB)."""
    return hashlib.sha256(value.encode()).hexdigest()


class MongoStore:
    def __init__(self, uri, database, collection):
        self.client = MongoClient(uri)
        self.collection = self.client[database][collection]

    def insert_post(self, post: dict):
        """Insert post with sentiment if not already present."""
        post_id = make_hash(post["link"])
        if not self.collection.find_one({"_id": post_id}):
            post["_id"] = post_id

            # Run transformer sentiment analysis via helper function
            sentiment_result = analyze_sentiment(post.get("content", ""))
            post["sentiment"] = sentiment_result["label"]
            post["confidence"] = sentiment_result["score"]

            self.collection.insert_one(post)
            return True
        return False


class MinioStore:
    def __init__(self, endpoint, access_key, secret_key, bucket):
        self.client = Minio(
            endpoint.replace("http://", "").replace("https://", ""),
            access_key=access_key,
            secret_key=secret_key,
            secure=endpoint.startswith("https")
        )
        self.bucket = bucket

        if not self.client.bucket_exists(bucket):
            self.client.make_bucket(bucket)

    def upload_from_url(self, url, object_name):
        """
        Uploads a file from a URL to MinIO.
        ✅ Skips upload if object already exists.
        """
        try:
            # Check if object already exists
            try:
                self.client.stat_object(self.bucket, object_name)
                return f"{self.bucket}/{object_name}"
            except S3Error as e:
                if e.code != "NoSuchKey":
                    raise

            # If not found → fetch & upload
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200:
                self.client.put_object(
                    self.bucket,
                    object_name,
                    io.BytesIO(resp.content),
                    length=len(resp.content)
                )
                return f"{self.bucket}/{object_name}"
            else:
                print(f"⚠️ Failed to fetch {url}, status {resp.status_code}")
                return None
        except Exception as e:
            print(f"❌ Failed to upload {url}: {e}")
            return None
