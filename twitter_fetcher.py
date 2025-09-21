import os
import io
import httpx
import tweepy
from db.mongo_config import tweets_collection
from dotenv import load_dotenv
from db.minio_config import minio_client, twitter_bucket


load_dotenv()
bearer_token = os.getenv("TWITTER_BEARER_TOKEN")
if not bearer_token:
    raise ValueError("❌ Missing TWITTER_BEARER_TOKEN in .env")

client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)
last_seen = {}

async def download_media_to_minio(tweet_id: str, url: str):
    """Download media asynchronously and store in MinIO."""
    async with httpx.AsyncClient() as session:
        resp = await session.get(url)
        resp.raise_for_status()
        buffer = io.BytesIO(resp.content)

    minio_client.put_object(
        twitter_bucket,
        f"{tweet_id}.jpg",
        buffer,
        length=len(buffer.getvalue()),
        content_type="image/jpeg"
    )


    return f"{tweet_id}.jpg"

async def fetch_tweets(usernames: list[str], max_results: int = 10):
    """Fetch tweets for a list of usernames."""
    all_tweets = []

    for username in usernames:
        username = username.strip()
        try:
            user_data = client.get_user(username=username)
            if not user_data or not user_data.data:
                continue
            user_id = user_data.data.id
            since_id = last_seen.get(username)

            tweets = client.get_users_tweets(
                id=user_id,
                since_id=since_id,
                max_results=max_results,
                tweet_fields=["id", "text", "created_at"],
                expansions=["attachments.media_keys"],
                media_fields=["url", "type"]
            )

            if not tweets.data:
                continue

            last_seen[username] = tweets.data[0].id
            includes = tweets.includes or {}
            media_map = {m["media_key"]: m for m in includes.get("media", [])}

            for tweet in tweets.data:
                doc = {
                    "tweet_id": str(tweet.id),
                    "text": tweet.text,
                    "date": str(tweet.created_at),
                    "user_id": user_id,
                    "username": username,
                    "media_files": []
                }

                if tweet.attachments and "media_keys" in tweet.attachments:
                    for key in tweet.attachments["media_keys"]:
                        media = media_map.get(key)
                        if media and "url" in media:
                            try:
                                minio_obj = await download_media_to_minio(tweet.id, media["url"])
                                doc["media_files"].append(minio_obj)
                            except Exception as e:
                                print(f"❌ Error downloading media: {e}")

                res = tweets_collection.insert_one(doc)
                doc["_id"] = str(res.inserted_id)
                all_tweets.append(doc)

        except tweepy.TooManyRequests:
            print(f"⚠️ Rate limit hit for @{username}, skipping...")
        except Exception as e:
            print(f"❌ Error for @{username}: {e}")

    return all_tweets
