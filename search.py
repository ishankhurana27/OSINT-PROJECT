# import asyncio
# import aiohttp
# import feedparser
# from bs4 import BeautifulSoup
# from urllib.parse import urlparse
# import os
# import yaml

# from fastapi import FastAPI, Query, HTTPException

# # ---- Local imports ----
# from main import telegram, fetch_and_store
# from db.mongo_config import messages_collection
# from db.minio_config import MinioStore
# from twitter_fetcher import fetch_tweets
# from helpers import make_hash, analyze_sentiment

# from db.mongo_config import MongoStore
# from db.minio_config import MinioStore, substack_bucket

# mongo = MongoStore()  # now points to substack_collection
# minio = MinioStore(bucket=substack_bucket)


# # ---------------------------
# # Load config.yaml
# # ---------------------------
# with open("config.yaml", "r") as f:
#     config = yaml.safe_load(f)

# # Initialize MongoDB + MinIO
# mongo = MongoStore(config["mongo"]["uri"], config["mongo"]["database"], config["mongo"]["collection"])
# minio = MinioStore(config["minio"]["endpoint"], config["minio"]["access_key"], config["minio"]["secret_key"], config["minio"]["bucket"])


# # ---------------------------
# # FastAPI App
# # ---------------------------
# app = FastAPI(title="Unified Data API (Telegram + Twitter + Substack)")


# # ---------------------------
# # Root
# # ---------------------------
# @app.get("/")
# def root():
#     return {"message": "✅ Unified Data API is running (Telegram + Twitter + Substack)"}


# # =====================================================
# #  TELEGRAM
# # =====================================================
# @app.post("/fetch")
# async def fetch_messages(chat_id: str = Query(..., description="Telegram channel/group username or ID"),
#                          limit: int = Query(50, description="Number of recent messages to fetch")):
#     """Fetch latest messages from a Telegram channel/group and store them into MongoDB + MinIO."""
#     try:
#         async with telegram:
#             result = await fetch_and_store(chat_id, limit)
#         return {"chat_id": chat_id, "status": "success", "summary": result}
#     except Exception as e:
#         return {"chat_id": chat_id, "status": "error", "error": str(e)}


# @app.get("/search")
# def search_messages(query: str = Query(..., description="Search text in stored Telegram messages"),
#                     chat_id: str = Query(None, description="Optional: filter by chat_id")):
#     """Search for messages stored in MongoDB."""
#     search_filter = {"text": {"$regex": query, "$options": "i"}}
#     if chat_id:
#         search_filter["chat_id"] = str(chat_id)

#     results = list(messages_collection.find(search_filter, {"_id": 0}))
#     return {"count": len(results), "results": results}


# # =====================================================
# #  TWITTER
# # =====================================================
# @app.get("/fetch_tweets", summary="Fetch tweets for one or more usernames")
# async def fetch_user_tweets(usernames: str = Query(..., description="Comma-separated Twitter usernames"),
#                             max_results: int = 10):
#     """Fetch tweets for single or multiple usernames."""
#     user_list = [u.strip() for u in usernames.split(",") if u.strip()]
#     if not user_list:
#         raise HTTPException(status_code=400, detail="No valid usernames provided")

#     tweets = await fetch_tweets(user_list, max_results=max_results)
#     return {"fetched": len(tweets), "tweets": tweets}


# # =====================================================
# #  SUBSTACK
# # =====================================================
# async def process_feed(session, feed_url: str):
#     """Process a single Substack RSS feed and store results in Mongo + MinIO."""
#     feed = feedparser.parse(feed_url)
#     results = []

#     for entry in feed.entries:
#         content = entry.get("content", [{}])[0].get("value", entry.get("summary", ""))
#         soup = BeautifulSoup(content, "html.parser")
#         text_content = soup.get_text(separator="\n", strip=True)

#         # Sentiment analysis
#         sentiment_label, confidence_score = analyze_sentiment(text_content)

#         # Extract media
#         media_urls = []
#         for img in soup.find_all("img"):
#             if img.get("src"):
#                 media_urls.append(img["src"])
#         for video in soup.find_all("video"):
#             if video.get("src"):
#                 media_urls.append(video["src"])

#         content_hash = make_hash(text_content)

#         post = {
#             "title": entry.get("title", "Untitled"),
#             "link": entry.get("link", ""),
#             "published": entry.get("published", ""),
#             "content": text_content,
#             "content_hash": content_hash,
#             "media": [],
#             "sentiment": sentiment_label,
#             "confidence": confidence_score,
#         }

#         # Upload media to MinIO
#         for media_url in media_urls:
#             filename = os.path.basename(urlparse(media_url).path)
#             object_name = f"{content_hash}/{filename}"
#             stored_path = minio.upload_from_url(media_url, object_name)
#             if stored_path:
#                 post["media"].append(stored_path)

#         # Store in MongoDB
#         mongo.insert_post(post)
#         results.append(post)

#     return results


# @app.get("/scrape_substack")
# async def scrape(feed_url: str = Query(..., description="Substack RSS feed URL")):
#     """Fetch posts from a Substack RSS feed, store in MongoDB + MinIO."""
#     async with aiohttp.ClientSession() as session:
#         posts = await process_feed(session, feed_url)
#     return {"fetched_posts": len(posts), "details": posts}


import asyncio
import aiohttp
import feedparser
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import os

from fastapi import FastAPI, Query, HTTPException

# ---- Local imports ----
from telegram_main import telegram, fetch_and_store
from db.mongo_config import MongoStore, messages_collection
from db.minio_config import MinioStore, substack_bucket
from twitter_fetcher import fetch_tweets
from helpers import make_hash, analyze_sentiment

# ---------------------------
# Initialize MongoDB + MinIO
# ---------------------------
mongo = MongoStore()  # connects to your configured Mongo collection
minio = MinioStore(bucket=substack_bucket)  # connects to your configured MinIO bucket

# ---------------------------
# FastAPI App
# ---------------------------
app = FastAPI(title="Unified Data API (Telegram + Twitter + Substack)")

# ---------------------------
# Root
# ---------------------------
@app.get("/")
def root():
    return {"message": "✅ Unified Data API is running (Telegram + Twitter + Substack)"}

# =====================================================
#  TELEGRAM
# =====================================================
@app.post("/fetch")
async def fetch_messages(chat_id: str = Query(..., description="Telegram channel/group username or ID"),
                         limit: int = Query(50, description="Number of recent messages to fetch")):
    """Fetch latest messages from a Telegram channel/group and store them into MongoDB + MinIO."""
    try:
        async with telegram:
            result = await fetch_and_store(chat_id, limit)
        return {"chat_id": chat_id, "status": "success", "summary": result}
    except Exception as e:
        return {"chat_id": chat_id, "status": "error", "error": str(e)}


@app.get("/search")
def search_messages(query: str = Query(..., description="Search text in stored Telegram messages"),
                    chat_id: str = Query(None, description="Optional: filter by chat_id")):
    """Search for messages stored in MongoDB."""
    search_filter = {"text": {"$regex": query, "$options": "i"}}
    if chat_id:
        search_filter["chat_id"] = str(chat_id)

    results = list(messages_collection.find(search_filter, {"_id": 0}))
    return {"count": len(results), "results": results}

# =====================================================
#  TWITTER
# =====================================================
@app.get("/fetch_tweets", summary="Fetch tweets for one or more usernames")
async def fetch_user_tweets(usernames: str = Query(..., description="Comma-separated Twitter usernames"),
                            max_results: int = 10):
    """Fetch tweets for single or multiple usernames."""
    user_list = [u.strip() for u in usernames.split(",") if u.strip()]
    if not user_list:
        raise HTTPException(status_code=400, detail="No valid usernames provided")

    tweets = await fetch_tweets(user_list, max_results=max_results)
    return {"fetched": len(tweets), "tweets": tweets}

# =====================================================
#  SUBSTACK
# =====================================================
async def process_feed(session, feed_url: str):
    """Process a single Substack RSS feed and store results in Mongo + MinIO."""
    feed = feedparser.parse(feed_url)
    results = []

    for entry in feed.entries:
        content = entry.get("content", [{}])[0].get("value", entry.get("summary", ""))
        soup = BeautifulSoup(content, "html.parser")
        text_content = soup.get_text(separator="\n", strip=True)

        # Sentiment analysis
        sentiment_label, confidence_score = analyze_sentiment(text_content)

        # Extract media
        media_urls = []
        for img in soup.find_all("img"):
            if img.get("src"):
                media_urls.append(img["src"])
        for video in soup.find_all("video"):
            if video.get("src"):
                media_urls.append(video["src"])

        content_hash = make_hash(text_content)

        post = {
            "title": entry.get("title", "Untitled"),
            "link": entry.get("link", ""),
            "published": entry.get("published", ""),
            "content": text_content,
            "content_hash": content_hash,
            "media": [],
            "sentiment": sentiment_label,
            "confidence": confidence_score,
        }

        # Upload media to MinIO
        for media_url in media_urls:
            filename = os.path.basename(urlparse(media_url).path)
            object_name = f"{content_hash}/{filename}"
            stored_path = minio.upload_from_url(media_url, object_name)
            if stored_path:
                post["media"].append(stored_path)

        # Store in MongoDB
        mongo.insert_post(post)
        results.append(post)

    return results


@app.get("/scrape_substack")
async def scrape(feed_url: str = Query(..., description="Substack RSS feed URL")):
    """Fetch posts from a Substack RSS feed, store in MongoDB + MinIO."""
    async with aiohttp.ClientSession() as session:
        posts = await process_feed(session, feed_url)
    return {"fetched_posts": len(posts), "details": posts}
