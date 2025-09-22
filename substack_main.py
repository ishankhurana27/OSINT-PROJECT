import asyncio
import aiohttp
import feedparser
import logging
import argparse
import yaml
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import os
import time

from db.mongo_config import MongoStore
from db.minio_config import MinioStore
from helpers import make_hash, analyze_sentiment


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


async def fetch(session, url):
    """Fetch content from URL asynchronously."""
    try:
        async with session.get(url, timeout=30) as response:
            return await response.text()
    except Exception as e:
        logger.error(f"❌ Failed to fetch {url}: {e}")
        return None


async def process_feed(session, feed_url, mongo, minio):
    logger.info(f"📡 Fetching RSS feed: {feed_url}")
    try:
        feed = feedparser.parse(feed_url)
    except Exception as e:
        logger.error(f"❌ Failed to parse feed {feed_url}: {e}")
        return

    for entry in feed.entries:
        # Prefer full content if available, else fallback
        content = entry.get("content", [{}])[0].get("value", entry.get("summary", ""))
        soup = BeautifulSoup(content, "html.parser")
        text_content = soup.get_text(separator="\n", strip=True)
        
        sentiment_label, confidence_score = analyze_sentiment(text_content)

        # Extract images/media
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
            "confidence": confidence_score 
        }

        # Upload media to MinIO
        for media_url in media_urls:
            filename = os.path.basename(urlparse(media_url).path)
            object_name = f"{content_hash}/{filename}"
            stored_path = minio.upload_from_url(media_url, object_name)
            if stored_path:
                post["media"].append(stored_path)

        if mongo.insert_post(post):
            logger.info(f"✅ Inserted post '{post['title']}' with sentiment {post['sentiment']} (confidence: {post['confidence']})")
        else:
            logger.info(f"⚠️ Skipped duplicate: {post['title']}")


async def main(config_path):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    mongo = MongoStore(config["mongo"]["uri"], config["mongo"]["database"], config["mongo"]["collection"])
    minio = MinioStore(config["minio"]["endpoint"], config["minio"]["access_key"],
                       config["minio"]["secret_key"], config["minio"]["bucket"])

    async with aiohttp.ClientSession() as session:
        tasks = [process_feed(session, url, mongo, minio) for url in config["scrape"]["feeds"]]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    args = parser.parse_args()

    while True:
        try:
            asyncio.run(main(args.config))
        except Exception as e:
            logger.error(f"❌ Error during run: {e}")

        logger.info("⏳ Waiting 60 minutes before next run...")
        time.sleep(3600)  # 3600 seconds = 60 minutes
