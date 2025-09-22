from fastapi import FastAPI, Query
import asyncio
from main import telegram, fetch_and_store

app = FastAPI(title="Telegram Data Fetcher & Search API")


# ---------------------------
# Root Endpoint
# ---------------------------
@app.get("/")
def root():
    return {"message": "✅ Telegram Data Fetcher API is running"}


# ---------------------------
# Fetch messages from channel/group
# ---------------------------
@app.post("/fetch")
async def fetch_messages(chat_id: str = Query(..., description="Telegram channel/group username or ID"),
                         limit: int = Query(50, description="Number of recent messages to fetch")):
    """
    Fetch latest messages from a given Telegram channel/group
    and store them into MongoDB + MinIO.
    """
    try:
        async with telegram:
            result = await fetch_and_store(chat_id, limit)
        return {"chat_id": chat_id, "status": "success", "summary": result}
    except Exception as e:
        return {"chat_id": chat_id, "status": "error", "error": str(e)}


# ---------------------------
# Search stored messages
# ---------------------------
from db.mongo_config import messages_collection

@app.get("/search")
def search_messages(query: str = Query(..., description="Search text in stored Telegram messages"),
                    chat_id: str = Query(None, description="Optional: filter by chat_id")):
    """
    Search for messages stored in MongoDB.
    """
    search_filter = {"text": {"$regex": query, "$options": "i"}}
    if chat_id:
        search_filter["chat_id"] = str(chat_id)

    results = list(messages_collection.find(search_filter, {"_id": 0}))
    return {"count": len(results), "results": results}


from fastapi import FastAPI, Query, HTTPException
from twitter_fetcher import fetch_tweets
import asyncio

@app.get("/fetch_tweets", summary="Fetch tweets for one or more usernames")
async def fetch_user_tweets(usernames: str = Query(..., description="Comma-separated Twitter usernames"), max_results: int = 10):
    """Fetch tweets for single or multiple usernames."""
    user_list = [u.strip() for u in usernames.split(",") if u.strip()]
    if not user_list:
        raise HTTPException(status_code=400, detail="No valid usernames provided")

    tweets = await fetch_tweets(user_list, max_results=max_results)
    return {"fetched": len(tweets), "tweets": tweets}




