from fastapi import FastAPI, Query, HTTPException
from twitter_fetcher import fetch_tweets
import asyncio

app = FastAPI(title="Twitter Fetcher API")

@app.get("/fetch_tweets", summary="Fetch tweets for one or more usernames")
async def fetch_user_tweets(usernames: str = Query(..., description="Comma-separated Twitter usernames"), max_results: int = 10):
    """Fetch tweets for single or multiple usernames."""
    user_list = [u.strip() for u in usernames.split(",") if u.strip()]
    if not user_list:
        raise HTTPException(status_code=400, detail="No valid usernames provided")

    tweets = await fetch_tweets(user_list, max_results=max_results)
    return {"fetched": len(tweets), "tweets": tweets}
