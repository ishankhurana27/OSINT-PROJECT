from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from db.es_config import es, index_name
from nltk.sentiment import SentimentIntensityAnalyzer




# Initialize FastAPI
app = FastAPI(title="Telegram Search API")

# Initialize Sentiment Analyzer
sia = SentimentIntensityAnalyzer()

def analyze_sentiment(text: str) -> str:
    """Return sentiment label: positive, negative, or neutral."""
    if not text:
        return "neutral"
    scores = sia.polarity_scores(text)
    compound = scores["compound"]
    if compound >= 0.05:
        return "positive"
    elif compound <= -0.05:
        return "negative"
    else:
        return "neutral"

@app.get("/search")
def search(query: str = Query(..., description="Search text or keyword")):
    """
    Search stored Telegram messages and return text + media URLs + sentiment.
    """
    body = {
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["text"]
            }
        }
    }

    try:
        res = es.search(index=index_name, body=body)
        hits = res["hits"]["hits"]
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

    results = []
    for hit in hits:
        src = hit["_source"]
        text = src.get("text")

        item = {
            "message_id": src.get("message_id"),
            "text": text,
            "date": src.get("date"),
            "media_url": src.get("media_url"),
            # ✅ Add sentiment
            "sentiment": analyze_sentiment(text)
        }
        results.append(item)

    return {"results": results}
