import hashlib
from transformers import pipeline

# Load Hugging Face model once
sentiment_model = pipeline(
    "sentiment-analysis",
    model="cardiffnlp/twitter-roberta-base-sentiment-latest"
)

def make_hash(value: str) -> str:
    """Generate SHA256 hash of a string."""
    return hashlib.sha256(value.encode()).hexdigest()

def analyze_sentiment(text: str) -> dict:
    if not text.strip():
        return {"label": "unknown", "score": 0.0}
    try:
        result = sentiment_model(text[:512])[0]
        return {"label": result["label"].lower(), "score": float(result["score"])}
    except Exception:
        return {"label": "unknown", "score": 0.0}
