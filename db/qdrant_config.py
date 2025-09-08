# from qdrant_client import QdrantClient
# from qdrant_client.http import models

# qdrant_client = QdrantClient(host="localhost", port=6333)

# collection_name = "telegram_messages"

# # Ensure collection exists
# collections = [c.name for c in qdrant_client.get_collections().collections]
# if collection_name not in collections:
#     qdrant_client.recreate_collection(
#         collection_name=collection_name,
#         vectors_config=models.VectorParams(size=384, distance=models.Distance.COSINE),
#     )
#     print(f"📌 Created Qdrant collection: {collection_name}")
