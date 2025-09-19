# from elasticsearch import Elasticsearch
# import os
# from dotenv import load_dotenv

# load_dotenv()

# es = Elasticsearch(
#     [os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")],
#     request_timeout=30
# )

# index_name = "telegram_messages"

# # Ensure index exists with mapping
# if not es.indices.exists(index=index_name):
#     es.indices.create(
#         index=index_name,
#         body={
#             "mappings": {
#                 "properties": {
#                     "message_id": {"type": "keyword"},
#                     "chat_id": {"type": "keyword"},
#                     "text": {"type": "text"},
#                     "date": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
#                     "sender_id": {"type": "keyword"},
#                     "media_url": {"type": "keyword"}  # ✅ added
#                 }
#             }
#         },
#         ignore=400,  # ignore "already exists" errors
#     )
#     print(f"📌 Created Elasticsearch index: {index_name} with media_url mapping")
# else:
#     print(f"✅ Elasticsearch index already exists: {index_name}")
