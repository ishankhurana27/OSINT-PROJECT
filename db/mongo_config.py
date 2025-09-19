import pymongo
import gridfs
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

client = pymongo.MongoClient(MONGO_URI)
db = client["telegram_db"]

# Collection for metadata
messages_collection = db["messages"]

# # GridFS for media
# fs = gridfs.GridFS(db)
