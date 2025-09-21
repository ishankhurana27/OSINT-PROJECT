# import pymongo
# import gridfs
# import os
# from dotenv import load_dotenv

# load_dotenv()

# MONGO_URI = os.getenv("MONGO_URI")

# client = pymongo.MongoClient(MONGO_URI)
# db = client["telegram_db"]

# # Collection for metadata
# messages_collection = db["messages"]

# # # GridFS for media
# # fs = gridfs.GridFS(db)


import pymongo
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("MONGO_DB_NAME", "osint_db")

client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]

# Telegram collections
messages_collection = db["messages"]

# Twitter collections
tweets_collection = db["tweets"]

# (Optional) if you want to store users separately
users_collection = db["users"]
