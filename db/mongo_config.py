# # import pymongo
# # import gridfs
# # import os
# # from dotenv import load_dotenv

# # load_dotenv()

# # MONGO_URI = os.getenv("MONGO_URI")

# # client = pymongo.MongoClient(MONGO_URI)
# # db = client["telegram_db"]

# # # Collection for metadata
# # messages_collection = db["messages"]

# # # # GridFS for media
# # # fs = gridfs.GridFS(db)


# import pymongo
# import os
# from dotenv import load_dotenv

# load_dotenv()

# MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
# DB_NAME = os.getenv("MONGO_DB_NAME", "osint_db")

# client = pymongo.MongoClient(MONGO_URI)
# db = client[DB_NAME]

# # Telegram collections
# messages_collection = db["messages"]

# # Twitter collections
# tweets_collection = db["tweets"]

# # (Optional) store users separately
# users_collection = db["users"]

# # Substack collections
# substack_collection = db["substack_posts"]


# db/mongo_config.py

import pymongo
import os
from dotenv import load_dotenv
from helpers import make_hash

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("MONGO_DB_NAME", "osint_db")

client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]

# Collections
messages_collection = db["messages"]
tweets_collection = db["tweets"]
users_collection = db["users"]
substack_collection = db["substack_posts"]

class MongoStore:
    def __init__(self, collection=substack_collection):
        self.collection = collection

    def insert_post(self, post: dict):
        """Insert Substack post if not duplicate."""
        post_id = make_hash(post.get("link", ""))  # use link as unique ID
        if not self.collection.find_one({"_id": post_id}):
            post["_id"] = post_id
            self.collection.insert_one(post)
            return True
        return False
