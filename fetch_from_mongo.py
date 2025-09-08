# import os
# from bson import ObjectId
# from db.mongo_config import messages_collection, fs

# def fetch_message(message_id):
#     """Fetch a message by its Telegram message_id"""
#     doc = messages_collection.find_one({"message_id": message_id})
#     if not doc:
#         print(f"No message found with id {message_id}")
#         return

#     print("Message Info:")
#     print(f"Text: {doc.get('text')}")
#     print(f"Date: {doc.get('date')}")
#     print(f"Sender ID: {doc.get('sender_id')}")

#     # If media exists, fetch from GridFS
#     if "media_id" in doc:
#         media_id = doc["media_id"]
#         file_data = fs.get(ObjectId(media_id)).read()

#         os.makedirs("retrieved_media", exist_ok=True)
#         file_path = os.path.join("retrieved_media", f"{doc['message_id']}.bin")

#         with open(file_path, "wb") as f:
#             f.write(file_data)

#         print(f"Media saved to {file_path}")
#     else:
#         print("No media attached to this message.")

# if __name__ == "__main__":
#     # Change message_id to one that exists in your DB
#     fetch_message(message_id=12345)
