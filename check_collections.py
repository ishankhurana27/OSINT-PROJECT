from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
print(client.list_database_names())

for db_name in client.list_database_names():
    db = client[db_name]
    print(f"\nDatabase: {db_name}")
    print("Collections:", db.list_collection_names())