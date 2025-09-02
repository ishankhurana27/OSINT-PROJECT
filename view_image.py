from pymongo import MongoClient
import gridfs
from bson import ObjectId
import matplotlib.pyplot as plt
from PIL import Image
import io

# --- 1. Connect to MongoDB ---
client = MongoClient("mongodb://localhost:27017/")
db = client["telegram_db"]
fs = gridfs.GridFS(db)

# --- 2. Pick a file to view ---
# Option A: get the first file in GridFS
file_doc = db.fs.files.find_one()
if not file_doc:
    raise Exception("No files found in GridFS")

file_id = file_doc["_id"]

# --- 3. Load file from GridFS ---
file_data = fs.get(file_id).read()
image = Image.open(io.BytesIO(file_data))

# --- 4. Display the image ---
plt.imshow(image)
plt.axis("off")
plt.show()
