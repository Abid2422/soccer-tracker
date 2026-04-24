from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

client = MongoClient(os.getenv('MONGODB_URI'))
db = client.soccer_tracker

# Test write
db.test.insert_one({"message": "Hello World"})

# Test read
doc = db.test.find_one()
print(f"✅ MongoDB connected: {doc}")