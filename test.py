# from pymongo import MongoClient
# from dotenv import load_dotenv
# import os

# load_dotenv()

# client = MongoClient(os.getenv('MONGODB_URI'))
# db = client.soccer_tracker

# # Test write
# db.test.insert_one({"message": "Hello World"})

# # Test read
# doc = db.test.find_one()
# print(f"✅ MongoDB connected: {doc}")

# from coordinator import Coordinator

# coord = Coordinator()

# # Check all consumers
# consumers = coord.get_all_consumers()
# for c in consumers:
#     print(f"Consumer {c['consumer_id']}: {c['teams']}")
    
# # Check heartbeats
# for i in range(3):
#     alive = coord.is_alive(i)
#     print(f"Consumer {i} alive: {alive}")