from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=5000)
db = client["glacier_alerts"]

subscribers_collection = db["subscribers"]