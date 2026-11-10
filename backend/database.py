import os
from pymongo import MongoClient

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://f1_app_user:f1banjutjaya@localhost:27017/f1_analytics?authSource=f1_analytics"
)
DB_NAME = "f1_analytics"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

def get_database():
    return db