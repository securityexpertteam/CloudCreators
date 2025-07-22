# # database.py
# from pymongo import MongoClient
# import os
# from dotenv import load_dotenv

# load_dotenv()

# MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
# client = MongoClient(MONGO_URI)
# db = client["myDB"]
# users_collection = db["users"]


# # backend/database.py

# from pymongo import MongoClient

# def get_db():
#     client = MongoClient("mongodb://localhost:27017/")
#     db = client["myDB"]
#     return db


from pymongo import MongoClient
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get MongoDB URI from environment, fallback to localhost
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "myDB")

# Create MongoDB client and database
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# Reusable function to get DB
def get_db():
    return db

# Named collections
users_collection = db["users"]
resources_collection = db["newResourceDb"]
