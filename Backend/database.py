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
usersEnvironmentOnboarding_collection = db["environmentOnboarding"]
users_collection = db["users"]
resources_collection = db["newResourceDb"]
standard_config_collection = db["standardConfigsDb"]  # <-- Corrected name
users_signup_collection = db["signupdata"]