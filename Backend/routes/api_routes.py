# routes/api_routes.py
from fastapi import APIRouter, HTTPException
from models import User, Resource, StandardConfig
from database import users_collection, get_db
from fastapi.encoders import jsonable_encoder
from typing import List

router = APIRouter()

# === USER SIGNUP ===
@router.post("/signup")
def signup(user: User):
    existing_user = users_collection.find_one({"username": user.username})
    if existing_user:
        raise HTTPException(status_code=400, detail="Username already exists")
    users_collection.insert_one(user.dict())
    return {"message": "Signup successful"}

# === GET CLOUD RESOURCE DATA ===
@router.get("/api/resources", response_model=List[Resource])
def get_resources():
    db = get_db()
    collection = db["newResourceDb"]
    return list(collection.find({}, {"_id": 0}))

# === POST STANDARD CONFIG ===
@router.post("/api/configs")
def submit_config(config: StandardConfig):
    db = get_db()
    collection = db["standardConfigsDb"]
    data = jsonable_encoder(config)
    result = collection.insert_one(data)
    saved = collection.find_one({"_id": result.inserted_id})
    saved["_id"] = str(saved["_id"])
    return saved

# === GET LATEST CONFIG FOR SUMMARY ===
@router.get("/api/config/latest")
def get_latest_config():
    db = get_db()
    collection = db["standardConfigsDb"]
    latest = list(collection.find().sort("_id", -1).limit(1))
    if not latest:
        raise HTTPException(status_code=404, detail="No config found")
    config = latest[0]
    config["_id"] = str(config["_id"])
    return config
