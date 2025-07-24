# routes/api_routes.py
from fastapi import APIRouter, HTTPException
from models import User, Resource, StandardConfig,SignupUser
from database import users_collection, get_db,users_signup_collection
from fastapi.encoders import jsonable_encoder
from typing import List

router = APIRouter()

# === USER SIGNUP ===

@router.post("/signin")
def signin(user: SignupUser):
    # Check if email and password match any user
    existing_user = users_signup_collection.find_one({
        "email": user.email,
        "password": user.password
    })

    if not existing_user:
        raise HTTPException(status_code=401, detail="Invalid email or password")

    return {"message": "Login successful"}
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


#         ====User Onboarding routes============
#below route accept duplicates

# @router.post("/bulk_signup")
# def bulk_signup(users: List[User]):
#     inserted_users = []
#     for user in users:
#         # Check for unique combination of project, username, and password
#         existing = users_collection.find_one({
#             "project": user.project,
#             "username": user.username,
#             "password": user.password
#         })
#         if existing:
#             continue  # Skip duplicates

#         users_collection.insert_one(user.dict())
#         inserted_users.append(user.dict())

#     if not inserted_users:
#         raise HTTPException(status_code=400, detail="All entries are duplicates")

#     return {"message": f"{len(inserted_users)} users added successfully", "data": inserted_users}


#below code doesn't accept duplicates

@router.post("/bulk_signup")
def bulk_signup(users: List[User]):
    inserted_users = []

    for user in users:
        # Check if username OR password OR project already exists
        existing = users_collection.find_one({
            "$or": [
                {"username": user.username},
                {"password": user.password},
                {"project": user.project}
            ]
        })
        if existing:
            continue  # Skip if any of the fields already exist

        users_collection.insert_one(user.dict())
        inserted_users.append(user.dict())

    if not inserted_users:
        raise HTTPException(status_code=400, detail="Entries are duplicates")

    return {
        "message": f"{len(inserted_users)} users added successfully",
        "data": inserted_users
    }
