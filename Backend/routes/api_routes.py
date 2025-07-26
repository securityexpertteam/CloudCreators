from fastapi import APIRouter, HTTPException, Query,Request
from models import User,BulkSignupRequest, Resource, StandardConfig, SignupUser
from database import users_collection,usersEnvironmentOnboarding_collection, get_db, users_signup_collection
from fastapi.encoders import jsonable_encoder
from typing import List, Optional
import bcrypt

import os
import base64

router = APIRouter()

# === USER SIGNUP ===
@router.post("/signin")
def signin(user: SignupUser):
    existing_user = users_signup_collection.find_one({
        "email": user.email,
        "password": user.password
    })

    if not existing_user:
        raise HTTPException(status_code=401, detail="Invalid email or password")

    user_data = {
        "id": str(existing_user.get("_id")),
        "firstname": existing_user.get("firstname"),
        "lastname": existing_user.get("lastname"),
        "email": existing_user.get("email")
    }

    return {
        "message": "Login successful",
        "user": user_data
    }

# === POST STANDARD CONFIG ===
@router.post("/api/configs")
def submit_config(config: StandardConfig):
    db = get_db()
    collection = db["standardConfigsDb"]
    data = jsonable_encoder(config)
    # Insert a new config for this type and email
    result = collection.insert_one(data)
    saved = collection.find_one({"_id": result.inserted_id})
    saved["_id"] = str(saved["_id"])
    return saved

# === GET LATEST CONFIG FOR SUMMARY (per type and user email) ===
@router.get("/api/config/latest")
def get_latest_config(
    type: str = Query(..., description="Config type"),
    email: str = Query(..., description="User email")
):
    db = get_db()
    collection = db["standardConfigsDb"]
    latest = list(
        collection.find({"type": type, "email": email})
        .sort("_id", -1)
        .limit(1)
    )
    if not latest:
        raise HTTPException(status_code=404, detail="No config found")
    config = latest[0]
    config["_id"] = str(config["_id"])
    return config

# ====User Onboarding routes============
# @router.post("/environment_onboarding")
# def bulk_signup(request: BulkSignupRequest):
#     email = request.email
#     users = request.users 
#     inserted_users = []

#     for user in users:
#         # Check for duplicates based on cloudName, environment, rootId, managementUnitId
#         existing = usersEnvironmentOnboarding_collection.find_one({
#             "cloudName": user.cloudName,
#             "environment": user.environment,
#             "rootId": user.rootId,
#             "managementUnitId": user.managementUnitId
#         })
#         if existing:
#             continue

#         user_dict = user.dict()
#         user_dict["email"] = email
#         # Hash the service account password before storing
#         user_dict["srvacctPass"] = bcrypt.hashpw(user_dict["srvacctPass"].encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
#         result = usersEnvironmentOnboarding_collection.insert_one(user_dict)
#         user_dict["_id"] = str(result.inserted_id)
#         inserted_users.append(user_dict)

#     if not inserted_users:
#         raise HTTPException(status_code=400, detail="Entries are duplicates")

#     for user in inserted_users:
#         user.pop("_id", None)

#     return {
#         "message": f"{len(inserted_users)} users added to Environment successfully",
#         "data": inserted_users
#     }

@router.post("/environment_onboarding")
async def environment_onboarding(request: Request):
    data = await request.json()
    email = data.get("email")
    users = data.get("users", [])
    cred_folder = os.path.join(os.getcwd(), "Creds")
    os.makedirs(cred_folder, exist_ok=True)

    inserted_users = []

    for user in users:
        # 1. Save GCP JSON file if provided
        if user.get("cloudName") == "GCP" and user.get("gcpJsonFile"):
            try:
                base64_content = user["gcpJsonFile"].split(",")[1]
                json_bytes = base64.b64decode(base64_content)
                filename = f"{user['cloudName']}_{user['environment']}_{user['rootId']}.json"
                filepath = os.path.join(cred_folder, filename)
                with open(filepath, "wb") as f:
                    f.write(json_bytes)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Error saving GCP file: {str(e)}")

        # 2. Check for duplicates
        existing = usersEnvironmentOnboarding_collection.find_one({
            "cloudName": user["cloudName"],
            "environment": user["environment"],
            "rootId": user["rootId"],
            "managementUnitId": user["managementUnitId"]
        })
        if existing:
            continue

        # 3. Hash the password and insert into DB
        user_dict = dict(user)
        user_dict["email"] = email
        user_dict["srvacctPass"] = bcrypt.hashpw(
            user_dict["srvacctPass"].encode("utf-8"), bcrypt.gensalt()
        ).decode("utf-8")

        result = usersEnvironmentOnboarding_collection.insert_one(user_dict)
        user_dict["_id"] = str(result.inserted_id)
        inserted_users.append(user_dict)

    if not inserted_users:
        raise HTTPException(status_code=400, detail="All entries are duplicates or failed")

    for user in inserted_users:
        user.pop("_id", None)

    return {
        "message": f"{len(inserted_users)} users added to Environment successfully",
        "data": inserted_users
    
    }

    
@router.get("/environments/{email}")
def get_environments(email: str):
    entries = list(usersEnvironmentOnboarding_collection.find({"email": email}))
    for entry in entries:
        entry["_id"] = str(entry["_id"])
    return {"data": entries}    