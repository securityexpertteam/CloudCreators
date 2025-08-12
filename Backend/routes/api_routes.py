from fastapi import APIRouter, HTTPException, Query,Request,UploadFile, File, Form
from models import User,BulkSignupRequest, Resource, StandardConfig, SignupUser , Trigger,SigninUser
from database import users_collection,usersEnvironmentOnboarding_collection, get_db, users_signup_collection, resources_collection , triggers_collection
from fastapi.encoders import jsonable_encoder
from typing import List, Optional
from datetime import datetime, timezone
from cryptography.fernet import Fernet
import bcrypt
from bson import ObjectId
import os
import base64
from dateutil import parser
from fastapi.responses import JSONResponse
from typing import List
import shutil
import json
from fastapi import HTTPException

router = APIRouter()

from database import get_db

# === USER SIGNUP ===
# === GET ALL RESOURCES FOR DASHBOARD ===
@router.get("/api/resources")
def get_resources(email: str = None):
    db = get_db()
    resources_collection = db["Cost_Insights"]
    query = {}

    if email:
        query["Email"] = email

    resources = list(resources_collection.find(query))
    for r in resources:
        r["_id"] = str(r["_id"])
    return resources





@router.post("/signup")
def signup(user: SignupUser):
    # Check if user exists
    if users_signup_collection.find_one({"email": user.email}):
        raise HTTPException(status_code=400, detail="Email already registered")
    # Hash password
    hashed_pw = bcrypt.hashpw(user.password.encode("utf-8"), bcrypt.gensalt())
    user_dict = user.dict()
    user_dict["password"] = hashed_pw.decode("utf-8")
    users_signup_collection.insert_one(user_dict)
    return {"message": "Signup successful"}
    

@router.post("/signin")
def signin(user: SigninUser):
    existing_user = users_signup_collection.find_one({"email": user.email})
    if not existing_user:
        raise HTTPException(status_code=401, detail="Invalid email or password")
    # Check password
    if not bcrypt.checkpw(user.password.encode("utf-8"), existing_user["password"].encode("utf-8")):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    user_data = {
        "id": str(existing_user.get("_id")),
        "firstname": existing_user.get("firstname"),
        "lastname": existing_user.get("lastname"),
        "email": existing_user.get("email")
    }
    return {"message": "Login successful", "user": user_data}    

# === POST STANDARD CONFIG ===
@router.post("/api/configs")
def submit_config(config: dict):
    db = get_db()
    collection = db["standardConfigsDb"]
    email = config.get("email")
    if not email:
        raise HTTPException(status_code=400, detail="Email is required")
    # Upsert: update if exists, insert if not
    result = collection.update_one(
        {"email": email},
        {"$set": config},
        upsert=True
    )
    saved = collection.find_one({"email": email})
    saved["_id"] = str(saved["_id"])
    return saved

# === GET LATEST CONFIG FOR SUMMARY (per type and user email) ===

@router.get("/api/config/latest")
def get_latest_config(
    email: str = Query(..., description="User email"),
    type: Optional[str] = Query(None, description="Config type (optional)")
):
    db = get_db()
    collection = db["standardConfigsDb"]
    query = {"email": email}
    if type:
        query["type"] = type
    latest = list(
        collection.find(query)
        .sort("_id", -1)
        .limit(1)
    )
    if not latest:
        raise HTTPException(status_code=404, detail="No config found")
    config = latest[0]
    config["_id"] = str(config["_id"])
    return config

# ====Environment Onboarding routes============

@router.post("/environment_onboarding")
async def environment_onboarding(
    request: Request,
    email: str = Form(...),
    users: str = Form(...),
    files: List[UploadFile] = File(None)
):
    

    users = json.loads(users)

    # ✅ Correct path to Creds folder
    cred_folder = os.path.join(os.path.dirname(__file__), "../Creds")
    cred_folder = os.path.abspath(cred_folder)
    os.makedirs(cred_folder, exist_ok=True)

    inserted_environments = []
    file_map = {f.filename: f for f in files or []}
    
    for idx, user in enumerate(users):
        existing = usersEnvironmentOnboarding_collection.find_one({
        "cloudName": user["cloudName"],
        "environment": user["environment"],
        "rootId": user["rootId"],
        "managementUnitId": user["managementUnitId"]
    })
        if existing:
            continue

    user_dict = dict(user)
    user_dict["email"] = email

    # ✅ If GCP, remove vault-related fields before saving
    if user_dict["cloudName"] == "GCP":
        user_dict.pop("vaultname", None)
        user_dict.pop("srvaccntName", None)
        user_dict.pop("srvacctPass", None)
    else:
        # Encrypt credentials only if not GCP
        fernet = Fernet(os.environ["fernet_key"])
        if user.get("srvacctPass"):
            user_dict["srvacctPass"] = fernet.encrypt(user["srvacctPass"].encode()).decode()
        if user.get("srvaccntName"):
            user_dict["srvaccntName"] = fernet.encrypt(user["srvaccntName"].encode()).decode()

    # Insert into MongoDB
    result = usersEnvironmentOnboarding_collection.insert_one(user_dict)
    user_dict["_id"] = str(result.inserted_id)
    inserted_environments.append(user_dict)

    # Save GCP JSON file if applicable
    if user["cloudName"] == "GCP":
        saved = False
        for file in files or []:
            if file.filename.endswith(".json"):
                dest_path = os.path.join(cred_folder, f"{user['managementUnitId']}_{user['secretname']}.json")
                print(f"Saving file to: {dest_path}")
                with open(dest_path, "wb") as f:
                    shutil.copyfileobj(file.file, f)
                saved = True
                break
        if not saved:
            print("No GCP JSON file found to save.")


    if not inserted_environments:
        raise HTTPException(status_code=400, detail="All entries are duplicates or failed")

    for user in inserted_environments:
        user.pop("_id", None)

    return {
        "message": f"{len(inserted_environments)} Environment added successfully",
        "data": inserted_environments
    }



# Environment Fetch by Login Email    
@router.get("/environments/{email}")
def get_environments(email: str):
    entries = list(usersEnvironmentOnboarding_collection.find({"email": email}))
    for entry in entries:
        entry["_id"] = str(entry["_id"])
    return {"data": entries}  


@router.delete("/delete_environment/{env_id}")
def delete_environment(env_id: str):
    result = usersEnvironmentOnboarding_collection.delete_one({"_id": ObjectId(env_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Environment not found")
    return {"message": "Environment deleted successfully"}

# # === TRIGGER SCHEDULE === 
# @router.post("/triggers")
# async def create_trigger(trigger: Trigger):
#     # Find the latest trigger for this email
#     latest = triggers_collection.find_one({"email": trigger.email}, sort=[("_id", -1)])
#     if latest and latest.get("Status") in ("Pending", "InProgress"):
#         raise HTTPException(status_code=400, detail="Already scheduled or running for this user.")

#     data = {
#         "email": trigger.email,
#         "SystemTimeStamp": datetime.now(timezone.utc),
#         "ScheduledTimeStamp": trigger.ScheduledTimeStamp,
#         "Status": "Pending"
#     }
#     result = triggers_collection.insert_one(data)
#     data["_id"] = str(result.inserted_id)
#     return {"message": "Schedule saved", "data": data}

# @router.get("/trigger_status/{email}")
# def get_trigger_status(email: str):
#     trigger = triggers_collection.find_one({"email": email}, sort=[("_id", -1)])
#     if not trigger:
#         raise HTTPException(status_code=404, detail="No trigger found")
#     return {"status": trigger["Status"]}

# # --- ADD THIS ENDPOINT ---
# @router.get("/last_scan/{email}")
# def get_last_scan(email: str):
#     trigger = triggers_collection.find_one({"email": email}, sort=[("_id", -1)])
#     if not trigger:
#         return {"status": None, "scheduled_time": None}
#     # Convert datetime to ISO string in UTC for frontend
#     scheduled_time = None
#     if trigger.get("ScheduledTimeStamp"):
#         scheduled_time = trigger["ScheduledTimeStamp"].astimezone(timezone.utc).isoformat()
#     return {
#         "status": trigger.get("Status"),
#         "scheduled_time": scheduled_time
        
        
#     }
    
# @router.get("/triggers/latest")
# async def get_latest_trigger(email: str):
#     latest = triggers_collection.find_one(
#         {"email": email},
#         sort=[("SystemTimeStamp", -1)]
#     )
#     if latest:
#         latest["_id"] = str(latest["_id"])
#     return latest
   

@router.post("/triggers")
async def create_trigger(trigger: Trigger):
    # Delete all previous triggers for this user
    triggers_collection.delete_many({"email": trigger.email})
    # Find the latest trigger for this email
    latest = triggers_collection.find_one({"email": trigger.email}, sort=[("_id", -1)])
    if latest and latest.get("Status") in ("Pending", "InProgress"):
        raise HTTPException(status_code=400, detail="Already scheduled or running for this user.")

    st = trigger.ScheduledTimeStamp
    if isinstance(st, str):
        st = parser.isoparse(st)
    if st.tzinfo is None:
        st = st.replace(tzinfo=timezone.utc)
    else:
        st = st.astimezone(timezone.utc)

    data = {
        "email": trigger.email,
        "SystemTimeStamp": datetime.now(timezone.utc),
        "ScheduledTimeStamp": trigger.ScheduledTimeStamp,
        "Status": "Pending"
    }
    result = triggers_collection.insert_one(data)
    data["_id"] = str(result.inserted_id)
    return {"message": "Schedule saved", "data": data}

@router.get("/trigger_status/{email}")
def get_trigger_status(email: str):
    trigger = triggers_collection.find_one({"email": email}, sort=[("_id", -1)])
    if not trigger:
        raise HTTPException(status_code=404, detail="No trigger found")
    return {"status": trigger["Status"]}

# --- ADD THIS ENDPOINT ---
@router.get("/last_scan/{email}")
def get_last_scan(email: str):
    trigger = triggers_collection.find_one({"email": email}, sort=[("_id", -1)])
    if not trigger:
        return {"status": None, "scheduled_time": None}
    # Convert datetime to ISO string in UTC for frontend
    scheduled_time = None
    if trigger.get("ScheduledTimeStamp"):
        scheduled_time = trigger["ScheduledTimeStamp"].astimezone(timezone.utc).isoformat()
    return {
        "status": trigger.get("Status"),
        "scheduled_time": scheduled_time
    }
