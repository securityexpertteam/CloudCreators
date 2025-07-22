# routes/user_routes.py
from fastapi import APIRouter, HTTPException
from models import User
from database import users_collection

router = APIRouter()

@router.post("/signup")
def signup(user: User):
    existing_user = users_collection.find_one({"username": user.username})
    if existing_user:
        raise HTTPException(status_code=400, detail="Username already exists")
    
    users_collection.insert_one(user.dict())
    return {"message": "Signup successful"}
