# main.py
from fastapi import FastAPI
from routes.user_routes import router
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Allow frontend (React) to access backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # React default port
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)
