from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.routes.timeline import router as timeline_router
from backend.routes.confusion import router as confusion_router
from backend.routes.features import router as features_router

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routes
app.include_router(timeline_router)
app.include_router(confusion_router)
app.include_router(features_router)

@app.get("/")
def home():
    return {"message": "Glacier Risk API running"}