from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from backend.services.db import subscribers_collection
from datetime import datetime
from backend.routes.otp import verified_numbers

router = APIRouter()

# -------------------------------
# MODEL
# -------------------------------
class SubscribeRequest(BaseModel):
    name: str = Field(..., min_length=2)
    phone: str = Field(..., regex=r"^\+\d{10,15}$")
    lake_id: str


# -------------------------------
# ROUTE
# -------------------------------
@router.post("/subscribe")
def subscribe(data: SubscribeRequest):

    # 🔥 ENFORCE OTP
    if data.phone not in verified_numbers:
        raise HTTPException(status_code=403, detail="Phone not verified")

    # Prevent duplicate
    if subscribers_collection.find_one({
        "phone": data.phone,
        "lake_id": data.lake_id
    }):
        return {"message": "Already subscribed"}

    # Insert
    subscribers_collection.insert_one({
        "name": data.name,
        "phone": data.phone,
        "lake_id": data.lake_id,
        "created_at": datetime.utcnow(),
        "last_alert_sent": None   # 🔥 IMPORTANT
    })

    return {"message": "Subscribed successfully"}