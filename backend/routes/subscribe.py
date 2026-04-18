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
    phone: str = Field(..., pattern=r"^\+\d{10,15}$")
    lake_id: str


# -------------------------------
# ROUTE
# -------------------------------
@router.post("/subscribe")
def subscribe(data: SubscribeRequest):

    # 🔥 OTP CHECK
    if data.phone not in verified_numbers:
        raise HTTPException(status_code=403, detail="Phone not verified")

    try:
        # 🔥 DUPLICATE CHECK
        existing = subscribers_collection.find_one({
            "phone": data.phone,
            "lake_id": data.lake_id
        })

        if existing:
            return {"message": "Already subscribed"}

        # 🔥 INSERT
        subscribers_collection.insert_one({
            "name": data.name,
            "phone": data.phone,
            "lake_id": data.lake_id,
            "created_at": datetime.utcnow(),
            "last_alert_sent": None
        })

        return {"message": "Subscribed successfully"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")