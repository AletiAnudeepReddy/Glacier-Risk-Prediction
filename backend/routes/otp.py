from fastapi import APIRouter
from pydantic import BaseModel, Field
import os
from dotenv import load_dotenv
from twilio.rest import Client
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

router = APIRouter()

client = Client(
    os.getenv("TWILIO_ACCOUNT_SID"),
    os.getenv("TWILIO_AUTH_TOKEN")
)

VERIFY_SID = os.getenv("TWILIO_VERIFY_SID")

# -------------------------------
# TEMP VERIFIED STORE
# -------------------------------
verified_numbers = set()

# -------------------------------
# MODELS
# -------------------------------
class PhoneRequest(BaseModel):
    phone: str = Field(..., pattern=r"^\+\d{10,15}$")


class OTPVerifyRequest(BaseModel):
    phone: str = Field(..., pattern=r"^\+\d{10,15}$")
    otp: str


# -------------------------------
# SEND OTP
# -------------------------------
@router.post("/send-otp")
def send_otp(data: PhoneRequest):
    try:
        verification = client.verify.services(VERIFY_SID).verifications.create(
            to=data.phone,
            channel="sms"
        )
        return {"success": True, "status": verification.status}
    except Exception as e:
        return {"success": False, "error": str(e)}


# -------------------------------
# VERIFY OTP
# -------------------------------
@router.post("/verify-otp")
def verify_otp(data: OTPVerifyRequest):
    try:
        check = client.verify.services(VERIFY_SID).verification_checks.create(
            to=data.phone,
            code=data.otp
        )

        if check.status == "approved":
            verified_numbers.add(data.phone)
            return {"success": True}
        else:
            return {"success": False}

    except Exception as e:
        return {"success": False, "error": str(e)}