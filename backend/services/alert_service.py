import os
from dotenv import load_dotenv
from twilio.rest import Client

# Load env file
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_NUMBER = os.getenv("TWILIO_PHONE_NUMBER")

# Validate (important)
if not ACCOUNT_SID or not AUTH_TOKEN or not TWILIO_NUMBER:
    raise ValueError("❌ Twilio credentials missing in .env")

client = Client(ACCOUNT_SID, AUTH_TOKEN)

def send_sms(phone, message):
    client.messages.create(
        body=message,
        from_=TWILIO_NUMBER,
        to=phone
    )