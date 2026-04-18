
import pandas as pd
from backend.services.db import subscribers_collection
from backend.services.alert_service import send_sms
from datetime import datetime

# -------------------------------
# LOAD DATA
# -------------------------------
df = pd.read_csv("data/continuous/final/final_fused_predictions.csv")

if df.empty:
    print("❌ No data found")
    exit()

latest = df.sort_values("date").groupby("lake_id").tail(1)

high_risk = latest[latest["final_risk"] == "High"]

if high_risk.empty:
    print("✅ No high-risk lakes today")
    exit()

# -------------------------------
# SEND ALERTS
# -------------------------------
for _, lake in high_risk.iterrows():
    lake_id = lake["lake_id"]

    users = subscribers_collection.find({"lake_id": lake_id})

    for user in users:
        last_sent = user.get("last_alert_sent")

        # 🔥 PREVENT DAILY SPAM
        if last_sent:
            diff = datetime.utcnow() - last_sent
            if diff.days < 1:
                continue

        message = f"⚠️ ALERT: {lake_id} is at HIGH RISK. Stay cautious."

        send_sms(user["phone"], message)

        # Update last sent time
        subscribers_collection.update_one(
            {"_id": user["_id"]},
            {"$set": {"last_alert_sent": datetime.utcnow()}}
        )

        print(f"✅ Sent alert to {user['phone']}")