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

# -------------------------------
# GET LATEST STATE PER LAKE
# -------------------------------
latest = df.sort_values("date").groupby("lake_id").tail(1)

# -------------------------------
# SEND ALERTS FOR ALL LEVELS
# -------------------------------
for _, lake in latest.iterrows():
    lake_id = lake["lake_id"]
    risk = lake["final_risk"]

    users = subscribers_collection.find({"lake_id": lake_id})

    for user in users:
        last_sent = user.get("last_alert_sent")

        # 🔥 PREVENT DAILY SPAM
        if last_sent:
            diff = datetime.utcnow() - last_sent
            if diff.days < 1:
                continue

        # -------------------------------
        # MESSAGE BASED ON RISK
        # -------------------------------
        if risk == "High":
            message = f"⚠️ ALERT: {lake_id} is at HIGH RISK. Immediate caution advised!"
        elif risk == "Medium":
            message = f"⚠️ UPDATE: {lake_id} is at MEDIUM RISK. Stay alert."
        else:
            message = f"✅ STATUS: {lake_id} is at LOW RISK. No immediate concern."

        # -------------------------------
        # SEND SMS
        # -------------------------------
        send_sms(user["phone"], message)

        # -------------------------------
        # UPDATE DB
        # -------------------------------
        subscribers_collection.update_one(
            {"_id": user["_id"]},
            {"$set": {"last_alert_sent": datetime.utcnow()}}
        )

        print(f"✅ Sent {risk} alert to {user['phone']}")