from fastapi import APIRouter
from backend.services.data_loader import load_data

router = APIRouter()

@router.get("/alerts")
def alerts():
    df = load_data()

    latest = df.sort_values("date").groupby("lake_id").tail(1)

    alerts = []

    for _, row in latest.iterrows():
        if row["final_risk"] == "High":
            alerts.append({
                "lake_id": row["lake_id"],
                "risk": "High",
                "message": "High risk detected"
            })

    return alerts