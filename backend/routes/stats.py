from fastapi import APIRouter
from backend.services.data_loader import load_data

router = APIRouter()

@router.get("/stats")
def stats():
    df = load_data()

    latest = df.sort_values("date").groupby("lake_id").tail(1)

    return {
        "total_lakes": int(len(latest)),
        "high_risk": int((latest["final_risk"] == "High").sum()),
        "medium_risk": int((latest["final_risk"] == "Medium").sum()),
        "low_risk": int((latest["final_risk"] == "Low").sum()),
        "avg_area": float(round(latest["lake_area_km2"].mean(), 2))
    }