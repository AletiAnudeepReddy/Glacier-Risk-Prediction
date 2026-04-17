from fastapi import APIRouter
import json
from backend.services.data_loader import load_data

router = APIRouter()

@router.get("/map")
def map_data():
    df = load_data()

    latest = df.sort_values("date").groupby("lake_id").tail(1)

    with open("config/lakes.json") as f:
        lakes = json.load(f)

    result = []

    for lake in lakes:
        lid = lake["lake_id"]
        row = latest[latest["lake_id"] == lid]

        if not row.empty:
            result.append({
                "lake_id": lid,
                "lat": (lake["bbox"][1] + lake["bbox"][3]) / 2,
                "lon": (lake["bbox"][0] + lake["bbox"][2]) / 2,
                "risk": row.iloc[0]["final_risk"]
            })

    return result