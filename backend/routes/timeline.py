from fastapi import APIRouter
import pandas as pd

router = APIRouter()

DATA_PATH = "data/processed/model_predictions.csv"

@router.get("/timeline/{lake_id}")
def get_timeline(lake_id: str):
    df = pd.read_csv(DATA_PATH)

    # filter correct lake (based on one-hot encoding)
    col_name = f"lake_id_{lake_id}"

    if col_name not in df.columns:
        return {"error": "Invalid lake_id"}

    df = df[df[col_name] == True]

    result = df[["date", "lake_area_km2", "predicted_risk"]]

    return result.to_dict(orient="records")