from fastapi import APIRouter
from backend.services.data_loader import load_data

router = APIRouter()

@router.get("/area/{lake_id}")
def area(lake_id: str):
    df = load_data()
    df = df[df["lake_id"] == lake_id]
    return df[["date", "lake_area_km2"]].to_dict(orient="records")


@router.get("/temperature/{lake_id}")
def temp(lake_id: str):
    df = load_data()
    df = df[df["lake_id"] == lake_id]
    return df[["date", "temperature"]].to_dict(orient="records")


@router.get("/precipitation/{lake_id}")
def precip(lake_id: str):
    df = load_data()
    df = df[df["lake_id"] == lake_id]
    return df[["date", "precipitation"]].to_dict(orient="records")


@router.get("/growth/{lake_id}")
def growth(lake_id: str):
    df = load_data()
    df = df[df["lake_id"] == lake_id]
    return df[["date", "growth_rate"]].to_dict(orient="records")