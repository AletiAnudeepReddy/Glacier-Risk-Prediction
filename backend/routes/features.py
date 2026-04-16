from fastapi import APIRouter

router = APIRouter()

@router.get("/features")
def get_features():
    return [
        {"feature": "temp_lag_1", "importance": 0.148},
        {"feature": "growth_rate", "importance": 0.075},
        {"feature": "area_rolling_std_3", "importance": 0.073},
        {"feature": "area_change", "importance": 0.068},
        {"feature": "growth_acceleration", "importance": 0.066},
        {"feature": "month", "importance": 0.063},
        {"feature": "precip_rolling_3", "importance": 0.061}
    ]