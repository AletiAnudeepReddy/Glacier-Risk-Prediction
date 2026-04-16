from fastapi import APIRouter
import pandas as pd
from sklearn.metrics import confusion_matrix

router = APIRouter()

DATA_PATH = "data/processed/model_predictions.csv"

@router.get("/confusion")
def get_confusion():
    df = pd.read_csv(DATA_PATH)

    labels = ["Low", "Medium", "High"]

    cm = confusion_matrix(
        df["risk_level"],
        df["predicted_risk"],
        labels=labels
    )

    return {
        "labels": labels,
        "matrix": cm.tolist()
    }