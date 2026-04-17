import pandas as pd

DATA_PATH = "data/continuous/final/final_fused_predictions.csv"

def load_data():
    df = pd.read_csv(DATA_PATH)
    df["date"] = pd.to_datetime(df["date"])
    return df