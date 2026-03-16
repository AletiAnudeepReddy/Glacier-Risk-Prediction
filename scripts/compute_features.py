import pandas as pd

df = pd.read_csv("data/raw/lake_area_timeseries.csv")

df["date"] = pd.to_datetime(df["date"])

df = df.sort_values(["lake_id", "date"])

df["area_previous"] = df.groupby("lake_id")["lake_area_km2"].shift(1)

df["area_change"] = df["lake_area_km2"] - df["area_previous"]

df["growth_rate"] = df["area_change"] / df["area_previous"]

df.to_csv("data/processed/lake_features.csv", index=False)

print("Feature dataset saved: data/processed/lake_features.csv")