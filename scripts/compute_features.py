import pandas as pd

df = pd.read_csv("data/processed/combined_lake_data.csv")

df["date"] = pd.to_datetime(df["date"])

df = df.sort_values(["lake_id", "date"])

# Previous area
df["area_previous"] = df.groupby("lake_id")["lake_area_km2"].shift(1)

# Change
df["area_change"] = df["lake_area_km2"] - df["area_previous"]

# Growth rate
df["growth_rate"] = df["area_change"] / df["area_previous"]

# Handle division issues
df["growth_rate"] = df["growth_rate"].replace([float("inf"), -float("inf")], 0)

# Fill first rows
df = df.fillna(0)

# -------------------------------
# CREATE LABEL (RISK)
# -------------------------------
df["risk"] = (df["growth_rate"] > 0.3).astype(int)

# -------------------------------
# SAVE
# -------------------------------
df.to_csv("data/processed/lake_features.csv", index=False)

print("✅ Feature dataset ready!")