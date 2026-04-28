import pandas as pd
import numpy as np

# -------------------------------
# LOAD L5 DATA (WITH CLIMATE)
# -------------------------------
df = pd.read_csv("data/additional/L5_with_climate.csv")

df["date"] = pd.to_datetime(df["date"])

# Sort properly
df = df.sort_values(["lake_id", "date"])

# -------------------------------
# TEMPORAL FEATURES (LAGS)
# -------------------------------
for lag in [1, 2, 3]:
    df[f"area_lag_{lag}"] = df.groupby("lake_id")["lake_area_km2"].shift(lag)

# -------------------------------
# CHANGE & GROWTH
# -------------------------------
df["area_change"] = df["lake_area_km2"] - df["area_lag_1"]

df["growth_rate"] = df["area_change"] / df["area_lag_1"]

# Fix division issues
df["growth_rate"] = df["growth_rate"].replace([np.inf, -np.inf], 0)

# -------------------------------
# ACCELERATION
# -------------------------------
df["growth_acceleration"] = df.groupby("lake_id")["growth_rate"].diff()

# -------------------------------
# ROLLING FEATURES (AREA)
# -------------------------------
df["area_rolling_mean_3"] = (
    df.groupby("lake_id")["lake_area_km2"]
    .rolling(3)
    .mean()
    .reset_index(level=0, drop=True)
)

df["area_rolling_std_3"] = (
    df.groupby("lake_id")["lake_area_km2"]
    .rolling(3)
    .std()
    .reset_index(level=0, drop=True)
)

# -------------------------------
# CLIMATE FEATURES
# -------------------------------
df["temp_lag_1"] = df.groupby("lake_id")["temperature"].shift(1)
df["precip_lag_1"] = df.groupby("lake_id")["precipitation"].shift(1)

df["precip_rolling_3"] = (
    df.groupby("lake_id")["precipitation"]
    .rolling(3)
    .mean()
    .reset_index(level=0, drop=True)
)

# -------------------------------
# SEASONAL FEATURES
# -------------------------------
df["month"] = df["date"].dt.month

# -------------------------------
# CLEAN NaNs
# -------------------------------
df = df.fillna(0)

# -------------------------------
# SAVE OUTPUT
# -------------------------------
output_path = "data/additional/L5_features.csv"
df.to_csv(output_path, index=False)

print(f"🔥 L5 FEATURE DATASET READY → {output_path}")