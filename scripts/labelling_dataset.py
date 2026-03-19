import pandas as pd
import numpy as np

# -------------------------------
# LOAD DATA
# -------------------------------
df = pd.read_csv("data/processed/lake_features.csv")

df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["lake_id", "date"])

# -------------------------------
# ROBUST SCALING FUNCTION
# -------------------------------
def robust_scale(series):
    median = series.median()
    iqr = series.quantile(0.75) - series.quantile(0.25)
    
    if iqr == 0:
        return pd.Series(0, index=series.index)
    
    return (series - median) / iqr

# -------------------------------
# APPLY PER LAKE (IMPORTANT)
# -------------------------------
scaled_dfs = []

for lake_id, group in df.groupby("lake_id"):

    group = group.copy()

    # -------------------------------
    # ROBUST NORMALIZATION
    # -------------------------------
    group["growth_scaled"] = robust_scale(group["growth_rate"])
    group["accel_scaled"] = robust_scale(group["growth_acceleration"])
    group["precip_scaled"] = robust_scale(group["precipitation"])
    group["temp_scaled"] = robust_scale(group["temperature"])

    # -------------------------------
    # EXTREME EVENT SCORE (Z-SCORE STYLE)
    # -------------------------------
    group["growth_z"] = (
        (group["growth_rate"] - group["growth_rate"].mean()) /
        (group["growth_rate"].std() + 1e-6)
    )

    group["precip_z"] = (
        (group["precipitation"] - group["precipitation"].mean()) /
        (group["precipitation"].std() + 1e-6)
    )

    # -------------------------------
    # COMPOSITE RISK SCORE
    # -------------------------------
    group["risk_score"] = (
        0.35 * np.maximum(group["growth_scaled"], 0) +   # expansion
        0.25 * np.maximum(group["accel_scaled"], 0) +    # instability
        0.25 * np.maximum(group["precip_scaled"], 0) +   # rainfall
        0.15 * np.maximum(group["temp_scaled"], 0)       # melting
    )

    # -------------------------------
    # BOOST EXTREME EVENTS
    # -------------------------------
    extreme_boost = (
        (group["growth_z"] > 2).astype(int) +
        (group["precip_z"] > 2).astype(int)
    )

    group["risk_score"] = group["risk_score"] + 0.5 * extreme_boost

    # -------------------------------
    # SMOOTHING (avoid spikes)
    # -------------------------------
    group["risk_score"] = group["risk_score"].rolling(3, min_periods=1).mean()

    # -------------------------------
    # DYNAMIC LABELING (PER LAKE)
    # -------------------------------
    low = group["risk_score"].quantile(0.6)
    high = group["risk_score"].quantile(0.85)

    def assign_label(x):
        if x >= high:
            return "High"
        elif x >= low:
            return "Medium"
        else:
            return "Low"

    group["risk_level"] = group["risk_score"].apply(assign_label)

    scaled_dfs.append(group)

# -------------------------------
# FINAL DATASET
# -------------------------------
df = pd.concat(scaled_dfs, ignore_index=True)

# -------------------------------
# SAVE
# -------------------------------
df.to_csv("data/processed/final_labeled_dataset.csv", index=False)

print("🔥 Research-level labeling completed!")