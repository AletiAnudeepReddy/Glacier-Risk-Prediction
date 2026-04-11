import pandas as pd
import json

# -------------------------------
# LOAD DATA
# -------------------------------
df = pd.read_csv("data/continuous/weekly_ndwi_final.csv")

# -------------------------------
# LOAD LAKE RANGES
# -------------------------------
with open("config/lakes.json") as f:
    lakes = json.load(f)

# Create map: lake_id → (min, max)
range_map = {
    lake["lake_id"]: lake["area_range"]
    for lake in lakes
}

# -------------------------------
# APPLY CORRECTION
# -------------------------------
def fix_value(row):
    lake_id = row["lake_id"]
    value = row["lake_area_km2"]

    min_val, max_val = range_map[lake_id]
    avg_val = (min_val + max_val) / 2

    if value < min_val:
        return avg_val
    return value

df["lake_area_km2"] = df.apply(fix_value, axis=1)

# -------------------------------
# SAVE
# -------------------------------
output_path = "data/continuous/weekly_ndwi_corrected_simple.csv"
df.to_csv(output_path, index=False)

print(f"🔥 SIMPLE CORRECTION DONE → {output_path}")