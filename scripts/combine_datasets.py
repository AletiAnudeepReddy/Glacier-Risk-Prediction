import pandas as pd

# -------------------------------
# LOAD ALL LAKE FILES (WITH CLIMATE)
# -------------------------------
df_L1 = pd.read_csv("data/with_temperature/L1_with_climate.csv")
df_L2 = pd.read_csv("data/with_temperature/L2_with_climate.csv")
df_L3 = pd.read_csv("data/with_temperature/L3_with_climate.csv")
df_L4 = pd.read_csv("data/with_temperature/L4_with_climate.csv")

# -------------------------------
# COMBINE ALL LAKES
# -------------------------------
df = pd.concat([df_L1, df_L2, df_L3, df_L4], ignore_index=True)

# -------------------------------
# BASIC CLEANING
# -------------------------------
df["date"] = pd.to_datetime(df["date"])

# Sort (CRITICAL for time-series ML)
df = df.sort_values(["lake_id", "date"])

# Reset index
df = df.reset_index(drop=True)

# -------------------------------
# FINAL CHECK (DON'T SKIP THIS)
# -------------------------------
print(df.head())
print(df.columns)
print(df.isnull().sum())

# -------------------------------
# SAVE FINAL DATASET
# -------------------------------
df.to_csv("data/processed/final_combined_dataset.csv", index=False)

print("✅ FINAL dataset saved at: data/processed/combined_lake_data.csv")