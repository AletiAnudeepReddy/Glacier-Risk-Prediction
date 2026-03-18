import pandas as pd

# -------------------------------
# LOAD ALL LAKE FILES
# -------------------------------
df_L1 = pd.read_csv("data/raw/L1_imja_tsho_clean.csv")
df_L2 = pd.read_csv("data/raw/L2_tsho_rolpa_clean.csv")
df_L3 = pd.read_csv("data/raw/L3_lower_barun_clean.csv")
df_L4 = pd.read_csv("data/raw/L4_lumding_clean.csv")

# -------------------------------
# COMBINE
# -------------------------------
df = pd.concat([df_L1, df_L2, df_L3, df_L4], ignore_index=True)

# -------------------------------
# BASIC CLEANING
# -------------------------------
df["date"] = pd.to_datetime(df["date"])

# Sort properly (VERY IMPORTANT)
df = df.sort_values(["lake_id", "date"])

# Reset index
df = df.reset_index(drop=True)

# -------------------------------
# SAVE COMBINED DATA
# -------------------------------
df.to_csv("data/processed/combined_lake_data.csv", index=False)

print("✅ Combined dataset saved at: data/processed/combined_lake_data.csv")