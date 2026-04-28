import pandas as pd

# -------------------------------
# LOAD EXISTING FINAL DATASET (L1–L4)
# -------------------------------
df_main = pd.read_csv("data/processed/final_labeled_dataset.csv")

# -------------------------------
# LOAD L5 LABELED DATASET
# -------------------------------
df_l5 = pd.read_csv("data/processed/L5_labeled_dataset.csv")

# -------------------------------
# CONCAT (IMPORTANT)
# -------------------------------
df_final = pd.concat([df_main, df_l5], ignore_index=True)

# -------------------------------
# SORT (CLEAN)
# -------------------------------
df_final["date"] = pd.to_datetime(df_final["date"])
df_final = df_final.sort_values(["lake_id", "date"])

# -------------------------------
# SAVE BACK
# -------------------------------
output_path = "data/processed/final_labeled_dataset.csv"
df_final.to_csv(output_path, index=False)

print(f"🔥 L5 added successfully → {output_path}")