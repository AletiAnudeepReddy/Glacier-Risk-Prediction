import pandas as pd

# -------------------------------
# LOAD DATA
# -------------------------------
rule_df = pd.read_csv("data/continuous/processed/recent_labeled_dataset.csv")
model_df = pd.read_csv("data/continuous/final/recent_predictions.csv")

# Convert date
rule_df["date"] = pd.to_datetime(rule_df["date"])
model_df["date"] = pd.to_datetime(model_df["date"])

# -------------------------------
# MERGE BOTH DATASETS
# -------------------------------
df = pd.merge(
    rule_df,
    model_df[["date", "lake_id", "predicted_risk"]],
    on=["date", "lake_id"],
    how="inner"
)

# -------------------------------
# LABEL MAPPING
# -------------------------------
label_map = {"Low": 0, "Medium": 1, "High": 2}
reverse_map = {0: "Low", 1: "Medium", 2: "High"}

df["rule_num"] = df["risk_level"].map(label_map)
df["model_num"] = df["predicted_risk"].map(label_map)

# -------------------------------
# FUSION (WEIGHTED)
# -------------------------------
df["final_score"] = (
    0.6 * df["rule_num"] +
    0.4 * df["model_num"]
)

# -------------------------------
# FINAL RISK LABEL
# -------------------------------
df["final_risk"] = df["final_score"].round().astype(int).map(reverse_map)

# -------------------------------
# SAVE FINAL DATASET
# -------------------------------
output_path = "data/continuous/final/final_fused_predictions.csv"

df.to_csv(output_path, index=False)

print(f"🔥 FINAL DATASET WITH RISK SAVED → {output_path}")