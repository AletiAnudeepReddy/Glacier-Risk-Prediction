import pandas as pd
import joblib

# -------------------------------
# LOAD MODEL + ENCODER
# -------------------------------
model = joblib.load("models/xgboost_model.pkl")
le = joblib.load("models/label_encoder.pkl")

# -------------------------------
# LOAD RECENT DATA
# -------------------------------
df = pd.read_csv("data/continuous/processed/recent_lake_features.csv")

df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["lake_id", "date"])

# -------------------------------
# ONE-HOT ENCODE lake_id (SAME AS TRAINING)
# -------------------------------
df = pd.get_dummies(df, columns=["lake_id"])

# -------------------------------
# LOAD TRAIN FEATURE COLUMNS
# -------------------------------
train_df = pd.read_csv("data/processed/final_labeled_dataset.csv")

# Remove leakage (same as training)
drop_cols = [
    "growth_scaled", "accel_scaled", "precip_scaled", "temp_scaled",
    "growth_z", "precip_z", "risk_score"
]

train_df = train_df.drop(columns=[col for col in drop_cols if col in train_df.columns])

# Encode lake_id same way
train_df = pd.get_dummies(train_df, columns=["lake_id"])

feature_cols = [col for col in train_df.columns if col not in ["date", "risk_level", "risk_label"]]

# -------------------------------
# ALIGN COLUMNS (VERY IMPORTANT)
# -------------------------------
for col in feature_cols:
    if col not in df.columns:
        df[col] = 0

df = df[feature_cols]

# -------------------------------
# PREDICT
# -------------------------------
preds = model.predict(df)

# Convert back to labels
df_preds = pd.read_csv("data/continuous/processed/recent_lake_features.csv")
df_preds["predicted_risk"] = le.inverse_transform(preds)

# -------------------------------
# SAVE
# -------------------------------
output_path = "data/continuous/final/recent_predictions.csv"
df_preds.to_csv(output_path, index=False)

print(f"🔥 Predictions saved → {output_path}")