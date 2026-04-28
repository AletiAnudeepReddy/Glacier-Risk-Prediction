import pandas as pd
import joblib

# -------------------------------
# LOAD MODEL + ENCODER
# -------------------------------
model = joblib.load("models/xgboost_model.pkl")
le = joblib.load("models/label_encoder.pkl")

# -------------------------------
# LOAD L5 FEATURE DATA
# -------------------------------
df = pd.read_csv("data/additional/L5_features.csv")

df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["lake_id", "date"])

# -------------------------------
# ONE-HOT ENCODE lake_id
# -------------------------------
df = pd.get_dummies(df, columns=["lake_id"])

# -------------------------------
# LOAD TRAIN FEATURE STRUCTURE
# -------------------------------
train_df = pd.read_csv("data/processed/final_labeled_dataset.csv")

drop_cols = [
    "growth_scaled", "accel_scaled", "precip_scaled", "temp_scaled",
    "growth_z", "precip_z", "risk_score"
]

train_df = train_df.drop(columns=[col for col in drop_cols if col in train_df.columns])
train_df = pd.get_dummies(train_df, columns=["lake_id"])

feature_cols = [col for col in train_df.columns if col not in ["date", "risk_level", "risk_label"]]

# -------------------------------
# ALIGN FEATURES (CRITICAL)
# -------------------------------
for col in feature_cols:
    if col not in df.columns:
        df[col] = 0

df = df[feature_cols]

# -------------------------------
# PREDICT
# -------------------------------
preds = model.predict(df)

# -------------------------------
# ATTACH RESULTS
# -------------------------------
df_out = pd.read_csv("data/additional/L5_features.csv")
df_out["predicted_risk"] = le.inverse_transform(preds)

# -------------------------------
# SAVE
# -------------------------------
output_path = "data/additional/L5_predictions.csv"
df_out.to_csv(output_path, index=False)

print(f"🔥 L5 predictions saved → {output_path}")