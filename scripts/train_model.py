import pandas as pd
import numpy as np
import os
import joblib

from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.preprocessing import LabelEncoder

import xgboost as xgb

# -------------------------------
# LOAD DATA
# -------------------------------
df = pd.read_csv("data/processed/final_labeled_dataset.csv")

df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["lake_id", "date"])

print("Dataset shape:", df.shape)
print(df.head())

# -------------------------------
# REMOVE DATA LEAKAGE FEATURES
# -------------------------------
drop_cols = [
    "growth_scaled", "accel_scaled", "precip_scaled", "temp_scaled",
    "growth_z", "precip_z", "risk_score"
]

df = df.drop(columns=[col for col in drop_cols if col in df.columns])

# -------------------------------
# ENCODE TARGET
# -------------------------------
le = LabelEncoder()
df["risk_label"] = le.fit_transform(df["risk_level"])

# -------------------------------
# ONE-HOT ENCODE lake_id
# -------------------------------
df = pd.get_dummies(df, columns=["lake_id"])

# -------------------------------
# SELECT FEATURES
# -------------------------------
feature_cols = [col for col in df.columns if col not in ["date", "risk_level", "risk_label"]]

X = df[feature_cols]
y = df["risk_label"]

# -------------------------------
# TIME-BASED SPLIT (VERY IMPORTANT)
# -------------------------------
train_df = df[df["date"] < "2020-01-01"]
test_df  = df[df["date"] >= "2020-01-01"]

X_train = train_df[feature_cols]
y_train = train_df["risk_label"]

X_test = test_df[feature_cols]
y_test = test_df["risk_label"]

print("\nTrain size:", X_train.shape)
print("Test size:", X_test.shape)

# -------------------------------
# BASELINE MODEL - RANDOM FOREST
# -------------------------------
rf = RandomForestClassifier(
    n_estimators=200,
    max_depth=6,
    random_state=42,
    class_weight="balanced"
)

rf.fit(X_train, y_train)

rf_preds = rf.predict(X_test)

print("\n🔥 RANDOM FOREST RESULTS")
print(classification_report(y_test, rf_preds, target_names=le.classes_))
print("Confusion Matrix:\n", confusion_matrix(y_test, rf_preds))

# -------------------------------
# FINAL MODEL - XGBOOST (IMPROVED)
# -------------------------------
xgb_model = xgb.XGBClassifier(
    n_estimators=400,
    max_depth=6,
    learning_rate=0.03,
    subsample=0.85,
    colsample_bytree=0.85,
    objective="multi:softmax",   # FIX
    num_class=3,                 # FIX
    random_state=42,
    eval_metric="mlogloss"
)

xgb_model.fit(X_train, y_train)

xgb_preds = xgb_model.predict(X_test)

print("\n🚀 XGBOOST RESULTS")
print(classification_report(y_test, xgb_preds, target_names=le.classes_))
print("Confusion Matrix:\n", confusion_matrix(y_test, xgb_preds))

# -------------------------------
# FEATURE IMPORTANCE
# -------------------------------
importances = pd.Series(
    xgb_model.feature_importances_,
    index=X_train.columns
).sort_values(ascending=False)

print("\n🔥 TOP FEATURES:")
print(importances.head(15))

# -------------------------------
# SAVE MODEL (FIXED)
# -------------------------------
os.makedirs("models", exist_ok=True)

joblib.dump(xgb_model, "models/xgboost_model.pkl")
joblib.dump(le, "models/label_encoder.pkl")

print("\n✅ Model saved at models/xgboost_model.pkl")

# -------------------------------
# SAVE PREDICTIONS
# -------------------------------
df_test_results = test_df.copy()
df_test_results["predicted_risk"] = le.inverse_transform(xgb_preds)

df_test_results.to_csv("data/processed/model_predictions.csv", index=False)

print("✅ Predictions saved!")

print("\n🔥 TRAINING COMPLETE")