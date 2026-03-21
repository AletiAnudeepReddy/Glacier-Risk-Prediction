import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# -------------------------------
# LOAD DATA
# -------------------------------
df = pd.read_csv("data/processed/model_predictions.csv")

# If needed
df["date"] = pd.to_datetime(df["date"])

# -------------------------------
# 1. CONFUSION MATRIX
# -------------------------------
from sklearn.metrics import confusion_matrix

y_true = df["risk_level"]
y_pred = df["predicted_risk"]

labels = ["Low", "Medium", "High"]

cm = confusion_matrix(y_true, y_pred, labels=labels)

plt.figure()
sns.heatmap(cm, annot=True, fmt="d", xticklabels=labels, yticklabels=labels)
plt.title("Confusion Matrix")
plt.xlabel("Predicted")
plt.ylabel("Actual")
plt.show()

# -------------------------------
# 2. FEATURE IMPORTANCE
# -------------------------------
# Reload feature importance from training (manual for now)

importances = {
    "temp_lag_1": 0.148,
    "growth_rate": 0.075,
    "area_rolling_std_3": 0.073,
    "area_change": 0.068,
    "growth_acceleration": 0.066,
    "month": 0.063,
    "precip_rolling_3": 0.061
}

imp_df = pd.DataFrame({
    "feature": list(importances.keys()),
    "importance": list(importances.values())
}).sort_values(by="importance", ascending=True)

plt.figure()
plt.barh(imp_df["feature"], imp_df["importance"])
plt.title("Top Feature Importance")
plt.xlabel("Importance")
plt.show()

# -------------------------------
# 3. RISK TIMELINE (BEST)
# -------------------------------
lake = "L1"  # change to L2, L3, L4

lake_df = df[df["lake_id_L1"] == True].copy()

plt.figure()

# Plot area
plt.plot(lake_df["date"], lake_df["lake_area_km2"], label="Lake Area")

# Plot predicted risk (numeric)
risk_map = {"Low": 1, "Medium": 2, "High": 3}
lake_df["risk_numeric"] = lake_df["predicted_risk"].map(risk_map)

plt.plot(lake_df["date"], lake_df["risk_numeric"], linestyle="--", label="Risk Level")

plt.title(f"Lake {lake} Risk Timeline")
plt.xlabel("Date")
plt.ylabel("Value")
plt.legend()

plt.show()