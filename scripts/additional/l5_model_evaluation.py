import pandas as pd
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix

# -------------------------------
# LOAD DATA
# -------------------------------
pred_df = pd.read_csv("data/additional/L5_predictions.csv")
true_df = pd.read_csv("data/additional/L5_labeled_dataset.csv")

# -------------------------------
# FORMAT + SORT
# -------------------------------
pred_df["date"] = pd.to_datetime(pred_df["date"])
true_df["date"] = pd.to_datetime(true_df["date"])

pred_df = pred_df.sort_values("date")
true_df = true_df.sort_values("date")

# -------------------------------
# MERGE ON DATE
# -------------------------------
df = pd.merge(
    pred_df,
    true_df[["date", "risk_level"]],
    on="date",
    how="inner"
)

# Rename for clarity
df = df.rename(columns={
    "risk_level": "true_risk",
    "predicted_risk": "pred_risk"
})

# -------------------------------
# CLEAN
# -------------------------------
df = df.dropna(subset=["true_risk", "pred_risk"])

# -------------------------------
# DEBUG (IMPORTANT)
# -------------------------------
print("\n📊 TRUE LABEL DISTRIBUTION:\n")
print(df["true_risk"].value_counts())

print("\n📊 PREDICTED LABEL DISTRIBUTION:\n")
print(df["pred_risk"].value_counts())

# -------------------------------
# ACCURACY
# -------------------------------
accuracy = accuracy_score(df["true_risk"], df["pred_risk"])
print(f"\n🔥 L5 MODEL ACCURACY: {accuracy:.4f}")

# -------------------------------
# CLASSIFICATION REPORT
# -------------------------------
print("\n📊 Classification Report:\n")
print(classification_report(
    df["true_risk"],
    df["pred_risk"],
    zero_division=0   # removes warning
))

# -------------------------------
# CONFUSION MATRIX
# -------------------------------
labels = ["Low", "Medium", "High"]

cm = confusion_matrix(
    df["true_risk"],
    df["pred_risk"],
    labels=labels
)

print("\n📉 Confusion Matrix:\n")
print(pd.DataFrame(cm, index=labels, columns=labels))

# -------------------------------
# MATCH ANALYSIS
# -------------------------------
df["match"] = df["true_risk"] == df["pred_risk"]

print("\n✅ MATCH COUNT:")
print(df["match"].value_counts())

# -------------------------------
# SAVE RESULTS
# -------------------------------
output_path = "data/additional/L5_evaluation_results.csv"
df.to_csv(output_path, index=False)

print(f"\n🔥 Evaluation results saved → {output_path}")