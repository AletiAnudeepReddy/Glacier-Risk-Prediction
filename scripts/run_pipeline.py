import subprocess
import sys

# -------------------------------
# PIPELINE STEPS (ORDER MATTERS)
# -------------------------------
pipeline = [
    "scripts/ndwi/weekly_monitoring.py",
    "scripts/ndwi/weekly_climate_monitoring.py",
    "scripts/ndwi/compute_features.py",
    "scripts/ndwi/labelling_dataset.py",
    "scripts/ndwi/risk_level_prediction.py",
    "scripts/ndwi/final_risk_predictions.py"cls
    
]

# -------------------------------
# RUN PIPELINE
# -------------------------------
for step in pipeline:
    print(f"\n🚀 Running: {step}")
    
    result = subprocess.run([sys.executable, step])
    
    if result.returncode != 0:
        print(f"❌ Error in {step}")
        sys.exit(1)

    print(f"✅ Completed: {step}")

print("\n🔥 FULL PIPELINE EXECUTED SUCCESSFULLY")