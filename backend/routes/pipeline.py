from fastapi import APIRouter, BackgroundTasks
import subprocess
import sys
from backend.services.pipeline_state import pipeline_status

router = APIRouter()

pipeline_steps = [
    "scripts/ndwi/weekly_monitoring.py",
    "scripts/ndwi/weekly_climate_monitoring.py",
    "scripts/ndwi/compute_features.py",
    "scripts/ndwi/labelling_dataset.py",
    "scripts/ndwi/risk_level_prediction.py",
    "scripts/ndwi/final_risk_predictions.py"
]


# -------------------------------
# PIPELINE FUNCTION
# -------------------------------
def run_pipeline():

    pipeline_status["status"] = "running"

    for step in pipeline_steps:
        pipeline_status["current_step"] = step
        print(f"🚀 Running: {step}")

        result = subprocess.run([sys.executable, step])

        if result.returncode != 0:
            pipeline_status["status"] = "failed"
            print(f"❌ Failed at: {step}")
            return

    pipeline_status["status"] = "completed"
    pipeline_status["current_step"] = None
    print("🔥 Pipeline completed")


# -------------------------------
# START PIPELINE
# -------------------------------
@router.post("/run-pipeline")
def trigger_pipeline(background_tasks: BackgroundTasks):

    # prevent duplicate runs
    if pipeline_status["status"] == "running":
        return {"message": "Pipeline already running"}

    background_tasks.add_task(run_pipeline)

    return {"message": "Pipeline started"}


# -------------------------------
# GET STATUS
# -------------------------------
@router.get("/pipeline-status")
def get_status():
    return pipeline_status