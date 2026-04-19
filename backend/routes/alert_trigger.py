from fastapi import APIRouter
import subprocess
import sys
from backend.services.pipeline_state import pipeline_status

router = APIRouter()

@router.post("/trigger-alerts")
def trigger_alerts():

    print("🚀 Triggering alerts...")


    try:
        # ✅ RUN AS MODULE (IMPORTANT FIX)
        result = subprocess.run(
            [sys.executable, "-m", "backend.scheduler.send_alerts"],
            capture_output=True,
            text=True
        )

        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)

        if result.returncode != 0:
            return {
                "success": False,
                "error": result.stderr
            }

        return {
            "success": True,
            "message": "Alerts sent successfully"
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }