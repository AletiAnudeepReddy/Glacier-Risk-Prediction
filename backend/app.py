from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.routes.timeline import router as timeline_router
from backend.routes.confusion import router as confusion_router
from backend.routes.features import router as features_router

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routes
app.include_router(timeline_router)
app.include_router(confusion_router)
app.include_router(features_router)
from backend.routes import stats, lakes, charts, alerts

app.include_router(stats.router)
app.include_router(lakes.router)
app.include_router(charts.router)
app.include_router(alerts.router)
from backend.routes import otp, subscribe

app.include_router(otp.router)
app.include_router(subscribe.router)
from backend.routes import pipeline

app.include_router(pipeline.router)
from backend.routes import alert_trigger

app.include_router(alert_trigger.router)

@app.get("/")
def home():
    return {"message": "Glacier Risk API running"}