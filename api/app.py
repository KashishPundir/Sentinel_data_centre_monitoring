from pathlib import Path
import socket
import sqlite3

import joblib
import numpy as np
import pandas as pd

from fastapi import (
    FastAPI,
    HTTPException,
    Query,
)

from pydantic import BaseModel

from src.monitoring.monitoring_service import (
    MonitoringService,
)

from src.simulation.simulation_manager import (
    simulation_manager,
)


# ============================================================
# APPLICATION
# ============================================================

app = FastAPI(
    title="SentinelDC Temperature Prediction API",
    description=(
        "Real-time data center temperature "
        "prediction and monitoring service"
    ),
    version="1.0.0",
)


monitoring_service = MonitoringService()


# ============================================================
# MODEL
# ============================================================

BASE_DIR = Path(
    __file__
).resolve().parents[1]

MODEL_PATH = (
    BASE_DIR
    / "models"
    / "xgb_temperature_predictor.pkl"
)

FEATURE_PATH = (
    BASE_DIR
    / "models"
    / "feature_columns.pkl"
)

DATABASE_PATH = (
    BASE_DIR
    / "data"
    / "sentineldc.db"
)


model = joblib.load(
    MODEL_PATH
)

feature_names = joblib.load(
    FEATURE_PATH
)

print(
    "Number of features:",
    len(feature_names),
)

print(
    feature_names
)


# ============================================================
# REQUEST MODELS
# ============================================================

class ScenarioRequest(BaseModel):

    scenario: str


class SpeedRequest(BaseModel):

    speed: float


# ============================================================
# SYSTEM HEALTH HELPERS
# ============================================================

def check_kafka():
    """
    Check whether Kafka is reachable.
    """

    try:

        with socket.create_connection(
            (
                "127.0.0.1",
                9092,
            ),
            timeout=1,
        ):

            return True

    except OSError:

        return False


def check_database():
    """
    Check whether SQLite database is accessible.
    """

    try:

        conn = sqlite3.connect(
            DATABASE_PATH,
            timeout=1,
        )

        cursor = conn.cursor()

        cursor.execute(
            "SELECT 1"
        )

        cursor.fetchone()

        conn.close()

        return True

    except Exception:

        return False


# ============================================================
# ROOT
# ============================================================

@app.get("/")
def home():

    return {

        "project": "SentinelDC",

        "service": (
            "Temperature Prediction API"
        ),

        "status": "running",

        "features_required": (
            len(feature_names)
        ),
    }


# ============================================================
# SIMULATION STATUS
# ============================================================

@app.get("/simulation/status")
def simulation_status():

    return simulation_manager.get_status()


# ============================================================
# SIMULATION START
# ============================================================

@app.post("/simulation/start")
def start_simulation(
    request: SpeedRequest,
):

    try:

        return simulation_manager.start(
            speed=request.speed
        )

    except Exception as error:

        raise HTTPException(
            status_code=500,
            detail=str(error),
        )


# ============================================================
# SIMULATION PAUSE
# ============================================================

@app.post("/simulation/pause")
def pause_simulation():

    return simulation_manager.pause()


# ============================================================
# SIMULATION STOP
# ============================================================

@app.post("/simulation/stop")
def stop_simulation():

    return simulation_manager.stop()


# ============================================================
# SIMULATION SPEED
# ============================================================

@app.post("/simulation/speed")
def set_simulation_speed(
    request: SpeedRequest,
):

    try:

        return simulation_manager.set_speed(
            request.speed
        )

    except ValueError as error:

        raise HTTPException(
            status_code=400,
            detail=str(error),
        )


# ============================================================
# DEMO SCENARIO
# ============================================================

@app.post("/simulation/scenario")
def set_simulation_scenario(
    request: ScenarioRequest,
):

    try:

        return simulation_manager.set_scenario(
            request.scenario
        )

    except ValueError as error:

        raise HTTPException(
            status_code=400,
            detail=str(error),
        )


# ============================================================
# SYSTEM PIPELINE STATUS
# ============================================================

@app.get("/monitoring/system-status")
def system_status():

    simulation = (
        simulation_manager.get_status()
    )

    kafka_connected = (
        check_kafka()
    )

    database_healthy = (
        check_database()
    )

    simulation_running = (
        simulation["status"]
        in {
            "RUNNING",
            "PAUSED",
        }
    )

    return {

        "simulation": {

            "status": (
                simulation["status"]
            ),

            "running": (
                simulation_running
            ),

            "speed": (
                simulation["speed"]
            ),

            "scenario": (
                simulation["scenario"]
            ),

            "records_sent": (
                simulation["records_sent"]
            ),

            "total_records": (
                simulation["total_records"]
            ),

            "progress_percent": (
                simulation[
                    "progress_percent"
                ]
            ),
        },

        "kafka": {

            "status": (
                "CONNECTED"
                if kafka_connected
                else "DISCONNECTED"
            ),

            "healthy": (
                kafka_connected
            ),
        },

        "ml_inference": {

            "status": (
                "PROCESSING"
                if (
                    simulation_running
                    and kafka_connected
                )
                else "IDLE"
            ),

            "healthy": (
                simulation_running
                and kafka_connected
            ),
        },

        "decision_engine": {

            "status": "ACTIVE",

            "healthy": True,
        },

        "database": {

            "status": (
                "HEALTHY"
                if database_healthy
                else "ERROR"
            ),

            "healthy": (
                database_healthy
            ),
        },

        "api": {

            "status": "HEALTHY",

            "healthy": True,
        },
    }


# ============================================================
# PREDICTION
# ============================================================

@app.post("/predict")
def predict(data: dict):

    # --------------------------------------------------------
    # CHECK MISSING FEATURES
    # --------------------------------------------------------

    missing_features = [

        feature

        for feature in feature_names

        if feature not in data

    ]

    if missing_features:

        raise HTTPException(

            status_code=400,

            detail={

                "message": (
                    "Missing required features"
                ),

                "missing_count": (
                    len(missing_features)
                ),

                "missing_features": (
                    missing_features[:20]
                ),
            },
        )

    # --------------------------------------------------------
    # CREATE DATAFRAME
    # --------------------------------------------------------

    input_df = pd.DataFrame(
        [data]
    )

    # --------------------------------------------------------
    # ENSURE CORRECT FEATURE ORDER
    # --------------------------------------------------------

    input_df = input_df[
        feature_names
    ]

    # --------------------------------------------------------
    # PREDICTION
    # --------------------------------------------------------

    prediction = model.predict(
        input_df
    )

    prediction_value = float(
        np.asarray(
            prediction
        ).reshape(-1)[0]
    )

    return {

        "prediction": (
            prediction_value
        ),

        "target": (
            "Module_8_Avg_Temp"
        ),

        "prediction_horizon_seconds": (
            60
        ),
    }


# ============================================================
# MONITORING API
# ============================================================

@app.get("/monitoring/health")
def monitoring_health():

    return monitoring_service.get_health()


@app.get("/monitoring/summary")
def monitoring_summary():

    return monitoring_service.get_summary()


@app.get("/monitoring/predictions")
def monitoring_predictions(

    limit: int = Query(
        default=50,
        ge=1,
        le=500,
    )
):

    return monitoring_service.get_predictions(
        limit
    )


@app.get("/monitoring/alerts")
def monitoring_alerts(

    limit: int = Query(
        default=50,
        ge=1,
        le=500,
    )
):

    return monitoring_service.get_alerts(
        limit
    )


@app.get("/monitoring/modules")
def monitoring_modules():

    return monitoring_service.get_module_status()


@app.get("/monitoring/system-events")
def monitoring_system_events(

    limit: int = Query(
        default=50,
        ge=1,
        le=500,
    )
):

    return monitoring_service.get_system_events(
        limit
    )


@app.get(
    "/monitoring/modules/{module}/history"
)
def monitoring_module_history(

    module: str,

    limit: int = Query(
        default=100,
        ge=1,
        le=1000,
    ),
):

    return monitoring_service.get_module_history(
        module=module,
        limit=limit,
    )