from pathlib import Path
import os
import secrets
import socket
import sqlite3

import joblib
import numpy as np
import pandas as pd

from fastapi import (
    FastAPI,
    Depends,
    Header,
    HTTPException,
    Query,
)

from pydantic import BaseModel

from src.monitoring.monitoring_service import (
    MonitoringService,
)
from src.monitoring.worker_manager import telemetry_worker_manager

from src.simulation.simulation_manager import (
    simulation_manager,
)
from src.storage.database import initialize_database


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


@app.on_event("startup")
def initialize_storage() -> None:
    """Run safe schema migrations before accepting traffic."""
    initialize_database()
    telemetry_worker_manager.ensure_running()


@app.on_event("shutdown")
def stop_telemetry_worker() -> None:
    telemetry_worker_manager.stop()


monitoring_service = MonitoringService()


# ============================================================
# GLOBAL CONFIGURATION
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


# IMPORTANT:
# The new model has been trained for a
# timestamp-based 5-minute forecast.

PREDICTION_HORIZON_SECONDS = 300

PREDICTION_HORIZON_MINUTES = (
    PREDICTION_HORIZON_SECONDS // 60
)


# ============================================================
# LOAD MODEL
# ============================================================

print("=" * 70)
print("SENTINELDC API")
print("=" * 70)

print("\nLoading XGBoost model...")

try:

    model = joblib.load(
        MODEL_PATH
    )

    print(
        "Model loaded successfully."
    )

except Exception as error:

    print(
        f"ERROR loading model: {error}"
    )

    raise


# ============================================================
# LOAD FEATURE COLUMNS
# ============================================================

print(
    "Loading feature columns..."
)

try:

    feature_names = joblib.load(
        FEATURE_PATH
    )

    print(
        "Number of features:",
        len(feature_names)
    )

except Exception as error:

    print(
        f"ERROR loading feature columns: "
        f"{error}"
    )

    raise


# ============================================================
# REQUEST MODELS
# ============================================================

class ScenarioRequest(BaseModel):

    scenario: str


class SpeedRequest(BaseModel):

    speed: float


def require_admin(
    x_api_key: str | None = Header(default=None),
) -> None:
    """Protect operational controls when deployed with an admin key.

    Set SENTINELDC_ADMIN_API_KEY in non-demo deployments.  Leaving it unset
    intentionally keeps the existing local replay workflow frictionless.
    """
    configured_key = os.getenv("SENTINELDC_ADMIN_API_KEY")
    if configured_key and not (
        x_api_key and secrets.compare_digest(x_api_key, configured_key)
    ):
        raise HTTPException(status_code=401, detail="invalid admin API key")


# ============================================================
# SYSTEM HEALTH
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
    Check whether SQLite is accessible.
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

        "project":
            "SentinelDC",

        "service":
            "Temperature Prediction API",

        "status":
            "running",

        "features_required":
            len(feature_names),

        "prediction_horizon_seconds":
            PREDICTION_HORIZON_SECONDS,

        "prediction_horizon":
            f"{PREDICTION_HORIZON_MINUTES} minutes",

    }


@app.get("/health/live")
def liveness():
    """Process liveness probe; no dependency checks by design."""
    return {"status": "ok"}


@app.get("/health/ready")
def readiness():
    """Readiness probe for a load balancer or container orchestrator."""
    database_ready = check_database()
    if not database_ready:
        raise HTTPException(status_code=503, detail="database unavailable")
    return {"status": "ready", "database": "healthy", "model": "loaded"}


# ============================================================
# FORECAST CONFIGURATION
# ============================================================

@app.get("/forecast/config")
def forecast_config():

    return {

        "horizon_seconds":
            PREDICTION_HORIZON_SECONDS,

        "horizon_minutes":
            PREDICTION_HORIZON_MINUTES,

        "description":
            (
                "XGBoost predicts module "
                "temperature 5 minutes ahead."
            ),

        "model":
            "XGBoost",

        "modules":
            8,

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
    _: None = Depends(require_admin),
):

    telemetry_worker_manager.ensure_running()

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
def pause_simulation(_: None = Depends(require_admin)):

    return simulation_manager.pause()


# ============================================================
# SIMULATION STOP
# ============================================================

@app.post("/simulation/stop")
def stop_simulation(_: None = Depends(require_admin)):

    return simulation_manager.stop()


# ============================================================
# SIMULATION SPEED
# ============================================================

@app.post("/simulation/speed")
def set_simulation_speed(
    request: SpeedRequest,
    _: None = Depends(require_admin),
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
    _: None = Depends(require_admin),
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

    telemetry_worker = telemetry_worker_manager.get_status()

    simulation_running = (
        simulation["status"]
        in {
            "RUNNING",
            "PAUSED",
        }
    )

    return {

        "simulation": {

            "status":
                simulation["status"],

            "running":
                simulation_running,

            "speed":
                simulation["speed"],

            "scenario":
                simulation.get(
                    "scenario",
                    "NORMAL"
                ),

            "records_sent":
                simulation[
                    "records_sent"
                ],

            "total_records":
                simulation[
                    "total_records"
                ],

            "progress_percent":
                simulation[
                    "progress_percent"
                ],

            "last_event_timestamp":
                simulation.get(
                    "last_event_timestamp"
                ),

        },

        "kafka": {

            "status":
                (
                    "CONNECTED"
                    if kafka_connected
                    else "DISCONNECTED"
                ),

            "healthy":
                kafka_connected,

        },

        "ml_inference": {

            "status":
                (
                    "PROCESSING"
                    if (
                        simulation_running
                        and kafka_connected
                        and telemetry_worker["healthy"]
                    )
                    else telemetry_worker["status"]
                ),

            "healthy":
                (
                    telemetry_worker["healthy"]
                    and kafka_connected
                ),

            "worker": telemetry_worker,

        },

        "decision_engine": {

            "status":
                "ACTIVE",

            "healthy":
                True,

        },

        "database": {

            "status":
                (
                    "HEALTHY"
                    if database_healthy
                    else "ERROR"
                ),

            "healthy":
                database_healthy,

        },

        "api": {

            "status":
                "HEALTHY",

            "healthy":
                True,

        },

        "model": {

            "status":
                "LOADED",

            "healthy":
                True,

        },

        "forecast": {

            "horizon_seconds":
                PREDICTION_HORIZON_SECONDS,

            "horizon_minutes":
                PREDICTION_HORIZON_MINUTES,

            "model":
                "XGBoost",

        },

    }


# ============================================================
# DIRECT PREDICTION API
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

                "message":
                    "Missing required features",

                "missing_count":
                    len(missing_features),

                "missing_features":
                    missing_features[:20],

            },

        )


    # --------------------------------------------------------
    # CREATE DATAFRAME
    # --------------------------------------------------------

    input_df = pd.DataFrame(
        [data]
    )


    # --------------------------------------------------------
    # ENSURE FEATURE ORDER
    # --------------------------------------------------------

    input_df = input_df[
        feature_names
    ]

    # Reject values that would silently poison model output (NaN/Infinity).
    try:
        input_df = input_df.astype(float)
    except (TypeError, ValueError) as error:
        raise HTTPException(
            status_code=422,
            detail="All model features must be numeric.",
        ) from error

    if not np.isfinite(input_df.to_numpy()).all():
        raise HTTPException(
            status_code=422,
            detail="All model features must be finite.",
        )


    # --------------------------------------------------------
    # MODEL PREDICTION
    # --------------------------------------------------------

    prediction = model.predict(
        input_df
    )


    prediction_array = np.asarray(
        prediction
    )


    # --------------------------------------------------------
    # HANDLE MODEL OUTPUT
    # --------------------------------------------------------

    if (
        prediction_array.ndim >= 2
        and prediction_array.shape[1] >= 8
    ):

        predictions = (
            prediction_array[0]
            .tolist()
        )

        module_predictions = {

            f"Module_{index + 1}_Avg_Temp":
                float(value)

            for index, value
            in enumerate(predictions)

        }

        primary_prediction = float(
            predictions[0]
        )

    else:

        primary_prediction = float(
            prediction_array
            .reshape(-1)[0]
        )

        module_predictions = {}


    # --------------------------------------------------------
    # RESPONSE
    # --------------------------------------------------------

    return {

        "prediction":
            primary_prediction,

        "module_predictions":
            module_predictions,

        "target":
            "Data center module temperature",

        "prediction_horizon_seconds":
            PREDICTION_HORIZON_SECONDS,

        "prediction_horizon":
            f"{PREDICTION_HORIZON_MINUTES} minutes",

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


@app.get("/monitoring/forecast-performance")
def monitoring_forecast_performance():

    return monitoring_service.get_forecast_performance(
        horizon_seconds=PREDICTION_HORIZON_SECONDS,
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
