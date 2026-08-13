import json
import pickle
from pathlib import Path

import numpy as np
from kafka import KafkaConsumer

from src.decision.decision_engine import TemperatureDecisionEngine
from src.alerts.alert_manager import AlertManager
from datetime import datetime, timezone

from src.storage.database import initialize_database
from src.storage.models import (
    PredictionRecord,
    AlertRecord,
    SystemEvent,
)
from src.storage.repository import SentinelRepository

# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]
KAFKA_SERVER = "127.0.0.1:9092"
TOPIC_NAME = "datacenter_telemetry_v2"

MODEL_PATH = BASE_DIR / "models" / "xgb_temperature_predictor.pkl"
FEATURE_PATH = BASE_DIR / "models" / "feature_columns.pkl"
THRESHOLD_PATH = BASE_DIR / "models" / "temperature_thresholds.json"


# ============================================================
# HEADER
# ============================================================

print("=" * 70)
print("SENTINELDC KAFKA ML CONSUMER")
print("=" * 70)


# ============================================================
# LOAD MODEL
# ============================================================

print("\nLoading model...")

with open(MODEL_PATH, "rb") as f:
    model = pickle.load(f)

print("Model loaded successfully.")


# ============================================================
# LOAD FEATURE COLUMNS
# ============================================================

print("Loading feature columns...")

with open(FEATURE_PATH, "rb") as f:
    feature_columns = pickle.load(f)

print("Number of features:", len(feature_columns))


# ============================================================
# LOAD TEMPERATURE THRESHOLDS
# ============================================================

print("Loading temperature thresholds...")

with open(THRESHOLD_PATH, "r", encoding="utf-8") as f:
    thresholds_raw = json.load(f)


# ============================================================
# NORMALIZE THRESHOLD FORMAT
# ============================================================

thresholds = {}


# ------------------------------------------------------------
# FORMAT 1:
#
# {
#     "Module_1_Avg_Temp": {
#         "warning": 24.45,
#         "critical": 24.89
#     }
# }
# ------------------------------------------------------------

if isinstance(thresholds_raw, dict):

    for module, values in thresholds_raw.items():

        if not isinstance(values, dict):

            raise ValueError(
                f"Invalid threshold data for {module}: "
                f"{values}"
            )

        # Format with lowercase keys
        if (
            "warning" in values
            and "critical" in values
        ):

            thresholds[module] = {
                "warning": float(values["warning"]),
                "critical": float(values["critical"])
            }

        # Format with notebook column names
        elif (
            "Warning_95" in values
            and "Critical_99" in values
        ):

            thresholds[module] = {
                "warning": float(values["Warning_95"]),
                "critical": float(values["Critical_99"])
            }

        else:

            raise ValueError(
                f"Invalid threshold keys for {module}: "
                f"{list(values.keys())}"
            )


# ------------------------------------------------------------
# FORMAT 2:
#
# [
#     {
#         "Module": "Module_1_Avg_Temp",
#         "Warning_95": 24.45,
#         "Critical_99": 24.89
#     }
# ]
# ------------------------------------------------------------

elif isinstance(thresholds_raw, list):

    for item in thresholds_raw:

        if not isinstance(item, dict):

            raise ValueError(
                f"Invalid threshold item: {item}"
            )

        module = item["Module"]

        thresholds[module] = {
            "warning": float(
                item["Warning_95"]
            ),
            "critical": float(
                item["Critical_99"]
            )
        }


# ------------------------------------------------------------
# Unknown format
# ------------------------------------------------------------

else:

    raise ValueError(
        "Unsupported temperature threshold JSON format."
    )


print(
    "Thresholds loaded for",
    len(thresholds),
    "modules."
)



# ============================================================
# CREATE DECISION ENGINE
# ============================================================

decision_engine = TemperatureDecisionEngine(
    thresholds
)

print("Decision engine initialized.")


# ============================================================
# CREATE ALERT MANAGER
# ============================================================

alert_manager = AlertManager(
    log_path=BASE_DIR / "logs" / "alerts.jsonl"
)

print("Alert manager initialized.")


# ============================================================
# DATABASE
# ============================================================

print("Initializing database...")

initialize_database()

repository = SentinelRepository()

print("Database initialized successfully.")


# Record system startup
try:

    repository.save_system_event(
        SystemEvent(
            timestamp=datetime.now(
                timezone.utc
            ).isoformat(),

            event_type="KAFKA_CONSUMER",

            status="STARTED",

            message=(
                "SentinelDC Kafka ML consumer "
                "started successfully."
            ),
        )
    )

except Exception as error:

    print(
        f"[STORAGE ERROR] "
        f"Could not save startup event: {error}"
    )


print("Connecting to Kafka...")

# ============================================================
# CREATE KAFKA CONSUMER
# ============================================================

consumer = KafkaConsumer(

    TOPIC_NAME,

    bootstrap_servers=KAFKA_SERVER,

    group_id="sentineldc-ml-consumer",

    auto_offset_reset="earliest",

    enable_auto_commit=True,

    value_deserializer=lambda value:
        json.loads(value.decode("utf-8"))
)

print("Kafka consumer connected.")

print("=" * 70)
print("WAITING FOR TELEMETRY...")
print("=" * 70)


# ============================================================
# PROCESS MESSAGES
# ============================================================

try:

    for message in consumer:

        try:

            data = message.value

            event_timestamp = datetime.now(
                timezone.utc
            ).isoformat()

            replay_id = data.get(
                "_replay_id",
                "unknown"
            )

            # ------------------------------------------------
            # Remove metadata from feature dictionary
            # ------------------------------------------------

            data_for_model = {
                key: value
                for key, value in data.items()
                if key != "_replay_id"
            }

            # ------------------------------------------------
            # Build feature vector
            # ------------------------------------------------

            missing_features = [
                feature
                for feature in feature_columns
                if feature not in data_for_model
            ]

            if missing_features:

                print(
                    f"\n[ERROR] replay_id={replay_id} "
                    f"missing {len(missing_features)} features"
                )

                print(
                    "Missing features:",
                    missing_features[:10]
                )

                continue

            X = np.array(
                [
                    [
                        data_for_model[feature]
                        for feature in feature_columns
                    ]
                ],
                dtype=float
            )

            # ------------------------------------------------
            # ML Prediction
            # ------------------------------------------------

            prediction = model.predict(X)

            prediction = np.asarray(
                prediction
            )

            # ------------------------------------------------
            # Handle multi-output prediction
            # ------------------------------------------------

            if prediction.ndim == 1:

                prediction = prediction.reshape(
                    1,
                    -1
                )

            predictions = prediction[0]

            # ------------------------------------------------
            # Current temperatures
            # ------------------------------------------------

            current_temperatures = {}

            for module_number in range(1, 9):

                module_name = (
                    f"Module_{module_number}_Avg_Temp"
                )

                current_temperatures[
                    module_name
                ] = data_for_model.get(
                    module_name
                )

            # ------------------------------------------------
            # Prediction output
            # ------------------------------------------------

            print(
                f"\n[PREDICTION] "
                f"replay_id={replay_id} | "
                f"partition={message.partition} | "
                f"offset={message.offset}"
            )

            print("-" * 60)

            # ------------------------------------------------
            # Decision + Alert for every module
            # ------------------------------------------------

            for i, module_number in enumerate(
                range(1, 9)
            ):

                module_name = (
                    f"Module_{module_number}_Avg_Temp"
                )

                predicted_temperature = float(
                    predictions[i]
                )

                current_temperature = (
                    current_temperatures[
                        module_name
                    ]
                )

                # --------------------------------------------
                # Decision Engine
                # --------------------------------------------

                decision = decision_engine.evaluate(

                    module=module_name,

                    predicted_temperature=
                        predicted_temperature,

                    current_temperature=
                        current_temperature
                )

                prediction_record = PredictionRecord(

                    timestamp=event_timestamp,

                    replay_id=replay_id,

                    kafka_partition=message.partition,

                    kafka_offset=message.offset,

                    module=decision["module"],

                    current_temperature=decision.get(
                        "current_temperature"
                    ),

                    predicted_temperature=decision[
                        "predicted_temperature"
                    ],

                    temperature_delta=decision.get(
                        "temperature_delta"
                    ),

                    warning_threshold=decision[
                        "warning_threshold"
                    ],

                    critical_threshold=decision[
                        "critical_threshold"
                    ],

                    risk_level=decision[
                        "risk_level"
                    ]
                )

                try:

                    repository.save_prediction(
                        prediction_record
                    )

                except Exception as storage_error:

                    print(
                        f"[STORAGE ERROR] "
                        f"Failed to save prediction: "
                        f"{storage_error}"
                    )

                # --------------------------------------------
                # Add Kafka metadata
                # --------------------------------------------

                decision["replay_id"] = replay_id

                decision["kafka_partition"] = (
                    message.partition
                )

                decision["kafka_offset"] = (
                    message.offset
                )

                # --------------------------------------------
                # Print decision
                # --------------------------------------------

                print(
                    f"{module_name}: "
                    f"current="
                    f"{current_temperature:.3f}°C | "
                    f"predicted="
                    f"{predicted_temperature:.3f}°C | "
                    f"risk="
                    f"{decision['risk_level']}"
                )

                # --------------------------------------------
                # Alert Manager
                # --------------------------------------------

                alert = alert_manager.process_decision(
                    decision
                )

                if alert is not None:

                    print(
                        "\n"
                        f"[ALERT] "
                        f"{alert['severity']} | "
                        f"{alert['module']}"
                    )

                    print(
                        f"Message: "
                        f"{alert['message']}"
                    )

                if decision["risk_level"] in {
                    "WARNING",
                    "CRITICAL"
                }:

                    alert_message = (
                        f"{decision['risk_level']} temperature risk "
                        f"detected in {decision['module']}. "
                        f"Predicted temperature: "
                        f"{decision['predicted_temperature']:.2f}°C."
                    )

                    try:

                        alert_record = AlertRecord(

                            timestamp=event_timestamp,

                            replay_id=replay_id,

                            kafka_partition=message.partition,

                            kafka_offset=message.offset,

                            module=decision["module"],

                            alert_level=decision["risk_level"],

                            predicted_temperature=decision[
                                "predicted_temperature"
                            ],

                            current_temperature=decision.get(
                                "current_temperature"
                            ),

                            warning_threshold=decision[
                                "warning_threshold"
                            ],

                            critical_threshold=decision[
                                "critical_threshold"
                            ],

                            message=alert_message,
                        )

                        repository.save_alert(
                            alert_record
                        )

                    except Exception as storage_error:

                        print(
                            f"[STORAGE ERROR] "
                            f"Failed to save alert: "
                            f"{storage_error}"
                        )
            print("-" * 60)

        except Exception as error:

            print(
                f"\n[ERROR] Failed to process "
                f"message: {error}"
            )

            continue


except KeyboardInterrupt:

    print("\nConsumer stopped by user.")


finally:

    consumer.close()

    print(
        "\nKafka consumer closed."
    )