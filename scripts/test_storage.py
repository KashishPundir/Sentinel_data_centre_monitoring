from datetime import datetime, timezone

from src.storage.database import initialize_database
from src.storage.models import (
    PredictionRecord,
    AlertRecord,
    SystemEvent,
)
from src.storage.repository import SentinelRepository


# ============================================================
# INITIALIZE
# ============================================================

initialize_database()

repository = SentinelRepository()


# ============================================================
# TEST PREDICTION
# ============================================================

prediction = PredictionRecord(

    timestamp=datetime.now(timezone.utc).isoformat(),

    replay_id=1,

    kafka_partition=0,

    kafka_offset=10,

    module="Module_6_Avg_Temp",

    current_temperature=27.14,

    predicted_temperature=27.06,

    temperature_delta=-0.08,

    warning_threshold=27.05,

    critical_threshold=27.30,

    risk_level="WARNING",
)

repository.save_prediction(prediction)

print("Prediction saved successfully.")


# ============================================================
# TEST ALERT
# ============================================================

alert = AlertRecord(

    timestamp=datetime.now(timezone.utc).isoformat(),

    replay_id=1,

    kafka_partition=0,

    kafka_offset=10,

    module="Module_6_Avg_Temp",

    alert_level="WARNING",

    predicted_temperature=27.06,

    current_temperature=27.14,

    warning_threshold=27.05,

    critical_threshold=27.30,

    message=(
        "WARNING temperature risk detected "
        "in Module_6_Avg_Temp."
    ),
)

repository.save_alert(alert)

print("Alert saved successfully.")


# ============================================================
# TEST SYSTEM EVENT
# ============================================================

event = SystemEvent(

    timestamp=datetime.now(timezone.utc).isoformat(),

    event_type="TEST",

    status="SUCCESS",

    message="Storage layer test completed.",
)

repository.save_system_event(event)

print("System event saved successfully.")


# ============================================================
# READ DATA BACK
# ============================================================

print("\nLatest predictions:")

predictions = repository.get_latest_predictions()

for item in predictions:

    print(item)


print("\nLatest alerts:")

alerts = repository.get_latest_alerts()

for item in alerts:

    print(item)


print("\nAlert summary:")

print(repository.get_alert_summary())


print("\nPrediction count:")

print(repository.get_prediction_count())


print("\nDatabase health:")

print(repository.health_check())