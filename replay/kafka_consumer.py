"""Production-oriented Kafka worker for SentinelDC telemetry.

The worker intentionally commits Kafka offsets only after validation,
inference, decisions and storage have completed.  This provides at-least-once
delivery; repository idempotency makes replays safe after a process crash.
"""
import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import joblib
import numpy as np
from kafka import KafkaConsumer

from src.alerts.alert_manager import AlertManager
from src.decision.decision_engine import TemperatureDecisionEngine
from src.storage.database import initialize_database
from src.storage.models import AlertRecord, PredictionRecord, SystemEvent
from src.storage.repository import SentinelRepository

BASE_DIR = Path(__file__).resolve().parents[1]
TOPIC_NAME = os.getenv("SENTINELDC_TOPIC", "datacenter_telemetry_v2")
KAFKA_SERVER = os.getenv("SENTINELDC_KAFKA_BOOTSTRAP", "127.0.0.1:9092")
CONSUMER_GROUP = os.getenv("SENTINELDC_CONSUMER_GROUP", "sentineldc-ml-consumer-v1")
MODULES = tuple(f"Module_{number}_Avg_Temp" for number in range(1, 9))

logging.basicConfig(
    level=os.getenv("SENTINELDC_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("sentineldc.consumer")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def telemetry_timestamp(payload: Dict[str, Any]) -> str:
    """Use source time so accelerated replays retain a usable timeline."""
    raw_timestamp = payload.get("Time [Date/Time]") or payload.get("timestamp")
    if raw_timestamp is None:
        return utc_now()
    text = str(raw_timestamp).strip()
    for timestamp_format in (
        "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M:%S",
    ):
        try:
            parsed = datetime.strptime(text.replace("Z", "+0000"), timestamp_format)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).isoformat()
        except ValueError:
            continue
    log.warning("Invalid telemetry timestamp %r; using ingestion time", raw_timestamp)
    return utc_now()


def load_thresholds(path: Path) -> Dict[str, Dict[str, float]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Threshold configuration must be a JSON object")
    thresholds = {}
    for module, values in raw.items():
        if not isinstance(values, dict):
            raise ValueError(f"Invalid threshold configuration for {module}")
        warning = values.get("warning", values.get("Warning_95"))
        critical = values.get("critical", values.get("Critical_99"))
        if warning is None or critical is None or float(warning) >= float(critical):
            raise ValueError(f"Invalid warning/critical thresholds for {module}")
        thresholds[module] = {"warning": float(warning), "critical": float(critical)}
    if set(thresholds) != set(MODULES):
        raise ValueError("Threshold configuration must define all eight modules")
    return thresholds


def validate_telemetry(payload: Any, feature_columns: list) -> Dict[str, Any]:
    """Reject malformed, missing, non-numeric and non-finite telemetry."""
    if not isinstance(payload, dict):
        raise ValueError("Telemetry payload must be a JSON object")
    missing = [name for name in feature_columns if name not in payload]
    if missing:
        raise ValueError(f"Missing {len(missing)} model features: {missing[:5]}")
    validated = {}
    for name in feature_columns:
        try:
            value = float(payload[name])
        except (TypeError, ValueError) as error:
            raise ValueError(f"Feature {name} is not numeric") from error
        if not np.isfinite(value):
            raise ValueError(f"Feature {name} must be finite")
        validated[name] = value
    return validated


class TelemetryWorker:
    def __init__(self) -> None:
        initialize_database()
        self.repository = SentinelRepository()
        self.model = joblib.load(BASE_DIR / "models" / "xgb_temperature_predictor.pkl")
        self.feature_columns = list(joblib.load(BASE_DIR / "models" / "feature_columns.pkl"))
        self.decision_engine = TemperatureDecisionEngine(
            load_thresholds(BASE_DIR / "models" / "temperature_thresholds.json")
        )
        self.alert_manager = AlertManager(
            BASE_DIR / "logs" / "alerts.jsonl",
            initial_states=self.repository.get_latest_risk_states(),
        )

    def process(self, message) -> None:
        payload = validate_telemetry(message.value, self.feature_columns)
        event_time = telemetry_timestamp(message.value)
        result = np.asarray(self.model.predict(np.array([[payload[f] for f in self.feature_columns]])))
        predictions = result.reshape(-1)
        if len(predictions) != len(MODULES):
            raise ValueError(f"Model returned {len(predictions)} predictions; expected {len(MODULES)}")

        for module, predicted in zip(MODULES, predictions):
            if self.repository.prediction_exists(message.partition, message.offset, module):
                continue
            decision = self.decision_engine.evaluate(module, float(predicted), payload[module])
            self.repository.save_prediction(PredictionRecord(
                timestamp=event_time, replay_id=message.value.get("_replay_id"),
                kafka_partition=message.partition, kafka_offset=message.offset,
                module=module, current_temperature=decision.get("current_temperature"),
                predicted_temperature=decision["predicted_temperature"],
                temperature_delta=decision.get("temperature_delta"),
                warning_threshold=decision["warning_threshold"],
                critical_threshold=decision["critical_threshold"], risk_level=decision["risk_level"],
            ))
            alert = self.alert_manager.process_decision(decision)
            if alert:
                self.repository.save_alert(AlertRecord(
                    timestamp=alert["timestamp"], module=module, alert_level=alert["severity"],
                    message=alert["message"], predicted_temperature=alert["predicted_temperature"],
                    current_temperature=alert["current_temperature"],
                    warning_threshold=alert["warning_threshold"], critical_threshold=alert["critical_threshold"],
                    replay_id=message.value.get("_replay_id"), kafka_partition=message.partition,
                    kafka_offset=message.offset,
                ))
        log.info("processed topic=%s partition=%s offset=%s", message.topic, message.partition, message.offset)

    def record_rejection(self, message, error: Exception) -> None:
        text = f"Rejected telemetry partition={message.partition} offset={message.offset}: {error}"
        log.warning(text)
        self.repository.save_system_event(SystemEvent(utc_now(), "TELEMETRY_REJECTED", "ERROR", text))

    def run(self, stop_event: threading.Event | None = None) -> None:
        self.repository.save_system_event(SystemEvent(utc_now(), "KAFKA_CONSUMER", "STARTED", "Telemetry worker started"))
        consumer = KafkaConsumer(
            TOPIC_NAME, bootstrap_servers=KAFKA_SERVER, group_id=CONSUMER_GROUP,
            auto_offset_reset="latest", enable_auto_commit=False,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            consumer_timeout_ms=1000, request_timeout_ms=30000,
        )
        try:
            while stop_event is None or not stop_event.is_set():
                for message in consumer:
                    if stop_event is not None and stop_event.is_set():
                        break
                    try:
                        self.process(message)
                    except ValueError as error:  # poison message: audit then advance
                        self.record_rejection(message, error)
                    except Exception:
                        log.exception("processing failed; offset will be replayed")
                        continue
                    consumer.commit()
        except KeyboardInterrupt:
            log.info("consumer stopped")
        finally:
            consumer.close()


if __name__ == "__main__":
    TelemetryWorker().run()
