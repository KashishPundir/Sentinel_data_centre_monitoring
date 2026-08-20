from dataclasses import dataclass
from typing import Optional


@dataclass
class PredictionRecord:
    """
    Represents one ML prediction for one datacenter module.
    """

    timestamp: str

    module: str

    predicted_temperature: float

    risk_level: str

    current_temperature: Optional[float] = None

    temperature_delta: Optional[float] = None

    warning_threshold: Optional[float] = None

    critical_threshold: Optional[float] = None

    replay_id: Optional[int] = None

    kafka_partition: Optional[int] = None

    kafka_offset: Optional[int] = None


@dataclass
class AlertRecord:
    """
    Represents one SentinelDC alert.
    """

    timestamp: str

    module: str

    alert_level: str

    message: str

    predicted_temperature: Optional[float] = None

    current_temperature: Optional[float] = None

    warning_threshold: Optional[float] = None

    critical_threshold: Optional[float] = None

    replay_id: Optional[int] = None

    kafka_partition: Optional[int] = None

    kafka_offset: Optional[int] = None


@dataclass
class SystemEvent:
    """
    Represents a system-level event.
    """

    timestamp: str

    event_type: str

    status: Optional[str] = None

    message: Optional[str] = None