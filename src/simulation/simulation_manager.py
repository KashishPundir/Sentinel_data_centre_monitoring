import json
import threading
import time
from pathlib import Path
from typing import Optional

import pandas as pd
from kafka import KafkaProducer


class SimulationManager:
    """
    Controls SentinelDC telemetry replay.

    Responsibilities:
        - Load historical telemetry
        - Publish records to Kafka
        - Start / pause / stop simulation
        - Control replay speed
        - Track simulation progress
        - Inject controlled demo scenarios

    Architecture:

        SimulationManager
                |
                v
          Kafka Producer
                |
                v
              Kafka
    """

    ALLOWED_SCENARIOS = {
        "NORMAL",
        "WARNING",
        "CRITICAL",
    }

    SCENARIO_DELTAS = {
        "NORMAL": 0.0,
        "WARNING": 5.0,
        "CRITICAL": 10.0,
    }

    def __init__(
        self,
        data_path: str = "data/processed/replay_data.csv",
        kafka_server: str = "127.0.0.1:9092",
        topic_name: str = "datacenter_telemetry_v2",
    ):

        self.data_path = Path(data_path)

        self.kafka_server = kafka_server

        self.topic_name = topic_name

        # ========================================================
        # STATE
        # ========================================================

        self.status = "STOPPED"

        self.speed = 1.0

        self.current_index = 0

        self.total_records = 0

        self.records_sent = 0

        self.started_at: Optional[str] = None

        self.last_error: Optional[str] = None

        self.scenario = "NORMAL"

        # ========================================================
        # THREAD CONTROL
        # ========================================================

        self._thread = None

        self._stop_event = threading.Event()

        self._pause_event = threading.Event()

        # ========================================================
        # THREAD SAFETY
        # ========================================================

        self._lock = threading.Lock()

        # ========================================================
        # DATA
        # ========================================================

        self.df = None

        # ========================================================
        # PRODUCER
        # ========================================================

        self.producer = None

    # ========================================================
    # LOAD DATA
    # ========================================================

    def _load_data(self):

        if not self.data_path.exists():

            raise FileNotFoundError(
                f"Replay dataset not found: "
                f"{self.data_path}"
            )

        df = pd.read_csv(
            self.data_path
        )

        if df.empty:

            raise ValueError(
                "Replay dataset is empty."
            )

        # ----------------------------------------------------
        # REMOVE TARGET COLUMNS
        # ----------------------------------------------------

        target_columns = [
            f"Target_Module_{i}"
            for i in range(1, 9)
        ]

        target_columns = [
            column
            for column in target_columns
            if column in df.columns
        ]

        if target_columns:

            df = df.drop(
                columns=target_columns
            )

        self.df = df

        self.total_records = len(df)

    # ========================================================
    # CREATE PRODUCER
    # ========================================================

    def _create_producer(self):

        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_server,

            value_serializer=lambda value:
                json.dumps(value).encode("utf-8"),
        )

    # ========================================================
    # START
    # ========================================================

    def start(
        self,
        speed: float = 1.0,
    ):

        with self._lock:

            # ------------------------------------------------
            # ALREADY RUNNING
            # ------------------------------------------------

            if self.status == "RUNNING":

                return self.get_status()

            # ------------------------------------------------
            # RESUME FROM PAUSED
            # ------------------------------------------------

            if self.status == "PAUSED":

                self.speed = max(
                    0.1,
                    float(speed),
                )

                self._pause_event.clear()

                self.status = "RUNNING"

                return self.get_status()

            # ------------------------------------------------
            # FIRST START / RESTART
            # ------------------------------------------------

            self._load_data()

            self._create_producer()

            self.speed = max(
                0.1,
                float(speed),
            )

            self.current_index = 0

            self.records_sent = 0

            self.last_error = None

            self._stop_event.clear()

            self._pause_event.clear()

            self.status = "RUNNING"

            self.started_at = (
                time.strftime(
                    "%Y-%m-%dT%H:%M:%SZ",
                    time.gmtime(),
                )
            )

            self._thread = threading.Thread(
                target=self._run,
                daemon=True,
                name="sentineldc-simulation",
            )

            self._thread.start()

            return self.get_status()

    # ========================================================
    # RUN SIMULATION
    # ========================================================

    def _run(self):

        try:

            while not self._stop_event.is_set():

                # --------------------------------------------
                # PAUSE
                # --------------------------------------------

                while (
                    self._pause_event.is_set()
                    and not self._stop_event.is_set()
                ):

                    time.sleep(0.2)

                if self._stop_event.is_set():

                    break

                # --------------------------------------------
                # END OF DATASET
                # --------------------------------------------

                if (
                    self.df is None
                    or self.current_index
                    >= self.total_records
                ):

                    with self._lock:

                        self.status = "COMPLETED"

                    break

                # --------------------------------------------
                # GET RECORD
                # --------------------------------------------

                row = self.df.iloc[
                    self.current_index
                ]

                message = row.to_dict()

                message["_replay_id"] = int(
                    self.current_index
                )

                # --------------------------------------------
                # GET CURRENT SCENARIO
                # --------------------------------------------

                with self._lock:

                    scenario = self.scenario

                # --------------------------------------------
                # APPLY DEMO SCENARIO
                # --------------------------------------------

                message = self._apply_scenario(
                    message,
                    scenario,
                )

                # --------------------------------------------
                # SEND TO KAFKA
                # --------------------------------------------

                future = self.producer.send(
                    self.topic_name,
                    value=message,
                )

                # Wait for Kafka acknowledgement.

                future.get(
                    timeout=10
                )

                # --------------------------------------------
                # UPDATE STATE
                # --------------------------------------------

                with self._lock:

                    self.current_index += 1

                    self.records_sent += 1

                # --------------------------------------------
                # REPLAY DELAY
                # --------------------------------------------

                delay = 1.0 / self.speed

                time.sleep(delay)

        except Exception as error:

            with self._lock:

                self.status = "ERROR"

                self.last_error = str(
                    error
                )

    # ========================================================
    # APPLY DEMO SCENARIO
    # ========================================================

    def _apply_scenario(
        self,
        message: dict,
        scenario: str,
    ) -> dict:
        """
        Inject a controlled temperature shift into Module 6.

        NORMAL:
            No modification.

        WARNING:
            +5°C.

        CRITICAL:
            +10°C.

        We modify Module_6_Avg_Temp and its lag features
        together so the feature vector remains internally
        more consistent than changing only one column.
        """

        delta = self.SCENARIO_DELTAS.get(
            scenario,
            0.0,
        )

        if delta == 0.0:

            return message

        # --------------------------------------------
        # Module 6 average temperature
        # --------------------------------------------

        main_column = (
            "Module_6_Avg_Temp"
        )

        if main_column in message:

            message[main_column] = (
                float(message[main_column])
                + delta
            )

        # --------------------------------------------
        # Module 6 lag features
        # --------------------------------------------

        lag_prefix = (
            "Module_6_Avg_Temp_Lag_"
        )

        for column in list(message.keys()):

            if column.startswith(lag_prefix):

                try:

                    message[column] = (
                        float(message[column])
                        + delta
                    )

                except (
                    TypeError,
                    ValueError,
                ):

                    pass

        return message

    # ========================================================
    # SET SCENARIO
    # ========================================================

    def set_scenario(
        self,
        scenario: str,
    ):

        scenario = (
            scenario
            .upper()
            .strip()
        )

        if scenario not in self.ALLOWED_SCENARIOS:

            raise ValueError(
                f"Invalid scenario '{scenario}'. "
                f"Allowed values: "
                f"{sorted(self.ALLOWED_SCENARIOS)}"
            )

        with self._lock:

            self.scenario = scenario

        return self.get_status()

    # ========================================================
    # PAUSE
    # ========================================================

    def pause(self):

        with self._lock:

            if self.status != "RUNNING":

                return self.get_status()

            self._pause_event.set()

            self.status = "PAUSED"

            return self.get_status()

    # ========================================================
    # STOP
    # ========================================================

    def stop(self):

        with self._lock:

            self._stop_event.set()

            self._pause_event.clear()

            self.status = "STOPPED"

        # --------------------------------------------
        # WAIT FOR THREAD
        # --------------------------------------------

        if (
            self._thread is not None
            and self._thread.is_alive()
        ):

            self._thread.join(
                timeout=5
            )

        # --------------------------------------------
        # CLOSE PRODUCER
        # --------------------------------------------

        if self.producer is not None:

            try:

                self.producer.flush()

                self.producer.close()

            except Exception:

                pass

            finally:

                self.producer = None

        return self.get_status()

    # ========================================================
    # CHANGE SPEED
    # ========================================================

    def set_speed(
        self,
        speed: float,
    ):

        speed = max(
            0.1,
            float(speed),
        )

        with self._lock:

            self.speed = speed

        return self.get_status()

    # ========================================================
    # STATUS
    # ========================================================

    def get_status(self):

        with self._lock:

            return {

                "status": self.status,

                "speed": self.speed,

                "scenario": self.scenario,

                "current_index": (
                    self.current_index
                ),

                "records_sent": (
                    self.records_sent
                ),

                "total_records": (
                    self.total_records
                ),

                "progress_percent": (
                    round(
                        (
                            self.records_sent
                            / self.total_records
                        ) * 100,
                        2,
                    )
                    if self.total_records
                    else 0.0
                ),

                "started_at": (
                    self.started_at
                ),

                "topic": (
                    self.topic_name
                ),

                "kafka_server": (
                    self.kafka_server
                ),

                "last_error": (
                    self.last_error
                ),
            }


# ============================================================
# SINGLE APPLICATION INSTANCE
# ============================================================

simulation_manager = SimulationManager()