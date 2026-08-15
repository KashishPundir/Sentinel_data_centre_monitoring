"""
SentinelDC - Simulation Manager

Responsible for:

    1. Loading historical telemetry
    2. Replaying telemetry records
    3. Publishing telemetry to Kafka
    4. Start / pause / stop controls
    5. Dynamic replay speed
    6. Demo scenario injection
    7. Progress tracking
    8. Thread-safe state management
    9. Graceful Kafka producer lifecycle
    10. Robust error reporting

Pipeline:

    replay_data.csv
          |
          v
    SimulationManager
          |
          | scenario injection
          v
    Kafka Producer
          |
          v
       Kafka
          |
          v
    ML Inference
          |
          v
    Decision Engine
          |
          v
       Alerts
"""


# ================================================================
# IMPORTS
# ================================================================

import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
from kafka import KafkaProducer


# ================================================================
# SIMULATION MANAGER
# ================================================================


class SimulationManager:
    """
    Controls SentinelDC telemetry replay.

    The manager runs the replay in a background thread so that
    FastAPI remains responsive while telemetry is being published.

    Supported controls:

        start()
        pause()
        stop()
        set_speed()
        set_scenario()
        get_status()

    Supported scenarios:

        NORMAL
            Original telemetry.

        WARNING
            Moderate temperature stress injected into Module 6.

        CRITICAL
            Strong temperature stress injected into Module 6.
    """

    # ------------------------------------------------------------
    # CONSTANTS
    # ------------------------------------------------------------

    DEFAULT_SPEED = 1.0

    MIN_SPEED = 0.1
    MAX_SPEED = 100.0

    DEFAULT_SCENARIO = "NORMAL"

    ALLOWED_SCENARIOS = {
        "NORMAL",
        "WARNING",
        "CRITICAL",
    }

    # These values are intentionally conservative.
    #
    # IMPORTANT:
    # They modify telemetry only.
    #
    # They do NOT directly set the risk level.
    #
    # XGBoost + Decision Engine still determine the actual risk.
    SCENARIO_INJECTIONS = {
        "NORMAL": 0.0,
        "WARNING": 1.0,
        "CRITICAL": 3.0,
    }

    MODULE_6_COLUMN = "Module_6_Avg_Temp"

    # ------------------------------------------------------------
    # CONSTRUCTOR
    # ------------------------------------------------------------

    def __init__(
        self,
        data_path: str = "data/processed/replay_data.csv",
        kafka_server: str = "127.0.0.1:9092",
        topic_name: str = "datacenter_telemetry_v2",
    ):
        """
        Initialize the simulation manager.

        Parameters
        ----------
        data_path:
            Path to replay CSV.

        kafka_server:
            Kafka bootstrap server.

        topic_name:
            Kafka topic used for telemetry.
        """

        # --------------------------------------------------------
        # PATH CONFIGURATION
        # --------------------------------------------------------

        self.data_path = self._resolve_data_path(
            data_path
        )

        # --------------------------------------------------------
        # KAFKA CONFIGURATION
        # --------------------------------------------------------

        self.kafka_server = kafka_server

        self.topic_name = topic_name

        # --------------------------------------------------------
        # SIMULATION STATE
        # --------------------------------------------------------

        self.status = "STOPPED"

        self.speed = self.DEFAULT_SPEED

        self.scenario = self.DEFAULT_SCENARIO

        self.current_index = 0

        self.total_records = 0

        self.records_sent = 0

        self.started_at: Optional[str] = None

        self.last_error: Optional[str] = None

        # --------------------------------------------------------
        # THREAD CONTROL
        # --------------------------------------------------------

        self._thread: Optional[
            threading.Thread
        ] = None

        self._stop_event = threading.Event()

        self._pause_event = threading.Event()

        # Used to wake the replay thread when speed/scenario/state
        # changes.
        self._wake_event = threading.Event()

        # --------------------------------------------------------
        # THREAD SAFETY
        # --------------------------------------------------------

        # RLock is intentional.
        #
        # The old implementation used threading.Lock() and then
        # called get_status() while already holding that lock.
        #
        # That can deadlock.
        #
        # RLock safely allows nested access.
        self._lock = threading.RLock()

        # --------------------------------------------------------
        # DATA
        # --------------------------------------------------------

        self.df: Optional[pd.DataFrame] = None

        # --------------------------------------------------------
        # KAFKA PRODUCER
        # --------------------------------------------------------

        self.producer: Optional[
            KafkaProducer
        ] = None

    # ============================================================
    # PATH MANAGEMENT
    # ============================================================

    @staticmethod
    def _resolve_data_path(
        data_path: str,
    ) -> Path:
        """
        Resolve replay dataset path robustly.

        The original implementation used:

            Path("data/processed/replay_data.csv")

        directly.

        That depends on the current working directory.

        This version first checks the supplied path and then tries
        to resolve it relative to the project root.
        """

        path = Path(data_path)

        # --------------------------------------------------------
        # ABSOLUTE PATH
        # --------------------------------------------------------

        if path.is_absolute():

            return path.resolve()

        # --------------------------------------------------------
        # CURRENT WORKING DIRECTORY
        # --------------------------------------------------------

        cwd_path = (
            Path.cwd() / path
        ).resolve()

        if cwd_path.exists():

            return cwd_path

        # --------------------------------------------------------
        # PROJECT ROOT
        # --------------------------------------------------------

        # simulation_manager.py is expected at:
        #
        # project/
        #   src/
        #       simulation/
        #           simulation_manager.py
        #
        # Therefore:
        #
        # parents[2] = project root

        project_root = (
            Path(__file__)
            .resolve()
            .parents[2]
        )

        project_path = (
            project_root / path
        ).resolve()

        return project_path

    # ============================================================
    # TIME
    # ============================================================

    @staticmethod
    def _utc_now() -> str:
        """
        Return current UTC timestamp in ISO format.
        """

        return (
            datetime.now(
                timezone.utc
            )
            .strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        )

    # ============================================================
    # LOAD DATA
    # ============================================================

    def _load_data(self) -> None:
        """
        Load historical replay telemetry.

        Target columns are removed so that future/target values
        are never leaked into the streaming pipeline.
        """

        # --------------------------------------------------------
        # VALIDATE FILE
        # --------------------------------------------------------

        if not self.data_path.exists():

            raise FileNotFoundError(
                "Replay dataset not found: "
                f"{self.data_path}"
            )

        # --------------------------------------------------------
        # READ CSV
        # --------------------------------------------------------

        df = pd.read_csv(
            self.data_path
        )

        # --------------------------------------------------------
        # EMPTY DATASET
        # --------------------------------------------------------

        if df.empty:

            raise ValueError(
                "Replay dataset is empty."
            )

        # --------------------------------------------------------
        # REMOVE TARGET COLUMNS
        # --------------------------------------------------------

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

        # --------------------------------------------------------
        # FINAL VALIDATION
        # --------------------------------------------------------

        if df.empty:

            raise ValueError(
                "Replay dataset contains no "
                "usable telemetry after removing "
                "target columns."
            )

        # --------------------------------------------------------
        # STORE
        # --------------------------------------------------------

        with self._lock:

            self.df = df

            self.total_records = len(df)

    # ============================================================
    # JSON SERIALIZATION
    # ============================================================

    @staticmethod
    def _make_json_safe(
        value: Any,
    ) -> Any:
        """
        Convert pandas / NumPy values into JSON-safe values.

        KafkaProducer's JSON serializer ultimately calls
        json.dumps(), so objects such as numpy integers,
        timestamps, NaN and pandas scalar types need handling.
        """

        # --------------------------------------------------------
        # NONE
        # --------------------------------------------------------

        if value is None:

            return None

        # --------------------------------------------------------
        # FLOAT NaN
        # --------------------------------------------------------

        try:

            if pd.isna(value):

                return None

        except (
            TypeError,
            ValueError,
        ):

            pass

        # --------------------------------------------------------
        # PANDAS TIMESTAMP
        # --------------------------------------------------------

        if isinstance(
            value,
            pd.Timestamp,
        ):

            return value.isoformat()

        # --------------------------------------------------------
        # PYTHON DATETIME
        # --------------------------------------------------------

        if isinstance(
            value,
            datetime,
        ):

            return value.isoformat()

        # --------------------------------------------------------
        # NUMPY / PANDAS SCALAR
        # --------------------------------------------------------

        if hasattr(
            value,
            "item",
        ):

            try:

                return value.item()

            except (
                ValueError,
                TypeError,
            ):

                pass

        # --------------------------------------------------------
        # STRING / NUMBER / BOOLEAN
        # --------------------------------------------------------

        if isinstance(
            value,
            (
                str,
                int,
                float,
                bool,
            ),
        ):

            return value

        # --------------------------------------------------------
        # FALLBACK
        # --------------------------------------------------------

        return str(value)

    # ============================================================
    # PREPARE MESSAGE
    # ============================================================

    def _prepare_message(
        self,
        row: pd.Series,
        replay_id: int,
    ) -> Dict[str, Any]:
        """
        Convert a dataframe row into a Kafka-safe dictionary.
        """

        raw_message = row.to_dict()

        message: Dict[
            str,
            Any
        ] = {}

        for key, value in raw_message.items():

            message[str(key)] = (
                self._make_json_safe(
                    value
                )
            )

        # --------------------------------------------------------
        # REPLAY ID
        # --------------------------------------------------------

        message["_replay_id"] = int(
            replay_id
        )

        return message

    # ============================================================
    # SCENARIO INJECTION
    # ============================================================

    def _apply_scenario(
        self,
        message: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Apply controlled thermal stress to Module 6.

        IMPORTANT:

        This method changes only telemetry.

        It does NOT:

            - assign risk level
            - generate alerts
            - modify predictions
            - bypass XGBoost
            - bypass Decision Engine

        Therefore the actual downstream ML pipeline remains
        responsible for determining the final risk.
        """

        with self._lock:

            scenario = self.scenario

        # --------------------------------------------------------
        # NORMAL
        # --------------------------------------------------------

        if scenario == "NORMAL":

            return message

        # --------------------------------------------------------
        # MODULE 6 COLUMN CHECK
        # --------------------------------------------------------

        if (
            self.MODULE_6_COLUMN
            not in message
        ):

            # Do not crash the entire simulation merely because
            # the expected demo column is absent.
            return message

        # --------------------------------------------------------
        # GET CURRENT TEMPERATURE
        # --------------------------------------------------------

        try:

            current_temperature = float(
                message[
                    self.MODULE_6_COLUMN
                ]
            )

        except (
            TypeError,
            ValueError,
        ):

            return message

        # --------------------------------------------------------
        # GET INJECTION
        # --------------------------------------------------------

        injection = self.SCENARIO_INJECTIONS.get(
            scenario,
            0.0,
        )

        # --------------------------------------------------------
        # MODIFY TEMPERATURE
        # --------------------------------------------------------

        message[
            self.MODULE_6_COLUMN
        ] = round(
            current_temperature
            + injection,
            4,
        )

        return message

    # ============================================================
    # CREATE KAFKA PRODUCER
    # ============================================================

    def _create_producer(self) -> KafkaProducer:
        """
        Create Kafka producer.

        Deliberately uses a conservative configuration.

        We do NOT pass questionable configuration names such as:

            api_version_auto_timeout_ms
            buffer_memory

        because your current environment previously reported them
        as unrecognized configurations.

        kafka-python will use its supported defaults.
        """

        producer = KafkaProducer(
            bootstrap_servers=self.kafka_server,

            value_serializer=lambda value:
                json.dumps(
                    value,
                    allow_nan=False,
                ).encode("utf-8"),

            # Safe standard kafka-python settings.
            acks="all",

            retries=3,

            linger_ms=5,

            request_timeout_ms=30000,

            max_block_ms=30000,
        )

        return producer

    # ============================================================
    # PRODUCER CLOSE
    # ============================================================

    def _close_producer(self) -> None:
        """
        Safely close Kafka producer.
        """

        producer = None

        with self._lock:

            producer = self.producer

            self.producer = None

        if producer is None:

            return

        try:

            producer.flush(
                timeout=10
            )

        except Exception:

            pass

        try:

            producer.close(
                timeout=10
            )

        except Exception:

            pass

    # ============================================================
    # START
    # ============================================================

    def start(
        self,
        speed: float = 1.0,
    ) -> Dict[str, Any]:
        """
        Start or resume the simulation.
        """

        # --------------------------------------------------------
        # VALIDATE SPEED
        # --------------------------------------------------------

        speed = self._validate_speed(
            speed
        )

        with self._lock:

            # ----------------------------------------------------
            # ALREADY RUNNING
            # ----------------------------------------------------

            if self.status == "RUNNING":

                self.speed = speed

                self._wake_event.set()

                return self._get_status_unlocked()

            # ----------------------------------------------------
            # PAUSED -> RESUME
            # ----------------------------------------------------

            if self.status == "PAUSED":

                self.speed = speed

                self._pause_event.clear()

                self.status = "RUNNING"

                self._wake_event.set()

                return self._get_status_unlocked()

            # ----------------------------------------------------
            # NEW START / RESTART
            # ----------------------------------------------------

            self.status = "STARTING"

            self.speed = speed

            self.current_index = 0

            self.records_sent = 0

            self.started_at = (
                self._utc_now()
            )

            self.last_error = None

            self._stop_event.clear()

            self._pause_event.clear()

            self._wake_event.clear()

        # --------------------------------------------------------
        # LOAD DATA
        # --------------------------------------------------------

        try:

            self._load_data()

        except Exception as error:

            with self._lock:

                self.status = "ERROR"

                self.last_error = (
                    f"{type(error).__name__}: "
                    f"{error}"
                )

            return self.get_status()

        # --------------------------------------------------------
        # CREATE KAFKA PRODUCER
        # --------------------------------------------------------

        try:

            producer = (
                self._create_producer()
            )

            with self._lock:

                self.producer = producer

        except Exception as error:

            with self._lock:

                self.status = "ERROR"

                self.last_error = (
                    f"{type(error).__name__}: "
                    f"{error}"
                )

            return self.get_status()

        # --------------------------------------------------------
        # START BACKGROUND THREAD
        # --------------------------------------------------------

        with self._lock:

            self.status = "RUNNING"

            self._thread = threading.Thread(
                target=self._run,
                daemon=True,
                name="sentineldc-simulation",
            )

            self._thread.start()

            return self._get_status_unlocked()

    # ============================================================
    # RUN SIMULATION
    # ============================================================

    def _run(self) -> None:
        """
        Background telemetry replay loop.
        """

        try:

            while not self._stop_event.is_set():

                # ------------------------------------------------
                # HANDLE PAUSE
                # ------------------------------------------------

                while (
                    self._pause_event.is_set()
                    and not self._stop_event.is_set()
                ):

                    self._wake_event.wait(
                        timeout=0.5
                    )

                    self._wake_event.clear()

                # ------------------------------------------------
                # STOP CHECK
                # ------------------------------------------------

                if self._stop_event.is_set():

                    break

                # ------------------------------------------------
                # DATASET END
                # ------------------------------------------------

                with self._lock:

                    if (
                        self.df is None
                        or self.current_index
                        >= self.total_records
                    ):

                        self.status = (
                            "COMPLETED"
                        )

                        break

                    row_index = (
                        self.current_index
                    )

                    df = self.df

                # ------------------------------------------------
                # GET ROW
                # ------------------------------------------------

                row = df.iloc[
                    row_index
                ]

                # ------------------------------------------------
                # CREATE MESSAGE
                # ------------------------------------------------

                message = (
                    self._prepare_message(
                        row=row,
                        replay_id=row_index,
                    )
                )

                # ------------------------------------------------
                # APPLY DEMO SCENARIO
                # ------------------------------------------------

                message = (
                    self._apply_scenario(
                        message
                    )
                )

                # ------------------------------------------------
                # GET PRODUCER
                # ------------------------------------------------

                with self._lock:

                    producer = (
                        self.producer
                    )

                if producer is None:

                    raise RuntimeError(
                        "Kafka producer is not available."
                    )

                # ------------------------------------------------
                # SEND TO KAFKA
                # ------------------------------------------------

                future = producer.send(
                    self.topic_name,
                    value=message,
                )

                # ------------------------------------------------
                # WAIT FOR ACK
                # ------------------------------------------------

                future.get(
                    timeout=10
                )

                # ------------------------------------------------
                # UPDATE PROGRESS
                # ------------------------------------------------

                with self._lock:

                    self.current_index += 1

                    self.records_sent += 1

                # ------------------------------------------------
                # CALCULATE DELAY
                # ------------------------------------------------

                with self._lock:

                    current_speed = (
                        self.speed
                    )

                delay = (
                    1.0
                    / max(
                        current_speed,
                        self.MIN_SPEED,
                    )
                )

                # ------------------------------------------------
                # INTERRUPTIBLE DELAY
                # ------------------------------------------------

                # Instead of time.sleep(), use Event.wait().
                #
                # This means a speed/pause/stop change can wake the
                # simulation immediately.

                self._wake_event.wait(
                    timeout=delay
                )

                self._wake_event.clear()

        except Exception as error:

            # ----------------------------------------------------
            # RECORD ERROR
            # ----------------------------------------------------

            with self._lock:

                # If stop() caused the thread to wake up, don't
                # incorrectly report an error.
                if not self._stop_event.is_set():

                    self.status = "ERROR"

                    self.last_error = (
                        f"{type(error).__name__}: "
                        f"{error}"
                    )

        finally:

            # ----------------------------------------------------
            # NATURAL COMPLETION
            # ----------------------------------------------------

            with self._lock:

                completed = (
                    self.status
                    == "COMPLETED"
                )

            # ----------------------------------------------------
            # CLOSE PRODUCER
            # ----------------------------------------------------

            if completed:

                self._close_producer()

    # ============================================================
    # PAUSE
    # ============================================================

    def pause(self) -> Dict[str, Any]:
        """
        Pause the simulation without losing current position.
        """

        with self._lock:

            if self.status != "RUNNING":

                return (
                    self._get_status_unlocked()
                )

            self._pause_event.set()

            self._wake_event.set()

            self.status = "PAUSED"

            return (
                self._get_status_unlocked()
            )

    # ============================================================
    # STOP
    # ============================================================

    def stop(self) -> Dict[str, Any]:
        """
        Stop the simulation.

        The next start begins a fresh replay from record 0.
        """

        with self._lock:

            self._stop_event.set()

            self._pause_event.clear()

            self._wake_event.set()

            self.status = "STOPPED"

        # --------------------------------------------------------
        # WAIT FOR THREAD
        # --------------------------------------------------------

        thread = None

        with self._lock:

            thread = self._thread

        if (
            thread is not None
            and thread.is_alive()
            and thread is not threading.current_thread()
        ):

            thread.join(
                timeout=5
            )

        # --------------------------------------------------------
        # CLOSE PRODUCER
        # --------------------------------------------------------

        self._close_producer()

        # --------------------------------------------------------
        # RESET POSITION
        # --------------------------------------------------------

        with self._lock:

            self.current_index = 0

            self.records_sent = 0

            self.started_at = None

            self.last_error = None

            self._thread = None

            self.scenario = (
                self.DEFAULT_SCENARIO
            )

        return self.get_status()

    # ============================================================
    # SET SPEED
    # ============================================================

    def set_speed(
        self,
        speed: float,
    ) -> Dict[str, Any]:
        """
        Change replay speed dynamically.

        Examples:

            1x  -> 1 second/record
            2x  -> 0.5 seconds/record
            5x  -> 0.2 seconds/record
            10x -> 0.1 seconds/record
            20x -> 0.05 seconds/record
        """

        speed = self._validate_speed(
            speed
        )

        with self._lock:

            self.speed = speed

            self._wake_event.set()

            return (
                self._get_status_unlocked()
            )

    # ============================================================
    # SPEED VALIDATION
    # ============================================================

    @classmethod
    def _validate_speed(
        cls,
        speed: float,
    ) -> float:
        """
        Validate and clamp simulation speed.
        """

        try:

            speed = float(speed)

        except (
            TypeError,
            ValueError,
        ) as error:

            raise ValueError(
                "Simulation speed must be numeric."
            ) from error

        if speed <= 0:

            raise ValueError(
                "Simulation speed must be greater than 0."
            )

        return min(
            max(
                speed,
                cls.MIN_SPEED,
            ),
            cls.MAX_SPEED,
        )

    # ============================================================
    # SET SCENARIO
    # ============================================================

    def set_scenario(
        self,
        scenario: str,
    ) -> Dict[str, Any]:
        """
        Change the active demo scenario.

        Supported values:

            NORMAL
            WARNING
            CRITICAL

        The scenario can be changed while the simulation is
        running. The next telemetry record will use the new
        scenario.
        """

        if not isinstance(
            scenario,
            str,
        ):

            raise ValueError(
                "Scenario must be a string."
            )

        scenario = (
            scenario
            .strip()
            .upper()
        )

        if (
            scenario
            not in self.ALLOWED_SCENARIOS
        ):

            allowed = ", ".join(
                sorted(
                    self.ALLOWED_SCENARIOS
                )
            )

            raise ValueError(
                "Invalid scenario. "
                f"Allowed values: {allowed}"
            )

        with self._lock:

            self.scenario = scenario

            self._wake_event.set()

            return (
                self._get_status_unlocked()
            )

    # ============================================================
    # GET SCENARIO
    # ============================================================

    def get_scenario(self) -> str:
        """
        Return currently active scenario.
        """

        with self._lock:

            return self.scenario

    # ============================================================
    # STATUS
    # ============================================================

    def get_status(self) -> Dict[str, Any]:
        """
        Return thread-safe simulation status.
        """

        with self._lock:

            return (
                self._get_status_unlocked()
            )

    # ============================================================
    # INTERNAL STATUS
    # ============================================================

    def _get_status_unlocked(
        self,
    ) -> Dict[str, Any]:
        """
        Internal status method.

        IMPORTANT:

        Caller must already hold self._lock.
        """

        # --------------------------------------------------------
        # PROGRESS
        # --------------------------------------------------------

        if self.total_records > 0:

            progress = (
                self.records_sent
                / self.total_records
            ) * 100

        else:

            progress = 0.0

        # --------------------------------------------------------
        # THREAD STATUS
        # --------------------------------------------------------

        thread_alive = (
            self._thread is not None
            and self._thread.is_alive()
        )

        # --------------------------------------------------------
        # RETURN
        # --------------------------------------------------------

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

            "progress_percent": round(
                progress,
                2,
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

            "data_path": str(
                self.data_path
            ),

            "thread_alive": (
                thread_alive
            ),

            "last_error": (
                self.last_error
            ),
        }


# ================================================================
# SINGLE APPLICATION INSTANCE
# ================================================================

simulation_manager = (
    SimulationManager()
)