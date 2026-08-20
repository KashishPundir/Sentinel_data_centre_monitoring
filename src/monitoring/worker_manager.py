"""Lifecycle management for the in-process SentinelDC telemetry worker."""

import threading
from datetime import datetime, timezone
from typing import Any, Dict, Optional


class TelemetryWorkerManager:
    """Run one Kafka consumer alongside the API and expose its real status."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._status = "STOPPED"
        self._last_error: Optional[str] = None
        self._started_at: Optional[str] = None

    def ensure_running(self) -> Dict[str, Any]:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return self._status_unlocked()
            self._stop_event = threading.Event()
            self._status = "STARTING"
            self._last_error = None
            self._started_at = datetime.now(timezone.utc).isoformat()
            self._thread = threading.Thread(target=self._run, daemon=True, name="sentineldc-telemetry-worker")
            self._thread.start()
            return self._status_unlocked()

    def _run(self) -> None:
        # Docker can expose Kafka's port a few seconds before the broker is
        # ready for client bootstrap.  Keep retrying so a dashboard opened at
        # that moment heals itself without requiring a manual consumer start.
        while not self._stop_event.is_set():
            try:
                # Delayed import avoids loading the model while FastAPI imports.
                from replay.kafka_consumer import TelemetryWorker
                with self._lock:
                    self._status = "RUNNING"
                    self._last_error = None
                TelemetryWorker().run(stop_event=self._stop_event)
                if self._stop_event.is_set():
                    break
                raise RuntimeError("Telemetry worker stopped unexpectedly")
            except Exception as error:
                with self._lock:
                    self._status = "ERROR"
                    self._last_error = f"{type(error).__name__}: {error}"
                self._stop_event.wait(timeout=2)

        with self._lock:
            self._status = "STOPPED"

    def stop(self) -> None:
        with self._lock:
            self._stop_event.set()

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            return self._status_unlocked()

    def _status_unlocked(self) -> Dict[str, Any]:
        alive = self._thread is not None and self._thread.is_alive()
        return {"status": self._status, "healthy": self._status == "RUNNING" and alive,
                "thread_alive": alive, "started_at": self._started_at, "last_error": self._last_error}


telemetry_worker_manager = TelemetryWorkerManager()
