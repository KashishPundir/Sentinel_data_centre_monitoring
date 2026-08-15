from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from .database import get_connection
from .models import (
    PredictionRecord,
    AlertRecord,
    SystemEvent,
)


class SentinelRepository:
    """
    Repository responsible for all SentinelDC database operations.

    The rest of the application should interact with this class
    instead of writing SQL directly.
    """

    # ========================================================
    # PREDICTIONS
    # ========================================================

    def prediction_exists(
        self,
        kafka_partition: Optional[int],
        kafka_offset: Optional[int],
        module: str,
    ) -> bool:
        """Return whether this Kafka record has already been persisted.

        Kafka delivery is at-least-once, so an offset may be replayed after a
        crash.  This check makes processing idempotent without assuming a
        particular database backend.
        """
        if kafka_partition is None or kafka_offset is None:
            return False
        connection = get_connection()
        try:
            row = connection.execute(
                """SELECT 1 FROM predictions
                   WHERE kafka_partition = ? AND kafka_offset = ? AND module = ?
                   LIMIT 1""",
                (kafka_partition, kafka_offset, module),
            ).fetchone()
            return row is not None
        finally:
            connection.close()

    def save_prediction(self, prediction: PredictionRecord):

        connection = get_connection()

        cursor = connection.cursor()

        cursor.execute(
            """
            INSERT INTO predictions (

                timestamp,
                replay_id,
                kafka_partition,
                kafka_offset,
                module,
                current_temperature,
                predicted_temperature,
                temperature_delta,
                warning_threshold,
                critical_threshold,
                risk_level

            )

            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                prediction.timestamp,
                prediction.replay_id,
                prediction.kafka_partition,
                prediction.kafka_offset,
                prediction.module,
                prediction.current_temperature,
                prediction.predicted_temperature,
                prediction.temperature_delta,
                prediction.warning_threshold,
                prediction.critical_threshold,
                prediction.risk_level,
            ),
        )

        connection.commit()

        connection.close()

    def get_latest_risk_states(self) -> Dict[str, str]:
        """Load last known module risk states for alert de-duplication."""
        connection = get_connection()
        try:
            rows = connection.execute(
                """SELECT p.module, p.risk_level FROM predictions p
                   INNER JOIN (
                       SELECT module, MAX(id) AS id FROM predictions GROUP BY module
                   ) latest ON latest.id = p.id"""
            ).fetchall()
            return {row["module"]: row["risk_level"] for row in rows}
        finally:
            connection.close()

    # ========================================================
    # ALERTS
    # ========================================================

    def save_alert(self, alert: AlertRecord):

        connection = get_connection()

        cursor = connection.cursor()

        cursor.execute(
            """
            INSERT INTO alerts (

                timestamp,
                replay_id,
                kafka_partition,
                kafka_offset,
                module,
                alert_level,
                predicted_temperature,
                current_temperature,
                warning_threshold,
                critical_threshold,
                message

            )

            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                alert.timestamp,
                alert.replay_id,
                alert.kafka_partition,
                alert.kafka_offset,
                alert.module,
                alert.alert_level,
                alert.predicted_temperature,
                alert.current_temperature,
                alert.warning_threshold,
                alert.critical_threshold,
                alert.message,
            ),
        )

        connection.commit()

        connection.close()

    # ========================================================
    # SYSTEM EVENTS
    # ========================================================

    def save_system_event(self, event: SystemEvent):

        connection = get_connection()

        cursor = connection.cursor()

        cursor.execute(
            """
            INSERT INTO system_events (

                timestamp,
                event_type,
                status,
                message

            )

            VALUES (?, ?, ?, ?)
            """,
            (
                event.timestamp,
                event.event_type,
                event.status,
                event.message,
            ),
        )

        connection.commit()

        connection.close()

    # ========================================================
    # LATEST PREDICTIONS
    # ========================================================

    def get_latest_predictions(
        self,
        limit: int = 100
    ) -> List[Dict[str, Any]]:

        connection = get_connection()

        cursor = connection.cursor()

        cursor.execute(
            """
            SELECT *
            FROM predictions
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        )

        rows = cursor.fetchall()

        connection.close()

        return [dict(row) for row in rows]

    # ========================================================
    # LATEST ALERTS
    # ========================================================

    def get_latest_alerts(
        self,
        limit: int = 100
    ) -> List[Dict[str, Any]]:

        connection = get_connection()

        cursor = connection.cursor()

        cursor.execute(
            """
            SELECT *
            FROM alerts
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        )

        rows = cursor.fetchall()

        connection.close()

        return [dict(row) for row in rows]

    # ========================================================
    # ALERT SUMMARY
    # ========================================================

    def get_alert_summary(self) -> Dict[str, int]:

        connection = get_connection()

        cursor = connection.cursor()

        cursor.execute(
            """
            SELECT
                COUNT(*) AS total_alerts,

                SUM(
                    CASE
                        WHEN alert_level = 'WARNING'
                        THEN 1
                        ELSE 0
                    END
                ) AS warning_alerts,

                SUM(
                    CASE
                        WHEN alert_level = 'CRITICAL'
                        THEN 1
                        ELSE 0
                    END
                ) AS critical_alerts

            FROM alerts
            """
        )

        row = cursor.fetchone()

        connection.close()

        return {
            "total_alerts": row["total_alerts"] or 0,
            "warning_alerts": row["warning_alerts"] or 0,
            "critical_alerts": row["critical_alerts"] or 0,
        }

    # ========================================================
    # PREDICTION COUNT
    # ========================================================

    def get_prediction_count(self) -> int:

        connection = get_connection()

        cursor = connection.cursor()

        cursor.execute(
            """
            SELECT COUNT(*) AS count
            FROM predictions
            """
        )

        row = cursor.fetchone()

        connection.close()

        return row["count"]

    # ========================================================
    # DATABASE HEALTH
    # ========================================================

    def health_check(self) -> bool:

        try:

            connection = get_connection()

            cursor = connection.cursor()

            cursor.execute("SELECT 1")

            cursor.fetchone()

            connection.close()

            return True

        except Exception:

            return False

            # ========================================================
    # SYSTEM EVENTS
    # ========================================================

    def get_system_events(
        self,
        limit: int = 50
    ):

        connection = get_connection()

        cursor = connection.cursor()

        cursor.execute(
            """
            SELECT *
            FROM system_events
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        )

        rows = cursor.fetchall()

        connection.close()

        return [
            dict(row)
            for row in rows
        ]

    def get_module_prediction_history(
        self,
        module: str,
        limit: int = 100
    ):
        """
        Return historical predictions for one module.

        Results are returned in chronological order.
        """

        limit = max(1, min(limit, 1000))

        connection = get_connection()
        cursor = connection.cursor()

        cursor.execute(
            """
            SELECT
                id,
                timestamp,
                module,
                current_temperature,
                predicted_temperature,
                temperature_delta,
                warning_threshold,
                critical_threshold,
                risk_level,
                replay_id,
                kafka_partition,
                kafka_offset
            FROM predictions
            WHERE module = ?
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (module, limit),
        )

        rows = cursor.fetchall()

        connection.close()

        records = [
            dict(row)
            for row in rows
        ]

        # Database gives newest first.
        # Dashboard needs chronological order.

        records.reverse()

        return records
