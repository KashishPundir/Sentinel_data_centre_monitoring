from typing import Dict, Any, List

from src.storage.repository import SentinelRepository


class MonitoringService:
    """
    Service layer for SentinelDC monitoring.

    This class sits between FastAPI and the database repository.

    Architecture:

        FastAPI
            ↓
        MonitoringService
            ↓
        SentinelRepository
            ↓
        SQLite
    """

    def __init__(self):

        self.repository = SentinelRepository()

    # ========================================================
    # HEALTH
    # ========================================================

    def get_health(self) -> Dict[str, Any]:

        database_healthy = self.repository.health_check()

        return {
            "status": "healthy" if database_healthy else "unhealthy",
            "database": (
                "healthy"
                if database_healthy
                else "unhealthy"
            )
        }

    # ========================================================
    # SUMMARY
    # ========================================================

    def get_summary(self) -> Dict[str, Any]:

        alert_summary = (
            self.repository.get_alert_summary()
        )

        prediction_count = (
            self.repository.get_prediction_count()
        )

        return {
            "total_predictions": prediction_count,

            "total_alerts": (
                alert_summary["total_alerts"]
            ),

            "warning_alerts": (
                alert_summary["warning_alerts"]
            ),

            "critical_alerts": (
                alert_summary["critical_alerts"]
            )
        }

    # ========================================================
    # RECENT PREDICTIONS
    # ========================================================

    def get_predictions(
        self,
        limit: int = 50
    ) -> List[Dict[str, Any]]:

        # Protect the API from unreasonable requests.

        limit = max(1, min(limit, 500))

        return self.repository.get_latest_predictions(
            limit
        )

    # ========================================================
    # RECENT ALERTS
    # ========================================================

    def get_alerts(
        self,
        limit: int = 50
    ) -> List[Dict[str, Any]]:

        limit = max(1, min(limit, 500))

        return self.repository.get_latest_alerts(
            limit
        )

    # ========================================================
    # MODULE STATUS
    # ========================================================

    def get_module_status(self) -> List[Dict[str, Any]]:

        predictions = (
            self.repository.get_latest_predictions(500)
        )

        latest_by_module = {}

        for prediction in predictions:

            module = prediction["module"]

            if module not in latest_by_module:

                latest_by_module[module] = prediction

        return list(
            latest_by_module.values()
        )

    # ========================================================
    # SYSTEM EVENTS
    # ========================================================

    def get_system_events(
        self,
        limit: int = 50
    ) -> List[Dict[str, Any]]:

        limit = max(1, min(limit, 500))

        # The current repository does not yet expose
        # system events, so query through a small extension
        # method that we will add next.

        return self.repository.get_system_events(
            limit
        )

    def get_module_history(
        self,
        module: str,
        limit: int = 100
    ):
        """
        Return historical prediction data
        for a specific module.
        """

        return self.repository.get_module_prediction_history(
            module=module,
            limit=limit,
        )