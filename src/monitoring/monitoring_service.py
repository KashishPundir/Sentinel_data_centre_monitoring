from typing import Dict, Any, List

import pandas as pd

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

    # ========================================================
    # FORECAST PERFORMANCE
    # ========================================================

    def get_forecast_performance(
        self,
        horizon_seconds: int = 300,
        tolerance_seconds: int = 30,
    ) -> Dict[str, Any]:
        """
        Score how accurate the +horizon_seconds forecasts have been.

        A forecast made at time T is "evaluated" once a later reading
        exists close to T + horizon_seconds for the same module -
        that later reading is treated as the "actual" temperature and
        compared against what was predicted. Forecasts whose target
        time has not happened yet in the replay are "pending".
        """

        rows = (
            self.repository.get_predictions_for_evaluation(
                limit=20000
            )
        )

        empty_result = {
            "horizon_seconds": horizon_seconds,
            "overall": {
                "evaluated": 0,
                "pending": 0,
                "mae": None,
                "rmse": None,
            },
            "modules": {},
        }

        if not rows:
            return empty_result

        frame = pd.DataFrame(rows)

        frame["timestamp"] = pd.to_datetime(
            frame["timestamp"],
            errors="coerce",
        )

        frame = (
            frame
            .dropna(subset=["timestamp"])
            .sort_values("timestamp")
        )

        if frame.empty:
            return empty_result

        horizon = pd.Timedelta(seconds=horizon_seconds)
        tolerance = pd.Timedelta(seconds=tolerance_seconds)

        modules: Dict[str, Any] = {}

        all_errors: List[float] = []
        total_evaluated = 0
        total_pending = 0

        for module_name, group in frame.groupby("module"):

            group = (
                group
                .sort_values("timestamp")
                .reset_index(drop=True)
            )

            # Readings we can treat as "actual" values, keyed by time.
            actual_lookup = (
                group[["timestamp", "current_temperature"]]
                .dropna()
                .sort_values("timestamp")
                .rename(
                    columns={
                        "timestamp": "actual_timestamp",
                        "current_temperature": "actual_temperature",
                    }
                )
            )

            forecasts = (
                group
                .dropna(subset=["predicted_temperature"])
                .copy()
            )

            forecasts["target_time"] = (
                forecasts["timestamp"] + horizon
            )

            if actual_lookup.empty:

                evaluated_count = 0
                pending_count = len(forecasts)
                mae = None
                rmse = None

            else:

                matched = pd.merge_asof(
                    forecasts.sort_values("target_time"),
                    actual_lookup,
                    left_on="target_time",
                    right_on="actual_timestamp",
                    direction="nearest",
                    tolerance=tolerance,
                )

                evaluated_mask = (
                    matched["actual_temperature"].notna()
                )

                errors = (
                    matched.loc[
                        evaluated_mask,
                        "actual_temperature",
                    ]
                    - matched.loc[
                        evaluated_mask,
                        "predicted_temperature",
                    ]
                )

                evaluated_count = int(evaluated_mask.sum())
                pending_count = int((~evaluated_mask).sum())

                if len(errors):

                    mae = float(errors.abs().mean())

                    rmse = float(
                        (errors ** 2).mean() ** 0.5
                    )

                    all_errors.extend(errors.tolist())

                else:

                    mae = None
                    rmse = None

            modules[module_name] = {
                "evaluated": evaluated_count,
                "pending": pending_count,
                "mae": mae,
                "rmse": rmse,
            }

            total_evaluated += evaluated_count
            total_pending += pending_count

        if all_errors:

            errors_series = pd.Series(all_errors)

            overall_mae = float(
                errors_series.abs().mean()
            )

            overall_rmse = float(
                (errors_series ** 2).mean() ** 0.5
            )

        else:

            overall_mae = None
            overall_rmse = None

        return {
            "horizon_seconds": horizon_seconds,
            "overall": {
                "evaluated": total_evaluated,
                "pending": total_pending,
                "mae": overall_mae,
                "rmse": overall_rmse,
            },
            "modules": modules,
        }