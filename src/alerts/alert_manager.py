import json
from datetime import datetime, timezone
from pathlib import Path


class AlertManager:

    """
    Prevents repeated alerts for the same module.

    Alert behavior:

        NORMAL
            ↓
        WARNING
            → generate WARNING alert

        WARNING
            ↓
        WARNING
            → suppress duplicate

        WARNING
            ↓
        CRITICAL
            → generate CRITICAL alert

        CRITICAL
            ↓
        CRITICAL
            → suppress duplicate

        WARNING / CRITICAL
            ↓
        NORMAL
            → reset alert state
    """

    def __init__(self, log_path="logs/alerts.jsonl"):

        self.log_path = Path(log_path)

        self.log_path.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        # Stores the last risk state of each module
        #
        # Example:
        #
        # {
        #     "Module_6_Avg_Temp": "WARNING"
        # }
        #
        self.module_states = {}


    # ========================================================
    # PROCESS DECISION
    # ========================================================

    def process_decision(self, decision):

        module = decision["module"]

        current_risk = decision["risk_level"]

        previous_risk = self.module_states.get(
            module,
            "NORMAL"
        )


        # ====================================================
        # UPDATE STATE
        # ====================================================

        self.module_states[module] = current_risk


        # ====================================================
        # NORMAL
        # ====================================================

        if current_risk == "NORMAL":

            # If module was previously abnormal,
            # record recovery.

            if previous_risk in {
                "WARNING",
                "CRITICAL"
            }:

                recovery = self._create_recovery_event(
                    decision
                )

                self._write_event(recovery)

                print(
                    f"[RECOVERY] {module} "
                    f"returned to NORMAL."
                )

            return None


        # ====================================================
        # WARNING
        # ====================================================

        if current_risk == "WARNING":

            # First transition:
            #
            # NORMAL → WARNING
            #
            if previous_risk == "NORMAL":

                alert = self._create_alert(
                    decision
                )

                self._write_event(alert)

                return alert


            # CRITICAL → WARNING
            #
            # This is a downgrade.
            # Don't generate another warning.
            #
            if previous_risk == "CRITICAL":

                return None


            # WARNING → WARNING
            #
            # Duplicate condition.
            #
            return None


        # ====================================================
        # CRITICAL
        # ====================================================

        if current_risk == "CRITICAL":

            # NORMAL → CRITICAL
            #
            if previous_risk == "NORMAL":

                alert = self._create_alert(
                    decision
                )

                self._write_event(alert)

                return alert


            # WARNING → CRITICAL
            #
            # Escalation — generate a new alert.
            #
            if previous_risk == "WARNING":

                alert = self._create_alert(
                    decision
                )

                self._write_event(alert)

                return alert


            # CRITICAL → CRITICAL
            #
            # Duplicate condition.
            #
            return None


        return None


    # ========================================================
    # CREATE ALERT
    # ========================================================

    def _create_alert(self, decision):

        risk = decision["risk_level"]

        module = decision["module"]

        predicted = decision[
            "predicted_temperature"
        ]

        if risk == "CRITICAL":

            message = (
                f"CRITICAL temperature risk detected "
                f"in {module}. "
                f"Predicted temperature: "
                f"{predicted:.2f}°C."
            )

        else:

            message = (
                f"WARNING temperature risk detected "
                f"in {module}. "
                f"Predicted temperature: "
                f"{predicted:.2f}°C."
            )


        return {

            "timestamp":
                datetime.now(
                    timezone.utc
                ).isoformat(),

            "event_type":
                "TEMPERATURE_ALERT",

            "severity":
                risk,

            "module":
                module,

            "current_temperature":
                decision.get(
                    "current_temperature"
                ),

            "predicted_temperature":
                predicted,

            "temperature_delta":
                decision.get(
                    "temperature_delta"
                ),

            "warning_threshold":
                decision[
                    "warning_threshold"
                ],

            "critical_threshold":
                decision[
                    "critical_threshold"
                ],

            "message":
                message
        }


    # ========================================================
    # RECOVERY EVENT
    # ========================================================

    def _create_recovery_event(
        self,
        decision
    ):

        return {

            "timestamp":
                datetime.now(
                    timezone.utc
                ).isoformat(),

            "event_type":
                "TEMPERATURE_RECOVERY",

            "severity":
                "NORMAL",

            "module":
                decision["module"],

            "current_temperature":
                decision.get(
                    "current_temperature"
                ),

            "predicted_temperature":
                decision[
                    "predicted_temperature"
                ],

            "temperature_delta":
                decision.get(
                    "temperature_delta"
                ),

            "message":
                (
                    f"{decision['module']} "
                    f"returned to normal."
                )
        }


    # ========================================================
    # WRITE EVENT
    # ========================================================

    def _write_event(self, event):

        with self.log_path.open(
            "a",
            encoding="utf-8"
        ) as file:

            file.write(
                json.dumps(event)
                + "\n"
            )