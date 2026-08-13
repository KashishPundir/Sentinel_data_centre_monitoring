class TemperatureDecisionEngine:

    def __init__(
        self,
        thresholds,
        delta_warning=0.5,
        delta_critical=1.0
    ):
        self.thresholds = thresholds

        # Minimum predicted-temperature increase
        # required to strengthen the risk assessment.
        self.delta_warning = delta_warning
        self.delta_critical = delta_critical


    def evaluate(
        self,
        module,
        predicted_temperature,
        current_temperature=None,
        trend=None
    ):

        # =====================================================
        # VALIDATE MODULE
        # =====================================================

        if module not in self.thresholds:
            raise ValueError(
                f"No threshold found for {module}"
            )


        warning = float(
            self.thresholds[module]["warning"]
        )

        critical = float(
            self.thresholds[module]["critical"]
        )


        predicted_temperature = float(
            predicted_temperature
        )


        # =====================================================
        # TEMPERATURE DELTA
        # =====================================================

        temperature_delta = None

        if current_temperature is not None:

            current_temperature = float(
                current_temperature
            )

            temperature_delta = (
                predicted_temperature
                - current_temperature
            )


        # =====================================================
        # BASE RISK
        # =====================================================
        #
        # Absolute predicted temperature determines
        # the initial risk.
        #
        # NORMAL
        # WARNING
        # CRITICAL
        #
        # =====================================================

        if predicted_temperature >= critical:

            risk = "CRITICAL"

        elif predicted_temperature >= warning:

            risk = "WARNING"

        else:

            risk = "NORMAL"


        # =====================================================
        # TREND-BASED ADJUSTMENT
        # =====================================================

        if temperature_delta is not None:

            # -------------------------------------------------
            # If temperature is below warning threshold AND
            # prediction is not increasing significantly,
            # don't treat it as an escalating warning.
            # -------------------------------------------------

            if (
                risk == "WARNING"
                and predicted_temperature < critical
                and temperature_delta <= 0
            ):

                risk = "NORMAL"


            # -------------------------------------------------
            # If temperature is above critical but is falling,
            # retain CRITICAL because the absolute predicted
            # temperature is still dangerous.
            # -------------------------------------------------

            elif (
                risk == "CRITICAL"
                and temperature_delta < 0
            ):

                risk = "CRITICAL"


            # -------------------------------------------------
            # Strong upward movement can strengthen WARNING.
            # -------------------------------------------------

            elif (
                risk == "NORMAL"
                and temperature_delta >= self.delta_warning
                and predicted_temperature >= warning * 0.98
            ):

                risk = "WARNING"


            # -------------------------------------------------
            # Strong upward movement + high prediction
            # can strengthen to CRITICAL.
            # -------------------------------------------------

            elif (
                predicted_temperature >= warning
                and temperature_delta >= self.delta_critical
            ):

                risk = "CRITICAL"


        # =====================================================
        # RESULT
        # =====================================================

        result = {

            "module": module,

            "predicted_temperature":
                predicted_temperature,

            "warning_threshold":
                warning,

            "critical_threshold":
                critical,

            "risk_level":
                risk
        }


        # =====================================================
        # CURRENT TEMPERATURE
        # =====================================================

        if current_temperature is not None:

            result["current_temperature"] = (
                current_temperature
            )

            result["temperature_delta"] = (
                temperature_delta
            )


        # =====================================================
        # OPTIONAL TREND
        # =====================================================

        if trend is not None:

            result["trend"] = float(trend)


        return result