import requests
import pandas as pd
import streamlit as st

from streamlit_autorefresh import (
    st_autorefresh,
)


# ============================================================
# CONFIGURATION
# ============================================================

API_URL = "http://127.0.0.1:8000"

REFRESH_INTERVAL = 3000  # 3 seconds


# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="SentinelDC",
    page_icon="🌡️",
    layout="wide",
)


# ============================================================
# API HELPERS
# ============================================================

def get_api_data(endpoint):

    try:

        response = requests.get(
            f"{API_URL}{endpoint}",
            timeout=5,
        )

        response.raise_for_status()

        return response.json()

    except requests.ConnectionError:

        return None

    except requests.Timeout:

        return None

    except requests.RequestException as error:

        st.error(
            f"Monitoring API error: {error}"
        )

        return None


def post_api_data(
    endpoint,
    payload=None,
):

    try:

        response = requests.post(
            f"{API_URL}{endpoint}",
            json=payload,
            timeout=5,
        )

        response.raise_for_status()

        return response.json()

    except requests.RequestException as error:

        st.error(
            f"API request failed: {error}"
        )

        return None


# ============================================================
# HEADER
# ============================================================

st.title("🌡️ SentinelDC")

st.caption(
    "AI-Powered Data Center Temperature "
    "Monitoring & Predictive Alert Platform"
)

st.caption(
    "Kafka • XGBoost • FastAPI • SQLite • Streamlit"
)


# ============================================================
# FORECAST CONFIGURATION
# ============================================================

forecast_config = get_api_data(
    "/forecast/config"
)


if forecast_config:

    horizon_minutes = forecast_config.get(
        "horizon_minutes",
        5,
    )

    horizon_seconds = forecast_config.get(
        "horizon_seconds",
        300,
    )

else:

    # Fallback only for UI
    horizon_minutes = 5
    horizon_seconds = 300


# ============================================================
# FORECAST KPI CARDS
# ============================================================

col1, col2, col3 = st.columns(3)


with col1:

    st.metric(
        "AI Forecast Horizon",
        f"{horizon_minutes} min",
    )


with col2:

    st.metric(
        "Prediction Engine",
        "XGBoost",
    )


with col3:

    st.metric(
        "Telemetry Pipeline",
        "Live",
    )


st.caption(
    f"XGBoost forecast horizon: "
    f"+{horizon_seconds} seconds"
)


# ============================================================
# SYSTEM PIPELINE
# ============================================================

st.divider()

st.subheader(
    "🔗 System Pipeline"
)


pipeline = get_api_data(
    "/monitoring/system-status"
)


if pipeline:

    simulation = pipeline.get(
        "simulation",
        {},
    )

    kafka = pipeline.get(
        "kafka",
        {},
    )

    ml = pipeline.get(
        "ml_inference",
        {},
    )

    decision = pipeline.get(
        "decision_engine",
        {},
    )

    database = pipeline.get(
        "database",
        {},
    )

    api = pipeline.get(
        "api",
        {},
    )


    components = [

        (
            "Simulation",
            simulation,
        ),

        (
            "Kafka",
            kafka,
        ),

        (
            "ML Inference",
            ml,
        ),

        (
            "Decision Engine",
            decision,
        ),

        (
            "Database",
            database,
        ),

        (
            "FastAPI",
            api,
        ),

    ]


    columns = st.columns(6)


    for column, (
        name,
        component,
    ) in zip(
        columns,
        components,
    ):

        with column:

            healthy = component.get(
                "healthy",
                False,
            )

            status = component.get(
                "status",
                "UNKNOWN",
            )


            if healthy:

                st.success(
                    f"🟢 {name}"
                )

            else:

                st.error(
                    f"🔴 {name}"
                )


            st.caption(
                status
            )


    # --------------------------------------------------------
    # SIMULATION PROGRESS
    # --------------------------------------------------------

    progress = simulation.get(
        "progress_percent",
        0.0,
    )

    records_sent = simulation.get(
        "records_sent",
        0,
    )

    total_records = simulation.get(
        "total_records",
        0,
    )


    st.progress(
        min(
            progress / 100,
            1.0,
        )
    )


    st.caption(
        f"Simulation progress: "
        f"{progress:.2f}% "
        f"({records_sent} / "
        f"{total_records})"
    )


else:

    st.error(
        "Unable to load system pipeline."
    )


# ============================================================
# SIMULATION CONTROL
# ============================================================

st.divider()

st.subheader(
    "🎮 Simulation Control"
)


if pipeline:

    simulation_status = simulation.get(
        "status",
        "UNKNOWN",
    )

    simulation_speed = simulation.get(
        "speed",
        1.0,
    )

    current_scenario = simulation.get(
        "scenario",
        "NORMAL",
    )

else:

    simulation_status = "UNKNOWN"

    simulation_speed = 1.0

    current_scenario = "NORMAL"


# ------------------------------------------------------------
# CURRENT STATE
# ------------------------------------------------------------

status_col1, status_col2, status_col3 = (
    st.columns(3)
)


with status_col1:

    st.metric(
        "Simulation Status",
        simulation_status,
    )


with status_col2:

    st.metric(
        "Replay Speed",
        f"{simulation_speed}x",
    )


with status_col3:

    st.metric(
        "Scenario",
        current_scenario,
    )


# ------------------------------------------------------------
# SPEED CONTROL
# ------------------------------------------------------------

st.write(
    "**Replay Speed**"
)


speed_options = [
    1,
    2,
    5,
    10,
    20,
]


speed_columns = st.columns(
    len(speed_options)
)


for column, speed in zip(
    speed_columns,
    speed_options,
):

    with column:

        if st.button(
            f"{speed}x",
            width="stretch",
            key=f"speed_{speed}",
        ):

            result = post_api_data(
                "/simulation/speed",
                {
                    "speed": speed
                },
            )

            if result:

                st.rerun()


# ------------------------------------------------------------
# START / PAUSE / STOP
# ------------------------------------------------------------

control_col1, control_col2, control_col3 = (
    st.columns(3)
)


with control_col1:

    if st.button(
        "▶ Start",
        width="stretch",
    ):

        result = post_api_data(
            "/simulation/start",
            {
                "speed":
                    float(
                        simulation_speed
                    )
            },
        )

        if result:

            st.rerun()


with control_col2:

    if st.button(
        "⏸ Pause",
        width="stretch",
    ):

        result = post_api_data(
            "/simulation/pause"
        )

        if result:

            st.rerun()


with control_col3:

    if st.button(
        "⏹ Stop",
        width="stretch",
    ):

        result = post_api_data(
            "/simulation/stop"
        )

        if result:

            st.rerun()


# ============================================================
# DEMO SCENARIO
# ============================================================

st.divider()

st.subheader(
    "🎬 Demo Scenario"
)

st.caption(
    "Inject a controlled temperature anomaly "
    "into Module 6 and observe the complete "
    "ML → Decision → Alert pipeline."
)


scenario_options = [
    "NORMAL",
    "WARNING",
    "CRITICAL",
]


try:

    scenario_index = (
        scenario_options.index(
            current_scenario
        )
    )

except ValueError:

    scenario_index = 0


selected_scenario = st.selectbox(
    "Scenario",
    scenario_options,
    index=scenario_index,
)


scenario_descriptions = {

    "NORMAL":
        (
            "Original telemetry. "
            "No anomaly injection."
        ),

    "WARNING":
        (
            "Module 6 temperature "
            "anomaly injection."
        ),

    "CRITICAL":
        (
            "Strong Module 6 temperature "
            "anomaly injection."
        ),

}


st.info(
    scenario_descriptions[
        selected_scenario
    ]
)


if st.button(
    "🎯 Apply Scenario",
    width="stretch",
):

    result = post_api_data(
        "/simulation/scenario",
        {
            "scenario":
                selected_scenario
        },
    )

    if result:

        st.success(
            f"Scenario changed to "
            f"**{selected_scenario}**"
        )

        st.rerun()


# ============================================================
# AUTO REFRESH
# ============================================================

st_autorefresh(
    interval=REFRESH_INTERVAL,
    key="sentineldc_refresh",
)


# ============================================================
# LOAD MONITORING DATA
# ============================================================

health = get_api_data(
    "/monitoring/health"
)

summary = get_api_data(
    "/monitoring/summary"
)

modules = get_api_data(
    "/monitoring/modules"
)

alerts = get_api_data(
    "/monitoring/alerts?limit=10"
)

predictions = get_api_data(
    "/monitoring/predictions?limit=100"
)


# ============================================================
# SYSTEM HEALTH
# ============================================================

st.divider()

st.subheader(
    "❤️ System Health"
)


if health is None:

    st.error(
        "🔴 Monitoring API unavailable."
    )

else:

    status = health.get(
        "status",
        "unknown",
    )

    database_health = health.get(
        "database",
        "unknown",
    )


    health_col1, health_col2, health_col3 = (
        st.columns(3)
    )


    with health_col1:

        if status == "healthy":

            st.success(
                "🟢 API: HEALTHY"
            )

        else:

            st.error(
                "🔴 API: UNHEALTHY"
            )


    with health_col2:

        if database_health == "healthy":

            st.success(
                "🟢 DATABASE: HEALTHY"
            )

        else:

            st.error(
                "🔴 DATABASE: UNHEALTHY"
            )


    with health_col3:

        st.info(
            "🔄 Live refresh: 3 seconds"
        )


# ============================================================
# PREDICTION DATAFRAME
# ============================================================

prediction_df = pd.DataFrame()


if (
    isinstance(
        predictions,
        list,
    )
    and predictions
):

    prediction_df = pd.DataFrame(
        predictions
    )


# ============================================================
# RECENT MAE
# ============================================================

recent_mae = None


if not prediction_df.empty:

    required_columns = {

        "current_temperature",

        "predicted_temperature",

    }


    if required_columns.issubset(
        prediction_df.columns
    ):

        valid_predictions = (
            prediction_df[
                [
                    "current_temperature",
                    "predicted_temperature",
                ]
            ]
            .dropna()
        )


        if not valid_predictions.empty:

            recent_mae = (

                valid_predictions[
                    "current_temperature"
                ]

                -

                valid_predictions[
                    "predicted_temperature"
                ]

            ).abs().mean()


# ============================================================
# SYSTEM OVERVIEW
# ============================================================

st.subheader(
    "📊 System Overview"
)


if summary:

    total_predictions = summary.get(
        "total_predictions",
        0,
    )

    total_alerts = summary.get(
        "total_alerts",
        0,
    )

    warning_alerts = summary.get(
        "warning_alerts",
        0,
    )

    critical_alerts = summary.get(
        "critical_alerts",
        0,
    )

else:

    total_predictions = 0

    total_alerts = 0

    warning_alerts = 0

    critical_alerts = 0


kpi1, kpi2, kpi3, kpi4, kpi5 = (
    st.columns(5)
)


with kpi1:

    st.metric(
        "Predictions",
        total_predictions,
    )


with kpi2:

    st.metric(
        "Total Alerts",
        total_alerts,
    )


with kpi3:

    st.metric(
        "Warnings",
        warning_alerts,
    )


with kpi4:

    st.metric(
        "Critical",
        critical_alerts,
    )


with kpi5:

    if recent_mae is not None:

        st.metric(
            "Recent MAE",
            f"{recent_mae:.3f} °C",
        )

    else:

        st.metric(
            "Recent MAE",
            "N/A",
        )


# ============================================================
# MODULE HEALTH
# ============================================================

st.divider()

st.subheader(
    "🌡️ Module Health"
)


if (
    isinstance(
        modules,
        list,
    )
    and modules
):


    def module_sort_key(
        module
    ):

        name = module.get(
            "module",
            "",
        )

        try:

            return int(
                name.split("_")[1]
            )

        except (
            IndexError,
            ValueError,
        ):

            return 999


    modules = sorted(
        modules,
        key=module_sort_key,
    )


    module_columns = st.columns(4)


    for index, module in enumerate(
        modules
    ):

        module_name = module.get(
            "module",
            "Unknown",
        )

        predicted = module.get(
            "predicted_temperature"
        )

        current = module.get(
            "current_temperature"
        )

        risk = module.get(
            "risk_level",
            "UNKNOWN",
        )


        display_name = (
            module_name.replace(
                "_Avg_Temp",
                "",
            )
        )


        with module_columns[
            index % 4
        ]:


            # ------------------------------------------------
            # RISK
            # ------------------------------------------------

            if risk == "CRITICAL":

                st.error(
                    f"🔴 {display_name}"
                )

            elif risk == "WARNING":

                st.warning(
                    f"🟡 {display_name}"
                )

            elif risk == "NORMAL":

                st.success(
                    f"🟢 {display_name}"
                )

            else:

                st.info(
                    f"⚪ {display_name}"
                )


            st.write(
                f"Risk: **{risk}**"
            )


            # ------------------------------------------------
            # CURRENT TEMPERATURE
            # ------------------------------------------------

            if current is not None:

                current_value = float(
                    current
                )

                st.write(
                    f"Current: "
                    f"**{current_value:.2f} °C**"
                )

            else:

                current_value = None

                st.write(
                    "Current: **N/A**"
                )


            # ------------------------------------------------
            # 5-MINUTE AI FORECAST
            # ------------------------------------------------

            if predicted is not None:

                predicted_value = float(
                    predicted
                )

                st.write(
                    f"AI Forecast "
                    f"(+{horizon_minutes} min): "
                    f"**{predicted_value:.2f} °C**"
                )

            else:

                predicted_value = None

                st.write(
                    "AI Forecast: **N/A**"
                )


            # ------------------------------------------------
            # EXPECTED TEMPERATURE CHANGE
            # ------------------------------------------------

            if (
                current_value is not None
                and predicted_value is not None
            ):

                delta = (
                    predicted_value
                    - current_value
                )


                if delta > 0:

                    st.write(
                        f"Expected rise: "
                        f"**+{delta:.2f} °C**"
                    )

                elif delta < 0:

                    st.write(
                        f"Expected drop: "
                        f"**{delta:.2f} °C**"
                    )

                else:

                    st.write(
                        "Expected change: "
                        "**0.00 °C**"
                    )


else:

    st.info(
        "No module predictions available."
    )


# ============================================================
# RECENT ALERTS
# ============================================================

st.divider()

st.subheader(
    "🚨 Recent Alerts"
)


if (
    isinstance(
        alerts,
        list,
    )
    and alerts
):


    for alert in alerts:

        level = alert.get(
            "alert_level",
            "UNKNOWN",
        )

        module = alert.get(
            "module",
            "Unknown",
        )

        predicted = alert.get(
            "predicted_temperature"
        )

        current = alert.get(
            "current_temperature"
        )

        message = alert.get(
            "message",
            "",
        )


        predicted_text = (

            f"{float(predicted):.2f} °C"

            if predicted is not None

            else "N/A"

        )


        current_text = (

            f"{float(current):.2f} °C"

            if current is not None

            else "N/A"

        )


        alert_text = (

            f"**{level}** | "
            f"**{module}**  \n"
            f"Current: **{current_text}**  \n"
            f"5-min forecast: "
            f"**{predicted_text}**  \n\n"
            f"{message}"

        )


        if level == "CRITICAL":

            st.error(
                f"🔴 {alert_text}"
            )

        elif level == "WARNING":

            st.warning(
                f"🟡 {alert_text}"
            )

        else:

            st.info(
                f"⚪ {alert_text}"
            )


else:

    st.success(
        "✅ No WARNING or CRITICAL alerts recorded."
    )


# ============================================================
# RECENT PREDICTIONS
# ============================================================

st.divider()

st.subheader(
    "📋 Recent Predictions"
)


if (
    isinstance(
        predictions,
        list,
    )
    and predictions
):

    display_predictions = (
        pd.DataFrame(
            predictions
        )
    )


    # Rename for recruiter-friendly UI
    rename_map = {

        "current_temperature":
            "Current °C",

        "predicted_temperature":
            "5-Min Forecast °C",

        "temperature_delta":
            "Expected Change °C",

        "risk_level":
            "Risk",

        "module":
            "Module",

        "timestamp":
            "Timestamp",

    }


    display_predictions = (
        display_predictions.rename(
            columns=rename_map
        )
    )


    st.dataframe(
        display_predictions,
        width="stretch",
        hide_index=True,
    )


else:

    st.info(
        "No prediction data available."
    )


# ============================================================
# 5-MINUTE TEMPERATURE FORECAST
# ============================================================

st.divider()

st.subheader(
    "📈 5-Minute Temperature Forecast"
)

st.caption(
    "Current telemetry vs. XGBoost "
    f"forecast +{horizon_minutes} minutes."
)


if (
    isinstance(
        modules,
        list,
    )
    and modules
):


    available_modules = sorted(

        [

            module.get("module")

            for module in modules

            if module.get("module")

        ]

    )


    if available_modules:

        selected_module = st.selectbox(
            "Select Module",
            available_modules,
        )


        history = get_api_data(

            f"/monitoring/modules/"
            f"{selected_module}/history"
            f"?limit=100"

        )


        if (
            isinstance(
                history,
                list,
            )
            and history
        ):


            history_df = pd.DataFrame(
                history
            )


            if "timestamp" in (
                history_df.columns
            ):

                history_df[
                    "timestamp"
                ] = pd.to_datetime(
                    history_df[
                        "timestamp"
                    ],
                    errors="coerce",
                )


                history_df = (
                    history_df
                    .dropna(
                        subset=[
                            "timestamp"
                        ]
                    )
                    .sort_values(
                        "timestamp"
                    )
                    .set_index(
                        "timestamp"
                    )
                )


            chart_columns = []


            if (
                "current_temperature"
                in history_df.columns
            ):

                chart_columns.append(
                    "current_temperature"
                )


            if (
                "predicted_temperature"
                in history_df.columns
            ):

                chart_columns.append(
                    "predicted_temperature"
                )


            if chart_columns:

                chart_df = (
                    history_df[
                        chart_columns
                    ]
                    .copy()
                )


                chart_df.columns = [

                    (
                        "Current Temperature"
                        if column
                        == "current_temperature"
                        else
                        "5-Minute AI Forecast"
                    )

                    for column
                    in chart_df.columns

                ]


                st.line_chart(
                    chart_df,
                    width="stretch",
                )

            else:

                st.info(
                    "Temperature columns "
                    "are not available."
                )


        else:

            st.info(
                "No temperature history "
                "available yet."
            )


# ============================================================
# FOOTER
# ============================================================

st.divider()

st.caption(
    "SentinelDC | "
    "5-Minute XGBoost Forecasting | "
    "Kafka + FastAPI + SQLite + Streamlit"
)

st.caption(
    "Dashboard automatically refreshes "
    "every 3 seconds."
)