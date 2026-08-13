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

REFRESH_INTERVAL = 3000


# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="SentinelDC",
    page_icon="🌡️",
    layout="wide",
)


# ============================================================
# API HELPER
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
# GET SYSTEM PIPELINE
# ============================================================

pipeline = get_api_data(
    "/monitoring/system-status"
)


# ============================================================
# SYSTEM PIPELINE
# ============================================================

st.subheader(
    "System Pipeline"
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

    columns = st.columns(6)

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
    # PROGRESS
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

    simulation_status = (
        simulation.get(
            "status",
            "UNKNOWN",
        )
    )

    simulation_speed = (
        simulation.get(
            "speed",
            1.0,
        )
    )

    current_scenario = (
        simulation.get(
            "scenario",
            "NORMAL",
        )
    )

else:

    simulation_status = "UNKNOWN"

    simulation_speed = 1.0

    current_scenario = "NORMAL"


# ------------------------------------------------------------
# STATUS
# ------------------------------------------------------------

status_col1, status_col2 = (
    st.columns(2)
)


with status_col1:

    st.metric(
        "Simulation Status",
        simulation_status,
    )


with status_col2:

    st.metric(
        "Current Speed",
        f"{simulation_speed}x",
    )


# ------------------------------------------------------------
# SPEED
# ------------------------------------------------------------

st.write(
    "**Replay Speed**"
)

speed_columns = st.columns(5)

speed_options = [
    1,
    2,
    5,
    10,
    20,
]


for column, speed in zip(
    speed_columns,
    speed_options,
):

    with column:

        if st.button(
            f"{speed}x",
            use_container_width=True,
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
        use_container_width=True,
    ):

        result = post_api_data(
            "/simulation/start",
            {
                "speed": float(
                    simulation_speed
                )
            },
        )

        if result:

            st.success(
                "Simulation started."
            )

            st.rerun()


with control_col2:

    if st.button(
        "⏸ Pause",
        use_container_width=True,
    ):

        result = post_api_data(
            "/simulation/pause"
        )

        if result:

            st.info(
                "Simulation paused."
            )

            st.rerun()


with control_col3:

    if st.button(
        "⏹ Stop",
        use_container_width=True,
    ):

        result = post_api_data(
            "/simulation/stop"
        )

        if result:

            st.warning(
                "Simulation stopped."
            )

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
        "Original telemetry. No anomaly injection.",

    "WARNING":
        "Module 6 temperature is increased by +5°C.",

    "CRITICAL":
        "Module 6 temperature is increased by +10°C.",
}


st.info(
    scenario_descriptions[
        selected_scenario
    ]
)


if st.button(
    "🎯 Apply Scenario",
    use_container_width=True,
):

    result = post_api_data(
        "/simulation/scenario",
        {
            "scenario": selected_scenario
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

st.write(
    "Dashboard refresh:",
    pd.Timestamp.now(),
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
# API / SYSTEM HEALTH
# ============================================================

st.divider()

st.subheader(
    "System Status"
)


if health is None:

    st.error(
        "🔴 Monitoring API is unavailable"
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

    col1, col2, col3 = (
        st.columns(3)
    )

    with col1:

        if status == "healthy":

            st.success(
                "🟢 API: HEALTHY"
            )

        else:

            st.error(
                "🔴 API: UNHEALTHY"
            )


    with col2:

        if database_health == "healthy":

            st.success(
                "🟢 DATABASE: HEALTHY"
            )

        else:

            st.error(
                "🔴 DATABASE: UNHEALTHY"
            )


    with col3:

        st.info(
            "🔄 Auto-refresh: 3 seconds"
        )


# ============================================================
# PREPARE PREDICTION DATA
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
# CALCULATE RECENT MAE
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

            absolute_errors = (

                valid_predictions[
                    "current_temperature"
                ]

                -

                valid_predictions[
                    "predicted_temperature"
                ]

            ).abs()

            recent_mae = (
                absolute_errors.mean()
            )


# ============================================================
# SUMMARY KPI CARDS
# ============================================================

st.subheader(
    "System Overview"
)


if summary:

    total_predictions = (
        summary.get(
            "total_predictions",
            0,
        )
    )

    total_alerts = (
        summary.get(
            "total_alerts",
            0,
        )
    )

    warning_alerts = (
        summary.get(
            "warning_alerts",
            0,
        )
    )

    critical_alerts = (
        summary.get(
            "critical_alerts",
            0,
        )
    )

else:

    total_predictions = 0

    total_alerts = 0

    warning_alerts = 0

    critical_alerts = 0


col1, col2, col3, col4, col5 = (
    st.columns(5)
)


with col1:

    st.metric(
        "Predictions",
        total_predictions,
    )


with col2:

    st.metric(
        "Total Alerts",
        total_alerts,
    )


with col3:

    st.metric(
        "Warnings",
        warning_alerts,
    )


with col4:

    st.metric(
        "Critical",
        critical_alerts,
    )


with col5:

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
    "Module Health"
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


    columns = st.columns(4)


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


        with columns[
            index % 4
        ]:

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


            if current is not None:

                st.write(
                    f"Current: "
                    f"**{float(current):.2f} °C**"
                )

            else:

                st.write(
                    "Current: **N/A**"
                )


            if predicted is not None:

                st.write(
                    f"Predicted: "
                    f"**{float(predicted):.2f} °C**"
                )

            else:

                st.write(
                    "Predicted: **N/A**"
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
            f"**{module}**\n\n"

            f"Predicted: "
            f"**{predicted_text}**  \n"

            f"Current: "
            f"**{current_text}**  \n\n"

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
# TEMPERATURE TREND
# ============================================================

st.divider()

st.subheader(
    "📈 Temperature Prediction Trend"
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

                    .apply(
                        pd.to_numeric,
                        errors="coerce",
                    )

                    .dropna(
                        how="all"
                    )
                )


                chart_df.columns = [

                    (
                        "Current Temperature"

                        if column
                        == "current_temperature"

                        else
                        "Predicted Temperature"
                    )

                    for column
                    in chart_df.columns
                ]


                st.line_chart(
                    chart_df,
                    use_container_width=True,
                )

            else:

                st.info(
                    "Temperature data unavailable."
                )

        else:

            st.info(
                f"No history available for "
                f"{selected_module}."
            )

    else:

        st.info(
            "No modules available."
        )

else:

    st.info(
        "Module information unavailable."
    )


# ============================================================
# RECENT PREDICTIONS
# ============================================================

st.divider()

st.subheader(
    "Recent Predictions"
)


if not prediction_df.empty:

    display_df = (
        prediction_df.copy()
    )


    preferred_columns = [

        "timestamp",

        "module",

        "current_temperature",

        "predicted_temperature",

        "temperature_delta",

        "warning_threshold",

        "critical_threshold",

        "risk_level",

        "replay_id",

        "kafka_partition",

        "kafka_offset",
    ]


    existing_columns = [

        column

        for column in preferred_columns

        if column in display_df.columns
    ]


    remaining_columns = [

        column

        for column in display_df.columns

        if column not in existing_columns
    ]


    display_df = display_df[
        existing_columns
        + remaining_columns
    ]


    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
    )

else:

    st.info(
        "No predictions available."
    )


# ============================================================
# SYSTEM INFORMATION
# ============================================================

st.divider()

st.subheader(
    "System Information"
)


info_col1, info_col2, info_col3 = (
    st.columns(3)
)


with info_col1:

    st.write(
        "**Inference Engine**"
    )

    st.write(
        "XGBoost"
    )


with info_col2:

    st.write(
        "**Message Broker**"
    )

    st.write(
        "Apache Kafka"
    )


with info_col3:

    st.write(
        "**Monitoring Stack**"
    )

    st.write(
        "FastAPI + SQLite + Streamlit"
    )


# ============================================================
# FOOTER
# ============================================================

st.divider()

st.caption(
    "SentinelDC | AI-Powered Data Center "
    "Temperature Monitoring & Predictive Alert Platform"
)

st.caption(
    "Live monitoring refresh interval: 3 seconds"
)