# import requests
# import pandas as pd
# import streamlit as st

# from streamlit_autorefresh import (
#     st_autorefresh,
# )


# # ============================================================
# # CONFIGURATION
# # ============================================================

# API_URL = "http://127.0.0.1:8000"

# REFRESH_INTERVAL = 3000  # 3 seconds

# SPEED_OPTIONS = [1, 2, 5, 10, 20]


# # ============================================================
# # PAGE CONFIGURATION
# # ============================================================

# st.set_page_config(
#     page_title="SentinelDC",
#     page_icon="🌡️",
#     layout="wide",
# )


# # ============================================================
# # API HELPERS
# # ============================================================

# def get_api_data(endpoint):

#     try:

#         response = requests.get(
#             f"{API_URL}{endpoint}",
#             timeout=5,
#         )

#         response.raise_for_status()

#         return response.json()

#     except requests.ConnectionError:

#         return None

#     except requests.Timeout:

#         return None

#     except requests.RequestException as error:

#         st.error(
#             f"Monitoring API error: {error}"
#         )

#         return None


# def post_api_data(
#     endpoint,
#     payload=None,
# ):

#     try:

#         response = requests.post(
#             f"{API_URL}{endpoint}",
#             json=payload,
#             timeout=5,
#         )

#         response.raise_for_status()

#         return response.json()

#     except requests.RequestException as error:

#         st.error(
#             f"API request failed: {error}"
#         )

#         return None


# def format_temperature(value):

#     if value is None:

#         return "N/A"

#     return f"{float(value):.2f}°C"


# def module_display_name(module_name):

#     return module_name.replace(
#         "_Avg_Temp",
#         "",
#     )


# def module_sort_key(module_name):

#     try:

#         return int(
#             module_name.split("_")[1]
#         )

#     except (IndexError, ValueError):

#         return 999


# # ============================================================
# # HEADER
# # ============================================================

# st.title("🌡️ SentinelDC")

# st.caption(
#     "AI-Powered Data Center Temperature "
#     "Monitoring & Predictive Alert Platform"
# )


# # ============================================================
# # AUTO REFRESH
# # ============================================================

# st_autorefresh(
#     interval=REFRESH_INTERVAL,
#     key="sentineldc_refresh",
# )


# # ============================================================
# # LOAD CORE DATA
# # ============================================================

# pipeline = get_api_data("/monitoring/system-status")

# modules = get_api_data("/monitoring/modules")

# alerts = get_api_data("/monitoring/alerts?limit=10")

# forecast_performance = get_api_data(
#     "/monitoring/forecast-performance"
# )


# if pipeline:

#     simulation = pipeline.get("simulation", {})
#     kafka = pipeline.get("kafka", {})
#     database = pipeline.get("database", {})
#     api_component = pipeline.get("api", {})
#     model_component = pipeline.get("model", {})
#     inference = pipeline.get("ml_inference", {})

# else:

#     simulation = {}
#     kafka = {}
#     database = {}
#     api_component = {}
#     model_component = {}
#     inference = {}


# if forecast_performance:

#     overall_performance = forecast_performance.get(
#         "overall", {}
#     )

#     per_module_performance = forecast_performance.get(
#         "modules", {}
#     )

# else:

#     overall_performance = {}

#     per_module_performance = {}


# if not isinstance(modules, list):

#     modules = []

# modules = sorted(
#     modules,
#     key=lambda module: module_sort_key(
#         module.get("module", "")
#     ),
# )


# # ============================================================
# # SYSTEM STATUS
# # ============================================================

# status_col1, status_col2, status_col3, status_col4, status_col5 = st.columns(5)


# with status_col1:

#     st.metric(
#         "API",
#         api_component.get("status", "UNKNOWN"),
#     )


# with status_col2:

#     kafka_healthy = kafka.get("healthy", False)

#     st.metric(
#         "Kafka",
#         "HEALTHY" if kafka_healthy else "UNREACHABLE",
#     )


# with status_col3:

#     st.metric(
#         "Database",
#         database.get("status", "UNKNOWN"),
#     )


# with status_col4:

#     st.metric(
#         "Model",
#         model_component.get("status", "UNKNOWN"),
#     )


# with status_col5:

#     st.metric(
#         "Inference worker",
#         inference.get("status", "UNKNOWN"),
#     )

#     worker_error = inference.get("worker", {}).get("last_error")

#     if worker_error:

#         st.caption(worker_error)


# # ============================================================
# # LIVE OVERVIEW
# # ============================================================

# st.header("Live Overview")

# active_alert_modules = [
#     module
#     for module in modules
#     if module.get("risk_level") in ("WARNING", "CRITICAL")
# ]

# warning_count = sum(
#     1
#     for module in modules
#     if module.get("risk_level") == "WARNING"
# )

# critical_count = sum(
#     1
#     for module in modules
#     if module.get("risk_level") == "CRITICAL"
# )

# overview_mae = overall_performance.get("mae")

# overview_col1, overview_col2, overview_col3, \
#     overview_col4, overview_col5, overview_col6 = st.columns(6)


# with overview_col1:

#     st.metric(
#         "Evaluated forecasts",
#         overall_performance.get("evaluated", 0),
#     )


# with overview_col2:

#     st.metric(
#         "Pending forecasts",
#         overall_performance.get("pending", 0),
#     )


# with overview_col3:

#     st.metric(
#         "Active alerts",
#         len(active_alert_modules),
#     )


# with overview_col4:

#     st.metric(
#         "Warnings",
#         warning_count,
#     )


# with overview_col5:

#     st.metric(
#         "Criticals",
#         critical_count,
#     )


# with overview_col6:

#     st.metric(
#         "5-min MAE",
#         (
#             f"{overview_mae:.3f}°C"
#             if overview_mae is not None
#             else "N/A"
#         ),
#     )


# # ============================================================
# # SIMULATION CONTROL
# # ============================================================

# st.header("Simulation Control")

# simulation_speed = simulation.get("speed", 1.0)

# control_col1, control_col2, control_col3, \
#     control_col4, control_col5 = st.columns(5)


# with control_col1:

#     selected_speed = st.selectbox(
#         "Speed",
#         SPEED_OPTIONS,
#         index=(
#             SPEED_OPTIONS.index(int(simulation_speed))
#             if int(simulation_speed) in SPEED_OPTIONS
#             else 2
#         ),
#     )

#     if selected_speed != simulation_speed:

#         post_api_data(
#             "/simulation/speed",
#             {"speed": float(selected_speed)},
#         )

#         st.rerun()


# with control_col2:

#     if st.button("▶ Start"):

#         result = post_api_data(
#             "/simulation/start",
#             {"speed": float(selected_speed)},
#         )

#         if result:

#             st.rerun()


# with control_col3:

#     if st.button("⏸ Pause"):

#         result = post_api_data("/simulation/pause")

#         if result:

#             st.rerun()


# with control_col4:

#     if st.button("▶ Resume"):

#         result = post_api_data(
#             "/simulation/start",
#             {"speed": float(selected_speed)},
#         )

#         if result:

#             st.rerun()


# with control_col5:

#     if st.button("■ Stop"):

#         result = post_api_data("/simulation/stop")

#         if result:

#             st.rerun()


# simulation_status = simulation.get("status", "UNKNOWN")

# records_sent = simulation.get("records_sent", 0)

# total_records = simulation.get("total_records", 0)

# last_event = simulation.get("last_event_timestamp") or "—"

# st.caption(
#     f"Status: **{simulation_status}** | "
#     f"Records sent: {records_sent}/{total_records} | "
#     f"Last event: {last_event}"
# )


# # ============================================================
# # DEMO SCENARIO
# # ============================================================

# with st.expander("Demo scenario"):

#     st.caption(
#         "Inject a controlled temperature anomaly into "
#         "Module 6 and observe the complete "
#         "ML → Decision → Alert pipeline."
#     )

#     scenario_options = ["NORMAL", "WARNING", "CRITICAL"]

#     current_scenario = simulation.get("scenario", "NORMAL")

#     try:

#         scenario_index = scenario_options.index(current_scenario)

#     except ValueError:

#         scenario_index = 0

#     selected_scenario = st.selectbox(
#         "Scenario",
#         scenario_options,
#         index=scenario_index,
#     )

#     if st.button("Apply Scenario"):

#         result = post_api_data(
#             "/simulation/scenario",
#             {"scenario": selected_scenario},
#         )

#         if result:

#             st.success(
#                 f"Scenario changed to **{selected_scenario}**"
#             )

#             st.rerun()


# # ============================================================
# # MODULE HEALTH
# # ============================================================

# st.header("Module Health")

# if modules:

#     for module in modules:

#         module_name = module.get("module", "Unknown")

#         current = module.get("current_temperature")

#         predicted = module.get("predicted_temperature")

#         display_name = module_display_name(module_name)

#         performance = per_module_performance.get(module_name, {})

#         module_mae = performance.get("mae")

#         module_evaluated = performance.get("evaluated", 0)

#         forecast_delta = None

#         if current is not None and predicted is not None:

#             forecast_delta = float(predicted) - float(current)

#         with st.container(border=True):

#             st.write(f"**{display_name}**")

#             card_col1, card_col2, card_col3, card_col4 = st.columns(4)

#             with card_col1:

#                 st.metric(
#                     "Current",
#                     format_temperature(current),
#                 )

#             with card_col2:

#                 st.metric(
#                     "5-min Forecast",
#                     format_temperature(predicted),
#                     delta=(
#                         f"{forecast_delta:.2f}°C"
#                         if forecast_delta is not None
#                         else None
#                     ),
#                 )

#             with card_col3:

#                 st.metric(
#                     "5-min MAE",
#                     (
#                         f"{module_mae:.3f}°C"
#                         if module_mae is not None
#                         else "N/A"
#                     ),
#                 )

#             with card_col4:

#                 st.metric(
#                     "Evaluated",
#                     module_evaluated,
#                 )

# else:

#     st.info("No module data available.")


# # ============================================================
# # ACTIVE ALERTS
# # ============================================================

# st.header("Active Alerts")

# if active_alert_modules:

#     for module in active_alert_modules:

#         module_name = module.get("module", "Unknown")

#         risk = module.get("risk_level", "UNKNOWN")

#         display_name = module_display_name(module_name)

#         message = (
#             f"**{risk}** — {display_name} "
#             f"forecast: "
#             f"{format_temperature(module.get('predicted_temperature'))}"
#         )

#         if risk == "CRITICAL":

#             st.error(message)

#         else:

#             st.warning(message)

# else:

#     st.success("No active alerts.")


# with st.expander("Alert History"):

#     if isinstance(alerts, list) and alerts:

#         history_df = pd.DataFrame(alerts)

#         rename_map = {
#             "timestamp": "Timestamp",
#             "module": "Module",
#             "alert_level": "Level",
#             "current_temperature": "Current °C",
#             "predicted_temperature": "5-Min Forecast °C",
#             "message": "Message",
#         }

#         history_df = history_df.rename(columns=rename_map)

#         display_columns = [
#             column
#             for column in rename_map.values()
#             if column in history_df.columns
#         ]

#         st.dataframe(
#             history_df[display_columns],
#             width="stretch",
#             hide_index=True,
#         )

#     else:

#         st.caption("No alerts recorded yet.")


# # ============================================================
# # TEMPERATURE TREND
# # ============================================================

# st.header("Temperature Trend")

# available_modules = [
#     module.get("module")
#     for module in modules
#     if module.get("module")
# ]

# if available_modules:

#     selected_module = st.selectbox(
#         "Module",
#         available_modules,
#     )

#     history = get_api_data(
#         f"/monitoring/modules/{selected_module}/history?limit=100"
#     )

#     if isinstance(history, list) and history:

#         history_df = pd.DataFrame(history)

#         if "timestamp" in history_df.columns:

#             history_df["timestamp"] = pd.to_datetime(
#                 history_df["timestamp"],
#                 errors="coerce",
#             )

#             history_df = (
#                 history_df
#                 .dropna(subset=["timestamp"])
#                 .sort_values("timestamp")
#                 .set_index("timestamp")
#             )

#         chart_columns = [
#             column
#             for column in (
#                 "current_temperature",
#                 "predicted_temperature",
#             )
#             if column in history_df.columns
#         ]

#         if chart_columns:

#             chart_df = history_df[chart_columns].copy()

#             chart_df.columns = [
#                 (
#                     "Current"
#                     if column == "current_temperature"
#                     else "5-min Forecast"
#                 )
#                 for column in chart_df.columns
#             ]

#             st.line_chart(
#                 chart_df,
#                 width="stretch",
#             )

#         else:

#             st.info("Temperature columns are not available.")

#     else:

#         st.info("No temperature history available yet.")

# else:

#     st.info("No module data available.")


# # ============================================================
# # FORECAST PERFORMANCE
# # ============================================================

# st.header("Forecast Performance")

# performance_col1, performance_col2, \
#     performance_col3, performance_col4 = st.columns(4)

# overall_rmse = overall_performance.get("rmse")

# with performance_col1:

#     st.metric(
#         "5-min MAE",
#         (
#             f"{overview_mae:.3f}°C"
#             if overview_mae is not None
#             else "N/A"
#         ),
#     )

# with performance_col2:

#     st.metric(
#         "5-min RMSE",
#         (
#             f"{overall_rmse:.3f}°C"
#             if overall_rmse is not None
#             else "N/A"
#         ),
#     )

# with performance_col3:

#     st.metric(
#         "Evaluated forecasts",
#         overall_performance.get("evaluated", 0),
#     )

# with performance_col4:

#     st.metric(
#         "Pending forecasts",
#         overall_performance.get("pending", 0),
#     )


# with st.expander("Per-module forecast performance"):

#     if per_module_performance:

#         performance_rows = []

#         for module_name, performance in per_module_performance.items():

#             performance_rows.append(
#                 {
#                     "Module": module_display_name(module_name),
#                     "Evaluated": performance.get("evaluated", 0),
#                     "Pending": performance.get("pending", 0),
#                     "MAE °C": performance.get("mae"),
#                     "RMSE °C": performance.get("rmse"),
#                 }
#             )

#         performance_df = pd.DataFrame(performance_rows).sort_values(
#             "Module"
#         )

#         st.dataframe(
#             performance_df,
#             width="stretch",
#             hide_index=True,
#         )

#     else:

#         st.caption("No forecast performance data available yet.")


# # ============================================================
# # FOOTER
# # ============================================================

# st.divider()

# st.caption(
#     "SentinelDC | "
#     "5-Minute XGBoost Forecasting | "
#     "Kafka + FastAPI + SQLite + Streamlit"
# )

# st.caption(
#     "Dashboard automatically refreshes every 3 seconds."
# )


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

SPEED_OPTIONS = [1, 2, 5, 10, 20]


# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="SentinelDC | Data Center Intelligence",
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


def format_temperature(value):

    if value is None:

        return "N/A"

    return f"{float(value):.2f}°C"


def module_display_name(module_name):

    return module_name.replace(
        "_Avg_Temp",
        "",
    )


def module_sort_key(module_name):

    try:

        return int(
            module_name.split("_")[1]
        )

    except (IndexError, ValueError):

        return 999



# ============================================================
# RECRUITER-FACING UI THEME
# ============================================================

st.markdown(
    """
    <style>
    :root {
        --navy: #0f172a;
        --navy2: #172554;
        --blue: #2563eb;
        --cyan: #0891b2;
        --green: #16a34a;
        --amber: #d97706;
        --red: #dc2626;
        --slate: #475569;
        --muted: #64748b;
        --bg: #f5f7fb;
        --card: #ffffff;
        --border: #e2e8f0;
    }

    .stApp {
        background: var(--bg);
    }

    .block-container {
        max-width: 1500px;
        padding-top: 1.2rem;
        padding-bottom: 2.2rem;
    }

    .hero {
        background: linear-gradient(
            135deg,
            #0f172a 0%,
            #172554 58%,
            #075985 100%
        );
        border-radius: 20px;
        padding: 1.45rem 1.6rem;
        color: white;
        box-shadow: 0 12px 30px rgba(15, 23, 42, .18);
        margin-bottom: 1.15rem;
    }

    .hero-title {
        font-size: 2rem;
        font-weight: 800;
        letter-spacing: -.025em;
        margin: 0;
    }

    .hero-subtitle {
        color: #dbeafe;
        font-size: .96rem;
        margin-top: .35rem;
    }

    .hero-meta {
        color: #bfdbfe;
        font-size: .76rem;
        margin-top: .72rem;
    }

    .section-title {
        color: var(--navy);
        font-size: 1.25rem;
        font-weight: 800;
        margin-top: 1.3rem;
        margin-bottom: .18rem;
    }

    .section-caption {
        color: var(--muted);
        font-size: .84rem;
        margin-bottom: .75rem;
    }

    .health-card {
        background: var(--card);
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: .82rem .92rem;
        min-height: 84px;
        box-shadow: 0 3px 12px rgba(15, 23, 42, .045);
    }

    .health-label {
        color: var(--muted);
        font-size: .70rem;
        font-weight: 750;
        text-transform: uppercase;
        letter-spacing: .055em;
    }

    .health-value {
        color: var(--navy);
        font-size: .96rem;
        font-weight: 800;
        margin-top: .28rem;
    }

    .dot {
        display: inline-block;
        width: 9px;
        height: 9px;
        border-radius: 50%;
        margin-right: 7px;
        vertical-align: 1px;
    }

    .dot-green { background: var(--green); }
    .dot-blue { background: var(--blue); }
    .dot-amber { background: var(--amber); }
    .dot-red { background: var(--red); }
    .dot-gray { background: #94a3b8; }

    .module-shell {
        background: var(--card);
        border: 1px solid var(--border);
        border-radius: 17px;
        padding: 1rem 1rem .9rem;
        margin-bottom: .95rem;
        box-shadow: 0 5px 18px rgba(15, 23, 42, .055);
        border-top: 4px solid var(--blue);
    }

    .module-shell.normal { border-top-color: var(--green); }
    .module-shell.warning { border-top-color: var(--amber); }
    .module-shell.critical { border-top-color: var(--red); }

    .module-title {
        color: var(--navy);
        font-size: 1.05rem;
        font-weight: 800;
    }

    .module-subtitle {
        color: var(--muted);
        font-size: .75rem;
        margin-top: .15rem;
    }

    .badge {
        display: inline-block;
        padding: .24rem .58rem;
        border-radius: 999px;
        font-size: .68rem;
        font-weight: 800;
        letter-spacing: .04em;
    }

    .badge-normal {
        color: #166534;
        background: #dcfce7;
    }

    .badge-warning {
        color: #92400e;
        background: #fef3c7;
    }

    .badge-critical {
        color: #991b1b;
        background: #fee2e2;
    }

    .badge-unknown {
        color: #475569;
        background: #e2e8f0;
    }

    .performance-panel {
        background: #f8fafc;
        border: 1px solid #e5e7eb;
        border-radius: 12px;
        padding: .62rem .72rem;
        margin-top: .65rem;
    }

    .performance-heading {
        color: var(--navy);
        font-size: .78rem;
        font-weight: 800;
        margin-bottom: .42rem;
    }

    .alert-box {
        border-radius: 13px;
        padding: .78rem .95rem;
        margin-bottom: .55rem;
        border-left: 5px solid;
    }

    .alert-critical {
        border-color: var(--red);
        background: #fef2f2;
    }

    .alert-warning {
        border-color: var(--amber);
        background: #fffbeb;
    }

    .alert-title {
        color: var(--navy);
        font-weight: 800;
    }

    .alert-text {
        color: var(--slate);
        font-size: .82rem;
        margin-top: .16rem;
    }

    .trend-panel {
        background: white;
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: .8rem;
    }

    div[data-testid="stMetric"] {
        background: white;
        border: 1px solid var(--border);
        border-radius: 11px;
        padding: .68rem .78rem;
        box-shadow: 0 2px 9px rgba(15, 23, 42, .035);
    }

    div[data-testid="stMetricLabel"] {
        color: var(--muted);
    }

    div[data-testid="stMetricValue"] {
        color: var(--navy);
    }

    .stButton > button {
        border-radius: 9px;
        font-weight: 700;
    }

    .footer-note {
        text-align: center;
        color: var(--muted);
        font-size: .76rem;
        line-height: 1.55;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ============================================================
# HEADER
# ============================================================

st.markdown(
    """
    <div class="hero">
        <div class="hero-title">🌡️ SentinelDC</div>
        <div class="hero-subtitle">
            AI-Powered Data Center Temperature Monitoring & Predictive Alert Platform
        </div>
        <div class="hero-meta">
            XGBoost · 5-minute forecasting · Real-time monitoring · Kafka · FastAPI · SQLite
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# AUTO REFRESH
# ============================================================

st_autorefresh(
    interval=REFRESH_INTERVAL,
    key="sentineldc_refresh",
)


# ============================================================
# LOAD CORE DATA
# ============================================================

pipeline = get_api_data("/monitoring/system-status")

modules = get_api_data("/monitoring/modules")

alerts = get_api_data("/monitoring/alerts?limit=10")

forecast_performance = get_api_data(
    "/monitoring/forecast-performance"
)


if pipeline:

    simulation = pipeline.get("simulation", {})
    kafka = pipeline.get("kafka", {})
    database = pipeline.get("database", {})
    api_component = pipeline.get("api", {})
    model_component = pipeline.get("model", {})
    inference = pipeline.get("ml_inference", {})

else:

    simulation = {}
    kafka = {}
    database = {}
    api_component = {}
    model_component = {}
    inference = {}


if forecast_performance:

    overall_performance = forecast_performance.get(
        "overall", {}
    )

    per_module_performance = forecast_performance.get(
        "modules", {}
    )

else:

    overall_performance = {}

    per_module_performance = {}


if not isinstance(modules, list):

    modules = []

modules = sorted(
    modules,
    key=lambda module: module_sort_key(
        module.get("module", "")
    ),
)


# ============================================================
# SYSTEM STATUS
# ============================================================

status_col1, status_col2, status_col3, status_col4, status_col5 = st.columns(5)


with status_col1:

    st.metric(
        "API",
        api_component.get("status", "UNKNOWN"),
    )


with status_col2:

    kafka_healthy = kafka.get("healthy", False)

    st.metric(
        "Kafka",
        "HEALTHY" if kafka_healthy else "UNREACHABLE",
    )


with status_col3:

    st.metric(
        "Database",
        database.get("status", "UNKNOWN"),
    )


with status_col4:

    st.metric(
        "Model",
        model_component.get("status", "UNKNOWN"),
    )


with status_col5:

    st.metric(
        "Inference worker",
        inference.get("status", "UNKNOWN"),
    )

    worker_error = inference.get("worker", {}).get("last_error")

    if worker_error:

        st.caption(worker_error)


# ============================================================
# SIMULATION CONTROL
# ============================================================

st.header("Simulation Control")

simulation_speed = simulation.get("speed", 1.0)

control_col1, control_col2, control_col3, \
    control_col4, control_col5 = st.columns(5)


with control_col1:

    selected_speed = st.selectbox(
        "Speed",
        SPEED_OPTIONS,
        index=(
            SPEED_OPTIONS.index(int(simulation_speed))
            if int(simulation_speed) in SPEED_OPTIONS
            else 2
        ),
    )

    if selected_speed != simulation_speed:

        post_api_data(
            "/simulation/speed",
            {"speed": float(selected_speed)},
        )

        st.rerun()


with control_col2:

    if st.button("▶ Start"):

        result = post_api_data(
            "/simulation/start",
            {"speed": float(selected_speed)},
        )

        if result:

            st.rerun()


with control_col3:

    if st.button("⏸ Pause"):

        result = post_api_data("/simulation/pause")

        if result:

            st.rerun()


with control_col4:

    if st.button("▶ Resume"):

        result = post_api_data(
            "/simulation/start",
            {"speed": float(selected_speed)},
        )

        if result:

            st.rerun()


with control_col5:

    if st.button("■ Stop"):

        result = post_api_data("/simulation/stop")

        if result:

            st.rerun()


simulation_status = simulation.get("status", "UNKNOWN")

records_sent = simulation.get("records_sent", 0)

total_records = simulation.get("total_records", 0)

last_event = simulation.get("last_event_timestamp") or "—"

st.caption(
    f"Status: **{simulation_status}** | "
    f"Records sent: {records_sent}/{total_records} | "
    f"Last event: {last_event}"
)


# ============================================================
# MODULE HEALTH
# ============================================================

st.markdown(
    '<div class="section-title">Independent Module Monitoring</div>',
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="section-caption">
        Each module is presented independently so live temperature,
        5-minute prediction, risk state and forecasting accuracy are
        easy to interpret without mixing module results.
    </div>
    """,
    unsafe_allow_html=True,
)

if modules:

    for index, module in enumerate(modules, start=1):

        module_name = module.get("module", "Unknown")
        current = module.get("current_temperature")
        predicted = module.get("predicted_temperature")
        display_name = module_display_name(module_name)

        performance = per_module_performance.get(
            module_name,
            {},
        )

        module_mae = performance.get("mae")
        module_rmse = performance.get("rmse")
        module_evaluated = performance.get("evaluated", 0)
        module_pending = performance.get("pending", 0)

        risk = str(
            module.get("risk_level", "UNKNOWN")
        ).upper()

        forecast_delta = None

        if current is not None and predicted is not None:
            forecast_delta = float(predicted) - float(current)

        if risk == "CRITICAL":
            risk_class = "critical"
            risk_badge = "badge-critical"
        elif risk == "WARNING":
            risk_class = "warning"
            risk_badge = "badge-warning"
        elif risk == "NORMAL":
            risk_class = "normal"
            risk_badge = "badge-normal"
        else:
            risk_class = "normal"
            risk_badge = "badge-unknown"

        st.markdown(
            f"""
            <div class="module-shell {risk_class}">
                <div style="
                    display:flex;
                    justify-content:space-between;
                    align-items:center;
                    gap:14px;
                ">
                    <div>
                        <div class="module-title">
                            Module {index} · {display_name}
                        </div>
                        <div class="module-subtitle">
                            Live sensor state and 5-minute XGBoost prediction
                        </div>
                    </div>
                    <span class="badge {risk_badge}">
                        {risk}
                    </span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        live_col1, live_col2, live_col3 = st.columns(3)

        with live_col1:

            st.metric(
                "Current Temperature",
                format_temperature(current),
            )

        with live_col2:

            st.metric(
                "5-Min Forecast",
                format_temperature(predicted),
                delta=(
                    f"{forecast_delta:+.2f}°C"
                    if forecast_delta is not None
                    else None
                ),
            )

        with live_col3:

            if forecast_delta is None:
                movement = "N/A"
            elif forecast_delta > 0:
                movement = "↑ Expected rise"
            elif forecast_delta < 0:
                movement = "↓ Expected fall"
            else:
                movement = "→ Stable"

            st.markdown(
                f"""
                <div class="performance-panel">
                    <div class="performance-heading">
                        FORECAST MOVEMENT
                    </div>
                    <div style="
                        color:#0f172a;
                        font-size:1rem;
                        font-weight:800;
                    ">
                        {movement}
                    </div>
                    <div style="
                        color:#64748b;
                        font-size:.76rem;
                        margin-top:.12rem;
                    ">
                        {
                            f"Delta: {forecast_delta:+.2f}°C"
                            if forecast_delta is not None
                            else "No valid delta"
                        }
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        st.markdown(
            """
            <div class="performance-panel">
                <div class="performance-heading">
                    MODULE-SPECIFIC FORECAST PERFORMANCE
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        perf_col1, perf_col2, perf_col3, perf_col4 = st.columns(4)

        with perf_col1:

            st.metric(
                "MAE",
                (
                    f"{module_mae:.3f}°C"
                    if module_mae is not None
                    else "N/A"
                ),
            )

        with perf_col2:

            st.metric(
                "RMSE",
                (
                    f"{module_rmse:.3f}°C"
                    if module_rmse is not None
                    else "N/A"
                ),
            )

        with perf_col3:

            st.metric(
                "Evaluated",
                module_evaluated,
            )

        with perf_col4:

            st.metric(
                "Pending",
                module_pending,
            )

        if module_mae is not None:

            if module_mae < 0.5:
                accuracy_text = "Strong forecasting accuracy"
                accuracy_badge = "badge-normal"

            elif module_mae < 1.0:
                accuracy_text = "Good forecasting accuracy"
                accuracy_badge = "badge-normal"

            else:
                accuracy_text = "Needs model attention"
                accuracy_badge = "badge-warning"

            st.markdown(
                f"""
                <div style="
                    margin:.4rem 0 1rem;
                    color:#64748b;
                    font-size:.76rem;
                ">
                    <span class="badge {accuracy_badge}">
                        {accuracy_text}
                    </span>
                    <span style="margin-left:.35rem;">
                        Based on this module's MAE.
                    </span>
                </div>
                """,
                unsafe_allow_html=True,
            )

        st.divider()

else:

    st.info("No module data available.")

# ============================================================
# ACTIVE ALERT DATA PREPARATION
# ============================================================

# Keep this data preparation separate from the UI. The previous
# Live Overview section defined active_alert_modules; because that
# UI section was removed, the variable must still be created here.
active_alert_modules = [
    module
    for module in modules
    if module.get("risk_level") in ("WARNING", "CRITICAL")
]

warning_count = sum(
    1
    for module in modules
    if module.get("risk_level") == "WARNING"
)

critical_count = sum(
    1
    for module in modules
    if module.get("risk_level") == "CRITICAL"
)


# ============================================================
# ACTIVE ALERTS
# ============================================================

st.markdown(
    '<div class="section-title">Active Alerts</div>',
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="section-caption">
        A module becomes active when its 5-minute forecast reaches or
        exceeds its warning threshold. Critical forecasts are prioritized.
    </div>
    """,
    unsafe_allow_html=True,
)

# IMPORTANT:
# modules are returned by the API after the decision engine has evaluated
# the latest forecast. Therefore risk_level is the authoritative active
# alert state for the current dashboard snapshot.
active_alert_modules = [
    module
    for module in modules
    if module.get("risk_level") in {
        "WARNING",
        "CRITICAL"
    }
]

critical_modules = [
    module
    for module in active_alert_modules
    if module.get("risk_level") == "CRITICAL"
]

warning_modules = [
    module
    for module in active_alert_modules
    if module.get("risk_level") == "WARNING"
]

# Only show the total number of active alert modules here.
# Severity is still displayed on each individual alert below.
st.metric(
    "Active Alert Modules",
    len(active_alert_modules)
)

if active_alert_modules:

    # Critical modules first, then warning modules.
    sorted_active_alerts = sorted(
        active_alert_modules,
        key=lambda module: (
            0
            if module.get("risk_level") == "CRITICAL"
            else 1
        )
    )

    for module in sorted_active_alerts:

        module_name = module.get(
            "module",
            "Unknown"
        )

        display_name = module_display_name(
            module_name
        )

        risk = module.get(
            "risk_level",
            "UNKNOWN"
        )

        current = format_temperature(
            module.get(
                "current_temperature"
            )
        )

        predicted = format_temperature(
            module.get(
                "predicted_temperature"
            )
        )

        delta = module.get(
            "temperature_delta"
        )

        warning_threshold = module.get(
            "warning_threshold"
        )

        critical_threshold = module.get(
            "critical_threshold"
        )

        if risk == "CRITICAL":
            alert_class = "alert-critical"
        else:
            alert_class = "alert-warning"

        delta_text = (
            f"{float(delta):+.2f}°C"
            if delta is not None
            else "N/A"
        )

        threshold_text = (
            f"Warning ≥ {float(warning_threshold):.2f}°C"
            if warning_threshold is not None
            else "Warning threshold unavailable"
        )

        critical_text = (
            f"Critical ≥ {float(critical_threshold):.2f}°C"
            if critical_threshold is not None
            else "Critical threshold unavailable"
        )

        st.markdown(
            f"""
            <div class="alert-box {alert_class}">
                <div class="alert-title">
                    {risk} · {display_name}
                </div>
                <div class="alert-text">
                    5-minute forecast: <strong>{predicted}</strong>
                    &nbsp; | &nbsp;
                    Current: {current}
                    &nbsp; | &nbsp;
                    Forecast delta: {delta_text}
                    <br>
                    {threshold_text}
                    &nbsp; | &nbsp;
                    {critical_text}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

else:

    st.success(
        "No active alerts. Every module's 5-minute forecast is below its warning threshold."
    )


# ============================================================
# ALERT HISTORY DROPDOWN
# ============================================================

with st.expander(
    "▾ Alert History",
    expanded=False
):

    if isinstance(alerts, list) and alerts:

        history_df = pd.DataFrame(alerts)

        rename_map = {
            "timestamp": "Timestamp",
            "module": "Module",
            "event_type": "Event",
            "severity": "Severity",
            "alert_level": "Severity",
            "current_temperature": "Current °C",
            "predicted_temperature": "5-Min Forecast °C",
            "temperature_delta": "Delta °C",
            "warning_threshold": "Warning Threshold °C",
            "critical_threshold": "Critical Threshold °C",
            "message": "Message",
        }

        history_df = history_df.rename(
            columns=rename_map
        )

        # Keep the useful fields, while accepting both the old and
        # new JSONL event schemas.
        preferred_columns = [
            "Timestamp",
            "Module",
            "Event",
            "Current °C",
            "5-Min Forecast °C",
            "Delta °C",
            "Warning Threshold °C",
            "Critical Threshold °C",
        ]

        display_columns = [
            column
            for column in preferred_columns
            if column in history_df.columns
        ]

        st.dataframe(
            history_df[display_columns],
            width="stretch",
            hide_index=True,
        )

    else:

        st.caption(
            "No alert or recovery events have been recorded yet."
        )

# ============================================================
# TEMPERATURE TREND
# ============================================================

st.markdown(
    '<div class="section-title">Temperature Trends</div>',
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="section-caption">
        Select a module to inspect its historical temperature and
        5-minute forecast behavior.
    </div>
    """,
    unsafe_allow_html=True,
)

available_modules = [
    module.get("module")
    for module in modules
    if module.get("module")
]

if available_modules:

    # Module dropdown is intentionally placed inside the Temperature
    # Trends section. Changing it changes the API history endpoint and
    # therefore changes the chart without affecting any other section.
    selected_module = st.selectbox(
        "Select Module",
        available_modules,
        format_func=module_display_name,
        key="temperature_trend_module",
    )

    history = get_api_data(
        f"/monitoring/modules/{selected_module}/history?limit=100"
    )

    if isinstance(history, list) and history:

        history_df = pd.DataFrame(history)

        if "timestamp" in history_df.columns:

            history_df["timestamp"] = pd.to_datetime(
                history_df["timestamp"],
                errors="coerce",
            )

            history_df = (
                history_df
                .dropna(subset=["timestamp"])
                .sort_values("timestamp")
                .set_index("timestamp")
            )

        chart_columns = [
            column
            for column in (
                "current_temperature",
                "predicted_temperature",
            )
            if column in history_df.columns
        ]

        if chart_columns:

            chart_df = history_df[chart_columns].copy()

            chart_df.columns = [
                (
                    "Current"
                    if column == "current_temperature"
                    else "5-min Forecast"
                )
                for column in chart_df.columns
            ]

            st.line_chart(
                chart_df,
                width="stretch",
            )

            st.caption(
                f"Showing temperature trend for "
                f"{module_display_name(selected_module)}."
            )

        else:

            st.info(
                "Temperature columns are not available."
            )

    else:

        st.info(
            f"No temperature history available yet for "
            f"{module_display_name(selected_module)}."
        )

else:

    st.info("No module data available.")


# ============================================================
# FOOTER
# ============================================================

st.divider()

st.markdown(
    """
    <div class="footer-note">
        <strong>SentinelDC</strong> |
        5-Minute XGBoost Forecasting |
        Kafka + FastAPI + SQLite + Streamlit
        <br>
        Dashboard automatically refreshes every 3 seconds.
    </div>
    """,
    unsafe_allow_html=True,
)