import time
import requests
import pandas as pd
import joblib


# ============================================================
# CONFIGURATION
# ============================================================

DATA_PATH = "data/processed/replay_data.csv"

FEATURE_FILE = "models/feature_columns.pkl"

API_URL = "http://127.0.0.1:8000/predict"

# 1 = approximately real-time
# 10 = 10x faster than historical time
# 60 = 60x faster
REPLAY_SPEED = 10

# Number of records to replay
MAX_RECORDS = 100


# ============================================================
# LOAD DATA
# ============================================================

print("=" * 60)
print("SENTINELDC REPLAY ENGINE")
print("=" * 60)

print("\nLoading replay dataset...")

df = pd.read_csv(DATA_PATH)

print("Dataset shape:", df.shape)


# ============================================================
# LOAD MODEL FEATURES
# ============================================================

feature_columns = joblib.load(FEATURE_FILE)

print("Model features:", len(feature_columns))


# ============================================================
# CHECK FEATURES
# ============================================================

missing_features = [
    col for col in feature_columns
    if col not in df.columns
]

if missing_features:

    print("\nMissing features:")

    for feature in missing_features:
        print(feature)

    raise ValueError(
        f"{len(missing_features)} model features are missing."
    )


print("Feature validation: PASSED")


# ============================================================
# PREPARE DATA
# ============================================================

# Keep timestamp separately for replay timing
df["Time [Date/Time]"] = pd.to_datetime(
    df["Time [Date/Time]"],
    errors="coerce"
)

df = df.sort_values(
    "Time [Date/Time]"
).reset_index(drop=True)


# ============================================================
# START REPLAY
# ============================================================

print("\nStarting replay...")
print("Replay speed:", REPLAY_SPEED, "x")
print("Records:", MAX_RECORDS)

print("-" * 60)


previous_timestamp = None


for index, row in df.head(MAX_RECORDS).iterrows():

    current_timestamp = row["Time [Date/Time]"]


    # --------------------------------------------------------
    # Calculate historical time difference
    # --------------------------------------------------------

    if previous_timestamp is not None:

        historical_gap = (
            current_timestamp - previous_timestamp
        ).total_seconds()

        # Convert historical time to demo time
        demo_delay = historical_gap / REPLAY_SPEED

        # Prevent excessively long waits
        demo_delay = min(demo_delay, 2)

        # Avoid negative delays
        demo_delay = max(demo_delay, 0)

        time.sleep(demo_delay)


    previous_timestamp = current_timestamp


    # --------------------------------------------------------
    # Build API payload
    # --------------------------------------------------------

    payload = {}

    for feature in feature_columns:

        value = row[feature]

        # Convert NumPy values to normal Python float
        payload[feature] = float(value)


    # --------------------------------------------------------
    # Send prediction request
    # --------------------------------------------------------

    try:

        response = requests.post(
            API_URL,
            json=payload,
            timeout=10
        )


        # ----------------------------------------------------
        # Successful prediction
        # ----------------------------------------------------

        if response.status_code == 200:

            result = response.json()

            prediction = result["prediction"]

            print(
                f"[{current_timestamp}] "
                f"Prediction: {prediction:.2f} °C"
            )


        # ----------------------------------------------------
        # API validation/error
        # ----------------------------------------------------

        else:

            print(
                f"[{current_timestamp}] "
                f"API ERROR {response.status_code}"
            )

            print(response.text)


    except requests.exceptions.RequestException as e:

        print(
            f"[{current_timestamp}] "
            f"CONNECTION ERROR: {e}"
        )

        print(
            "\nIs FastAPI running?"
        )

        break


print("\n" + "=" * 60)
print("REPLAY COMPLETED")
print("=" * 60)