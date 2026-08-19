from pathlib import Path
import pandas as pd


# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]

INPUT_PATH = (
    BASE_DIR
    / "data"
    / "processed"
    / "replay_data.csv"
)

OUTPUT_PATH = (
    BASE_DIR
    / "data"
    / "processed"
    / "forecasting_data.csv"
)

TIMESTAMP_COLUMN = "Time [Date/Time]"

FORECAST_HORIZON_MINUTES = 5

MODULE_COLUMNS = [
    f"Module_{i}_Avg_Temp"
    for i in range(1, 9)
]

TARGET_COLUMNS = [
    f"Target_Module_{i}"
    for i in range(1, 9)
]


# ============================================================
# LOAD
# ============================================================

def load_data():

    print("=" * 70)
    print("SENTINELDC — 5-MINUTE FORECAST DATA PREPARATION")
    print("=" * 70)

    print(f"\nLoading:\n{INPUT_PATH}")

    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"Dataset not found:\n{INPUT_PATH}"
        )

    df = pd.read_csv(INPUT_PATH)

    print(f"Rows loaded: {len(df):,}")

    return df


# ============================================================
# VALIDATE
# ============================================================

def validate_columns(df):

    required = [
        TIMESTAMP_COLUMN,
        *MODULE_COLUMNS,
    ]

    missing = [
        col
        for col in required
        if col not in df.columns
    ]

    if missing:
        raise ValueError(
            "Missing required columns:\n"
            + "\n".join(
                f"  - {col}"
                for col in missing
            )
        )

    print("\nRequired columns validated.")


# ============================================================
# PREPARE TIMESTAMP
# ============================================================

def prepare_timestamp(df):

    df = df.copy()

    df[TIMESTAMP_COLUMN] = pd.to_datetime(
        df[TIMESTAMP_COLUMN],
        errors="coerce",
    )

    invalid = (
        df[TIMESTAMP_COLUMN]
        .isna()
        .sum()
    )

    if invalid:
        raise ValueError(
            f"Found {invalid} invalid timestamps."
        )

    df = df.sort_values(
        TIMESTAMP_COLUMN
    ).reset_index(drop=True)

    return df


# ============================================================
# REMOVE OLD TARGETS
# ============================================================

def remove_old_targets(df):

    existing = [
        col
        for col in TARGET_COLUMNS
        if col in df.columns
    ]

    if existing:

        print("\nRemoving old target columns:")

        for col in existing:
            print(f"  - {col}")

        df = df.drop(
            columns=existing
        )

    return df


# ============================================================
# NORMALIZE DUPLICATE TIMESTAMPS
# ============================================================

def normalize_timeline(df):

    print(
        "\nChecking timestamp structure..."
    )

    unique_before = (
        df[TIMESTAMP_COLUMN]
        .nunique()
    )

    duplicates = (
        len(df)
        - unique_before
    )

    print(
        f"Unique timestamps: "
        f"{unique_before:,}"
    )

    print(
        f"Duplicate rows: "
        f"{duplicates:,}"
    )

    if duplicates == 0:

        print(
            "No duplicate timestamps found."
        )

        return df

    print(
        "\nAggregating duplicate timestamps..."
    )

    # --------------------------------------------------------
    # Separate numeric telemetry from other columns
    # --------------------------------------------------------

    numeric_columns = (
        df.select_dtypes(
            include="number"
        )
        .columns
        .tolist()
    )

    # Never aggregate identifiers as if they were
    # temperatures.
    numeric_columns = [
        col
        for col in numeric_columns
        if col not in TARGET_COLUMNS
    ]

    # --------------------------------------------------------
    # For duplicate timestamps:
    #
    # numeric telemetry → mean
    # non-numeric metadata → first
    # --------------------------------------------------------

    aggregation = {}

    for col in df.columns:

        if col == TIMESTAMP_COLUMN:
            continue

        if col in numeric_columns:
            aggregation[col] = "mean"
        else:
            aggregation[col] = "first"

    df = (
        df
        .groupby(
            TIMESTAMP_COLUMN,
            as_index=False
        )
        .agg(aggregation)
    )

    df = df.sort_values(
        TIMESTAMP_COLUMN
    ).reset_index(drop=True)

    print(
        f"Rows after timestamp normalization: "
        f"{len(df):,}"
    )

    print(
        f"Unique timestamps after normalization: "
        f"{df[TIMESTAMP_COLUMN].nunique():,}"
    )

    return df


# ============================================================
# CHECK SAMPLING FREQUENCY
# ============================================================

def check_sampling_frequency(df):

    timestamps = (
        df[TIMESTAMP_COLUMN]
        .drop_duplicates()
        .sort_values()
    )

    differences = (
        timestamps
        .diff()
        .dropna()
    )

    print(
        "\nSampling interval distribution:"
    )

    print(
        differences
        .value_counts()
        .head(10)
    )

    one_second_ratio = (
        (differences == pd.Timedelta(seconds=1))
        .mean()
    )

    print(
        f"\n1-second interval ratio: "
        f"{one_second_ratio:.4%}"
    )

    if one_second_ratio < 0.95:

        print(
            "\nWARNING:"
            " Dataset is not consistently sampled at 1 Hz."
        )

    else:

        print(
            "\nSampling frequency confirmed:"
            " approximately 1 Hz."
        )


# ============================================================
# CREATE FUTURE TARGETS
# ============================================================

def create_targets(df):

    print(
        "\nCreating timestamp-based "
        f"+{FORECAST_HORIZON_MINUTES}-minute targets..."
    )

    current = df.copy()

    # --------------------------------------------------------
    # Prediction timestamp
    # --------------------------------------------------------

    current["_prediction_time"] = (
        current[TIMESTAMP_COLUMN]
    )

    # --------------------------------------------------------
    # Desired future timestamp
    # --------------------------------------------------------

    current["_target_time"] = (
        current[TIMESTAMP_COLUMN]
        + pd.Timedelta(
            minutes=FORECAST_HORIZON_MINUTES
        )
    )

    # --------------------------------------------------------
    # Future lookup table
    # --------------------------------------------------------

    future = df[
        [
            TIMESTAMP_COLUMN,
            *MODULE_COLUMNS,
        ]
    ].copy()

    future = future.rename(
        columns={
            TIMESTAMP_COLUMN:
                "_future_timestamp"
        }
    )

    # --------------------------------------------------------
    # Match exact future timestamp
    #
    # Since we have now normalized the timeline
    # to 1-second observations, exact matching
    # is appropriate.
    # --------------------------------------------------------

    merged = pd.merge(
        current,
        future,
        left_on="_target_time",
        right_on="_future_timestamp",
        how="left",
        suffixes=(
            "",
            "_future"
        ),
    )

    # --------------------------------------------------------
    # Rename future temperatures
    # --------------------------------------------------------

    for module in range(1, 9):

        source = (
            f"Module_{module}_Avg_Temp_future"
        )

        target = (
            f"Target_Module_{module}"
        )

        if source in merged.columns:

            merged[target] = (
                merged[source]
            )

    # --------------------------------------------------------
    # Remove helper future columns
    # --------------------------------------------------------

    future_columns = [
        f"{module}_future"
        for module in MODULE_COLUMNS
    ]

    future_columns.append(
        "_future_timestamp"
    )

    existing_future_columns = [
        col
        for col in future_columns
        if col in merged.columns
    ]

    merged = merged.drop(
        columns=existing_future_columns
    )

    # --------------------------------------------------------
    # Remove rows without a complete
    # five-minute future observation.
    # --------------------------------------------------------

    before = len(merged)

    merged = merged.dropna(
        subset=TARGET_COLUMNS
    )

    merged = merged.reset_index(
        drop=True
    )

    removed = (
        before - len(merged)
    )

    print(
        f"\nRows without a valid "
        f"T+{FORECAST_HORIZON_MINUTES} target: "
        f"{removed:,}"
    )

    print(
        f"Final forecasting rows: "
        f"{len(merged):,}"
    )

    return merged


# ============================================================
# VALIDATE TARGET
# ============================================================

def validate_targets(df):

    print(
        "\nValidating forecast horizon..."
    )

    expected = (
        df["_prediction_time"]
        + pd.Timedelta(
            minutes=FORECAST_HORIZON_MINUTES
        )
    )

    difference = (
        df["_target_time"]
        - expected
    ).abs()

    maximum_difference = (
        difference.max()
    )

    print(
        "Maximum target-time difference:",
        maximum_difference
    )

    if maximum_difference != pd.Timedelta(0):

        raise ValueError(
            "Target time is not exactly "
            "5 minutes after prediction time."
        )

    # --------------------------------------------------------
    # Verify actual future timestamps
    # --------------------------------------------------------

    actual_future = (
        df["_target_time"]
        - df["_prediction_time"]
    )

    expected_delta = pd.Timedelta(
        minutes=FORECAST_HORIZON_MINUTES
    )

    if not (
        actual_future == expected_delta
    ).all():

        raise ValueError(
            "Some targets do not have "
            "the correct forecast horizon."
        )

    print(
        "✓ Forecast horizon verified:"
        f" +{FORECAST_HORIZON_MINUTES} minutes"
    )

    # --------------------------------------------------------
    # Print example
    # --------------------------------------------------------

    example = df.iloc[0]

    print("\nExample forecast target:")

    print(
        f"Prediction time : "
        f"{example['_prediction_time']}"
    )

    print(
        f"Target time     : "
        f"{example['_target_time']}"
    )

    print(
        f"Module 1 current: "
        f"{example['Module_1_Avg_Temp']:.3f}°C"
    )

    print(
        f"Module 1 target : "
        f"{example['Target_Module_1']:.3f}°C"
    )


# ============================================================
# SAVE
# ============================================================

def save(df):

    OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    df.to_csv(
        OUTPUT_PATH,
        index=False
    )

    print(
        "\nForecasting dataset saved:"
    )

    print(
        OUTPUT_PATH
    )

    print(
        f"Rows    : {len(df):,}"
    )

    print(
        f"Columns : {len(df.columns):,}"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    df = load_data()

    validate_columns(
        df
    )

    df = prepare_timestamp(
        df
    )

    df = remove_old_targets(
        df
    )

    df = normalize_timeline(
        df
    )

    check_sampling_frequency(
        df
    )

    df = create_targets(
        df
    )

    validate_targets(
        df
    )

    save(
        df
    )

    print(
        "\n" + "=" * 70
    )

    print(
        "AMENDMENT 1B COMPLETE"
    )

    print(
        "=" * 70
    )


if __name__ == "__main__":
    main()