from pathlib import Path
import sqlite3


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"
DATABASE_PATH = DATA_DIR / "sentineldc.db"


# ============================================================
# DATABASE INITIALIZATION
# ============================================================

def get_connection():
    """
    Create and return a SQLite database connection.

    Returns
    -------
    sqlite3.Connection
        Active SQLite connection.
    """

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(
        DATABASE_PATH,
        check_same_thread=False,
        timeout=10,
    )

    connection.row_factory = sqlite3.Row
    # WAL lets the API serve the dashboard while the consumer writes telemetry.
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=10000")
    connection.execute("PRAGMA foreign_keys=ON")

    return connection


def initialize_database():
    """
    Create SentinelDC database tables if they do not exist.
    """

    connection = get_connection()

    cursor = connection.cursor()

    # --------------------------------------------------------
    # TELEMETRY PREDICTIONS
    # --------------------------------------------------------

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS predictions (

            id INTEGER PRIMARY KEY AUTOINCREMENT,

            timestamp TEXT NOT NULL,

            replay_id INTEGER,

            kafka_partition INTEGER,

            kafka_offset INTEGER,

            module TEXT NOT NULL,

            current_temperature REAL,

            predicted_temperature REAL,

            temperature_delta REAL,

            warning_threshold REAL,

            critical_threshold REAL,

            risk_level TEXT NOT NULL
        )
        """
    )

    # Query paths used by the dashboard and idempotency checks used by the
    # Kafka worker.  INDEX (rather than a new UNIQUE constraint) keeps this
    # migration safe for existing demo databases.
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_predictions_module_timestamp "
        "ON predictions(module, timestamp DESC)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_predictions_kafka "
        "ON predictions(kafka_partition, kafka_offset, module)"
    )

    # --------------------------------------------------------
    # ALERTS
    # --------------------------------------------------------

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS alerts (

            id INTEGER PRIMARY KEY AUTOINCREMENT,

            timestamp TEXT NOT NULL,

            replay_id INTEGER,

            kafka_partition INTEGER,

            kafka_offset INTEGER,

            module TEXT NOT NULL,

            alert_level TEXT NOT NULL,

            predicted_temperature REAL,

            current_temperature REAL,

            warning_threshold REAL,

            critical_threshold REAL,

            message TEXT NOT NULL
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_alerts_module_timestamp "
        "ON alerts(module, timestamp DESC)"
    )

    # --------------------------------------------------------
    # SYSTEM EVENTS
    # --------------------------------------------------------

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS system_events (

            id INTEGER PRIMARY KEY AUTOINCREMENT,

            timestamp TEXT NOT NULL,

            event_type TEXT NOT NULL,

            status TEXT,

            message TEXT
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_system_events_timestamp "
        "ON system_events(timestamp DESC)"
    )

    connection.commit()

    connection.close()


if __name__ == "__main__":

    initialize_database()

    print("=" * 60)
    print("SENTINELDC DATABASE")
    print("=" * 60)

    print(f"Database location:")
    print(DATABASE_PATH)

    print("Database initialized successfully.")

    print("=" * 60)
