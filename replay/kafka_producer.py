import json
import time

import pandas as pd
from kafka import KafkaProducer


# ============================================================
# CONFIGURATION
# ============================================================

DATA_PATH = "data/processed/replay_data.csv"

KAFKA_SERVER = "127.0.0.1:9092"

TOPIC_NAME = "datacenter_telemetry_v2"

MAX_RECORDS = 10


# ============================================================
# LOAD DATA
# ============================================================

print("=" * 60)
print("SENTINELDC KAFKA PRODUCER")
print("=" * 60)

df = pd.read_csv(DATA_PATH)

print("Dataset shape:", df.shape)


# ============================================================
# REMOVE TARGETS
# ============================================================

target_columns = [
    "Target_Module_1",
    "Target_Module_2",
    "Target_Module_3",
    "Target_Module_4",
    "Target_Module_5",
    "Target_Module_6",
    "Target_Module_7",
    "Target_Module_8"
]

# Keep only targets that actually exist
target_columns = [
    col for col in target_columns
    if col in df.columns
]

df = df.drop(
    columns=target_columns
)

print("Removed target columns:", target_columns)


# ============================================================
# CREATE KAFKA PRODUCER
# ============================================================

producer = KafkaProducer(

    bootstrap_servers=KAFKA_SERVER,

    value_serializer=lambda value:
        json.dumps(value).encode("utf-8")
)


# ============================================================
# SEND DATA
# ============================================================

for index, row in df.head(MAX_RECORDS).iterrows():

    message = row.to_dict()

    message["_replay_id"] = int(index)

    future = producer.send(
        TOPIC_NAME,
        value=message
    )

    metadata = future.get(timeout=10)

    print(
        f"Sent record {index} | "
        f"Partition: {metadata.partition} | "
        f"Offset: {metadata.offset}"
    )

    time.sleep(1)


# ============================================================
# FINISH
# ============================================================

producer.flush()

producer.close()

print("=" * 60)
print("PRODUCER FINISHED")
print("=" * 60)