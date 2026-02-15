import os
import json
import time
from datetime import datetime, timezone

import psycopg2
from kafka import KafkaConsumer


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "earthquakes_raw")
KAFKA_GROUP = os.getenv("KAFKA_GROUP", "quake-db-writer")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "earthquakes")
PG_USER = os.getenv("PG_USER", "quake")
PG_PASS = os.getenv("PG_PASS", "quakepass")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
POLL_TIMEOUT_MS = int(os.getenv("POLL_TIMEOUT_MS", "5000"))


UPSERT_SQL = """
INSERT INTO earthquakes (
  id, magnitude, place, time_ms, event_time, latitude, longitude, depth
) VALUES (
  %(id)s, %(magnitude)s, %(place)s, %(time_ms)s, %(event_time)s, %(latitude)s, %(longitude)s, %(depth)s
)
ON CONFLICT (id) DO UPDATE SET
  magnitude = EXCLUDED.magnitude,
  place     = EXCLUDED.place,
  time_ms   = EXCLUDED.time_ms,
  event_time= EXCLUDED.event_time,
  latitude  = EXCLUDED.latitude,
  longitude = EXCLUDED.longitude,
  depth     = EXCLUDED.depth;
"""


def pg_connect():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
    )


def to_utc_timestamp(ms: int):
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).replace(tzinfo=None)


def normalize(msg: dict):
    if "id" not in msg:
        return None

    time_ms = msg.get("time_ms")
    if time_ms is None:
        return None

    try:
        time_ms = int(time_ms)
    except Exception:
        return None

    return {
        "id": str(msg.get("id")),
        "magnitude": float(msg.get("magnitude")) if msg.get("magnitude") is not None else None,
        "place": msg.get("place"),
        "time_ms": time_ms,
        "event_time": to_utc_timestamp(time_ms),
        "latitude": float(msg.get("latitude")) if msg.get("latitude") is not None else None,
        "longitude": float(msg.get("longitude")) if msg.get("longitude") is not None else None,
        "depth": float(msg.get("depth")) if msg.get("depth") is not None else None,
    }


def main():
    print(f"[db-writer] Kafka: {KAFKA_BOOTSTRAP} topic={KAFKA_TOPIC} group={KAFKA_GROUP}")
    print(f"[db-writer] Postgres: {PG_HOST}:{PG_PORT} db={PG_DB} user={PG_USER}")

    conn = None
    while conn is None:
        try:
            conn = pg_connect()
            conn.autocommit = False
            print("[db-writer] Connected to Postgres")
        except Exception as e:
            print(f"[db-writer] Postgres not ready yet: {e}")
            time.sleep(2)

    cur = conn.cursor()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: v.decode("utf-8", errors="replace"),
    )

    buffer = []
    last_flush = time.time()

    def flush():
        nonlocal buffer, last_flush
        if not buffer:
            return
        try:
            cur.executemany(UPSERT_SQL, buffer)
            conn.commit()
            consumer.commit()
            print(f"[db-writer] Upserted {len(buffer)} rows")
            buffer = []
            last_flush = time.time()
        except Exception as e:
            conn.rollback()
            print(f"[db-writer] ERROR during upsert, rolled back: {e}")
            buffer = []
            last_flush = time.time()

    for msg in consumer:
        try:
            data = json.loads(msg.value)
        except Exception:
            continue

        row = normalize(data)
        if row is None:
            continue

        buffer.append(row)

        if len(buffer) >= BATCH_SIZE or (time.time() - last_flush) > 5:
            flush()


if __name__ == "__main__":
    main()
