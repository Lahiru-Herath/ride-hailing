"""

Speed layer: reads trip_events from Kafka via Structured Streaming and produces three independent outputs from the same source:
    1. Raw events -> Parquet (storage/raw_events/, partitioned by event_date) - Phase 2's raw storage requirement
    2. 5-minute tumbling window aggregation(active/idle vehicles, trips completed, earnings) by zone -> realtime_fleet_metrics (Postgres).
    3. Per-vehicle idel-duration tracking (stateful) -> alerts (Postgres) when a vehicle stays idle past IDLE_ALERT_THRESHOLD_MINUTES.

Runs inside the `spark` Docker service via spark-submit, not locally

"""

import os
import sys
from datetime import datetime
from typing import Iterator

import pandas as pd
import psycopg2
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.append("/opt/spark-apps")
from observability.logging_config import get_logger

logger = get_logger("speed_layer_job")

# --- Config from environment (passed via docker-compose, no .env file inside this container) ---
KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC_TRIP_EVENTS"]
POSTGRES_HOST = os.environ["POSTGRES_HOST"]
POSTGRES_PORT = 5432  # internal container port, always 5432 regardless of the host-side 5433 remap (see D11)
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
IDLE_ALERT_THRESHOLD_MINUTES = int(os.environ["IDLE_ALERT_THRESHOLD_MINUTES"])

STORAGE_ROOT = "/opt/spark-apps/storage"
RAW_EVENTS_PATH = f"{STORAGE_ROOT}/raw_events"
CHECKPOINT_BASE = f"{STORAGE_ROOT}/checkpoints"

# --- Must match the fake city grid in sources/streaming_producer.py exactly ---
LAT_MIN, LAT_MAX = 6.900, 6.960
LON_MIN, LON_MAX = 79.850, 79.900
GRID_SIZE = 3 # 3x3 = 9 zones, labeled Z1..Z9

EVENT_SCHEMA = StructType(
    [
        StructField("trip_id", StringType(), True),
        StructField("driver_id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("speed", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("fare", DoubleType(), True),
        StructField("timestamp", StringType(), True),
    ]
)

def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("ride-hailing_speed-layer")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4") # default 200 is overkill for 15 vehicles
        .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    return spark

def read_parsed_events(spark: SparkSession) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    return (
        raw.selectExpr("CAST(value as STRING) AS json_str")
        .select(F.from_json("json_str", EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn(
            "event_time",
            # No explicit pattern: Spark's built-in ISO-8601 parser handles the
            # 'T' separator and the +00:00 offset natively, and tolerates a
            # variable (or absent) fractional-seconds part — unlike a hand-written
            # pattern, which broke twice on this exact input.
            F.to_timestamp("timestamp"),
        )
    )

def with_zone(df: DataFrame) -> DataFrame:
    """Bucket lat/lon into a 3x3 grid of zones Z1..Z9 (simple enrichment per Phase 3)"""
    lat_bucket = F.least(
        F.greatest(
            F.floor((F.col("lat") - LAT_MIN) / (LAT_MAX - LAT_MIN) * GRID_SIZE),
            F.lit(0),
        ),
        F.lit(GRID_SIZE - 1),
    )
    lon_bucket = F.least(
        F.greatest(
            F.floor((F.col("lon") - LON_MIN) / (LON_MAX - LON_MIN) * GRID_SIZE),
            F.lit(0)
        ),
        F.lit(GRID_SIZE - 1),
    )
    zone_index = (lat_bucket * GRID_SIZE + lon_bucket + 1).cast("int")

    return df.withColumn("zone", F.concat(F.lit("Z"), zone_index.cast("string")))


# ------------------------------
# 1. Raw events -> Parquet
# ------------------------------

def start_raw_sink(parsed: DataFrame):
    raw_with_date = parsed.withColumn("event_date", F.to_date("event_time"))
    query = (
        raw_with_date.writeStream.format("parquet")
        .option("path", RAW_EVENTS_PATH)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/raw_events")
        .partitionBy("event_date")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info("raw_sink_started", extra={"path": RAW_EVENTS_PATH})

    return query


# ---------------------------------------------------------------------------
# 2. Windowed aggregation -> realtime_fleet_metrics
# ---------------------------------------------------------------------------

def build_metrics_df(parsed: DataFrame) -> DataFrame:
    """
    NOTE on trip completion / earnings: the producer contract (see streaming_producer.y) is that the event where a trip finishes carries 
    status="idel" AND  non-null trip_id, with `fare` holding the finalized total. That is the only place a trip's earnings should be counted
    from - every earlier on_trip event repeats the same running total, so summing fare across all on_trip events would massively over-count
    """

    zoned = with_zone(parsed)
    return (
        zoned.withWatermark("event_time", "2 minutes")
        .groupBy(F.window("event_time", "5 minutes"), F.col("zone"))
        .agg(
            F.expr("approx_count_distinct(case when status != 'idle' then vehicle_id end)").alias("active_vehicles"),
            F.expr("approx_count_distinct(case when status = 'idle' then vehicle_id end)").alias("idle_vehicles"),
            F.expr("count(case when status = 'idle' and trip_id is not null then 1 end)").alias("trips_completed"),
            F.expr("sum(case when status = 'idle' and trip_id is not null then fare else 0 end)").alias("total_earnings"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "zone",
            "active_vehicles",
            "idle_vehicles",
            "trips_completed",
            "total_earnings",
        )
    )

def write_metrics_batch(batch_df, batch_id: int) -> None:
    rows = batch_df.collect() # tiny per-batch volume (9 zones x 1 window) - fine to collect to driver
    if not rows:
        return

    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )

    try:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    """
                    INSERT INTO realtime_fleet_metrics (window_start, window_end, zone, active_vehicles, idle_vehicles, trips_completed, total_earnings) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        r.window_start, r.window_end, r.zone, r.active_vehicles, r.idle_vehicles, r.trips_completed, r.total_earnings,
                    ),
                )
        conn.commit()
        logger.info("metrics_batch_written", extra={"batch_id": batch_id, "row_count": len(rows)})
    finally:
        conn.close()

def start_metrics_sink(parsed: DataFrame):
    """
    Uses outputMode="append" with a 2-minute watermark: each window's row is written exactly once, after the watermark passes the window's end - so plain INSERT
    is safe (no duplicate/updating rows, no upsert or unique constraint needed). Trade-off: a window's metrics appear ~5-7 minutes after it starts, not instantly.
    Acceptable for a 5-minute dashboard window; log as a decision if you want a shorter window later.
    """
    metrics_df = build_metrics_df(parsed)

    query = (
        metrics_df.writeStream.outputMode("append")
        .foreachBatch(write_metrics_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/realtime_fleet_metrics")
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info("metrics_sink_started")
    return query


# ---------------------------------------------------------------------------
# 3. Idle-vehicle detection (stateful) -> alerts
# ---------------------------------------------------------------------------

ALERT_SCHEMA = StructType(
    [
        StructField("vehicle_id", StringType(), True),
        StructField("alert_type", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("message", StringType(), True),
        StructField("triggered_at", TimestampType(), True),
    ]
)

IDLE_STATE_SCHEMA = StructType(
    [
        StructField("idle_since_epoch", LongType(), True),  # -1 means "not currently idle"
        StructField("alerted", BooleanType(), True),
    ]
)

class IdleAlertProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        self.state = handle.getValueState("idle_state", IDLE_STATE_SCHEMA)
        self.threshold_seconds = IDLE_ALERT_THRESHOLD_MINUTES * 60

    def handleInputRows(self, key, rows: Iterator[pd.DataFrame], timerValues) -> Iterator[pd.DataFrame]:
        vehicle_id = key[0]

        if self.state.exists():
            idle_since_epoch, alerted = self.state.get()
        else:
            idle_since_epoch, alerted = -1, False

        alerts = []
        for chunk in rows:
            for _, row in chunk.sort_values("event_time").iterrows():
                ts_epoch = int(row["event_time"].timestamp())
                if row["status"] == "idle":
                    if idle_since_epoch == -1:
                        idle_since_epoch = ts_epoch
                    elif not alerted and (ts_epoch - idle_since_epoch) >= self.threshold_seconds:
                        alerts.append(
                            {
                                "vehicle_id": vehicle_id,
                                "alert_type": "idle_vehicle",
                                "severity": "warning",
                                "message": (
                                    f"{vehicle_id} has been idle for over "
                                    f"{self.threshold_seconds // 60} minutes"
                                ),
                                "triggered_at": row["event_time"],
                            }
                        )
                        alerted = True
                else:
                    idle_since_epoch = -1
                    alerted = False

        self.state.update((idle_since_epoch, alerted))

        if alerts:
            yield pd.DataFrame(alerts)
        else:
            yield pd.DataFrame(
                columns=["vehicle_id", "alert_type", "severity", "message", "triggered_at"]
            )

    def close(self) -> None:
        pass


def write_alerts_batch(batch_df, batch_id: int) -> None:
    rows = batch_df.collect()
    if not rows:
        return
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            for r in rows:
                # `resolved` has no server_default in the Alembic-generated schema
                # (SQLAlchemy's default=False is a Python-side/ORM default only), so
                # a raw INSERT must supply it explicitly or this violates NOT NULL.
                cur.execute(
                    """
                    INSERT INTO alerts (alert_type, severity, entity_id, message, triggered_at, resolved)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (r.alert_type, r.severity, r.vehicle_id, r.message, r.triggered_at, False),
                )
        conn.commit()
        logger.info("alerts_batch_written", extra={"batch_id": batch_id, "row_count": len(rows)})
    finally:
        conn.close()


def start_alerts_sink(parsed: DataFrame):
    alerts_df = parsed.groupBy("vehicle_id").transformWithStateInPandas(
        statefulProcessor=IdleAlertProcessor(),
        outputStructType=ALERT_SCHEMA,
        outputMode="Update",
        timeMode="None",
    )
    query = (
        alerts_df.writeStream.outputMode("update")
        .foreachBatch(write_alerts_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/alerts")
        .trigger(processingTime="10 seconds")
        .start()
    )
    logger.info("alerts_sink_started", extra={"threshold_minutes": IDLE_ALERT_THRESHOLD_MINUTES})
    return query


def main() -> None:
    logger.info(
        "speed_layer_starting",
        extra={"kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS, "topic": KAFKA_TOPIC},
    )
    spark = build_spark()
    parsed = read_parsed_events(spark)

    start_raw_sink(parsed)
    start_metrics_sink(parsed)
    start_alerts_sink(parsed)

    logger.info("all_queries_started")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()