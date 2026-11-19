# =============================================================================
# SILVER TRANSFORMER: kafka.py — HEADER (Top Imports)
# =============================================================================
"""
Kafka Silver Layer Transformations.

Pure, deterministic transformation functions for Kafka-sourced event data.
Each function takes a Bronze DataFrame and returns a Silver DataFrame with
explicit schema contracts. No I/O, no side effects, fully testable.
"""
from __future__ import annotations

# --- Standard Library ---
from typing import Optional

# --- PySpark Core ---
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    IntegerType, BooleanType, DateType
)

# --- Project Configuration (Centralized) ---
from scripts.storage.config import (
    get_spark_session,
    get_path_builder,
    PathBuilder,
    Layer,
    SourceSystem,
    SparkConfig,
    SparkSessionManager,
)
from scripts.storage.config.io_utils import write_table, read_bronze_table

# --- Environment & Runtime Markers ---
import os
DEPLOY_ENV: str = os.getenv("DEPLOY_ENV", "development")
INGESTION_DATE: str = os.getenv("INGESTION_DATE", "")
BATCH_ID: str = os.getenv("BATCH_ID", "")

# --- Active Path Builder ---
PATHS: PathBuilder = get_path_builder()



# ==================== CONSTANTS (Pristine Factory Alignment) ====================

# Valid interaction types from InteractionFactory.INTERACTIONS
VALID_INTERACTIONS = frozenset([
    "play", "pause", "complete", "like"
])

# Valid device types from InteractionFactory.DEVICES
VALID_DEVICES = frozenset([
    "mobile", "tv", "web"
])

# Legacy vocab mapping (Bronze → Silver) - aligns with factory output
DEVICE_TYPE_MAP = {
    "web": "web",
    "mobile": "mobile",
    "tv": "tv",
}

# Simulation temporal boundaries (injector constants)
SIMULATION_START = "2026-01-01 00:00:00"
SIMULATION_END   = "2026-12-31 23:59:59"

# Watch-time threshold for "exceeds video duration" (injector sets 10000 on play)
MAX_WATCH_TIME_SEC = 7200  # 2 hours


# ==================== SILVER SCHEMA CONTRACTS ====================

EVENTS_SILVER_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_timestamp", TimestampType(), True),
    StructField("event_timestamp_raw", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("video_id", StringType(), True),
    StructField("interaction_type", StringType(), True),
    StructField("watch_time_sec", IntegerType(), True),
    StructField("device_type", StringType(), True),
    # Data Quality Flags
    StructField("dq_missing_user_id", BooleanType(), False),
    StructField("dq_missing_video_id", BooleanType(), False),
    StructField("dq_missing_watch_time", BooleanType(), False),                    # ALL interaction types
    StructField("dq_missing_watch_time_play_complete", BooleanType(), False),      # Semantic subset
    StructField("dq_unexpected_watch_time", BooleanType(), False),
    StructField("dq_invalid_watch_time", BooleanType(), False),                    # <=0 or >120
    StructField("dq_watch_time_negative_or_zero", BooleanType(), False),           # Explicit <=0
    StructField("dq_watch_time_exceeds_duration", BooleanType(), False),           # >7200s on play
    StructField("dq_invalid_interaction", BooleanType(), False),
    StructField("dq_invalid_device", BooleanType(), False),
    StructField("dq_invalid_timestamp", BooleanType(), False),                     # Parse failure
    StructField("dq_timestamp_pre_simulation", BooleanType(), False),              # < 2026-01-01
    StructField("dq_timestamp_post_simulation", BooleanType(), False),             # > 2026-12-31
    # Derived analytical fields
    StructField("event_date", DateType(), True),
    StructField("event_hour", IntegerType(), True),
    # Metadata
    StructField("ingestion_date", DateType(), False),
    StructField("batch_id", StringType(), False),
])


# ==================== TRANSFORMATION FUNCTIONS ====================

def transform_events(df_bronze: DataFrame) -> DataFrame:
    """
    Transform Bronze Kafka events to Silver.
    
    Business Rules:
    - Preserve one Silver record per source event (no deduplication)
    - Parse event_timestamp from string to timestamp; invalid -> NULL + flag
    - Normalize interaction_type: trim + lower; unknown values preserved + flagged
    - Normalize device_type: trim + lower; map legacy vocab; unknown preserved + flagged
    - watch_time_sec preserved as-is; NULLs preserved; invalid ranges flagged
    - Add comprehensive data quality flags for all detected issues
    - Derive event_date and event_hour from parsed timestamp
    - Add ingestion metadata (date, batch_id)
    
    Args:
        df_bronze: Bronze events DataFrame with raw string columns
        
    Returns:
        Silver events DataFrame conforming to EVENTS_SILVER_SCHEMA
    """
    return (
        df_bronze
        # Preserve raw timestamp string
        .withColumn("event_timestamp_raw", F.col("event_timestamp"))
        
        # Parse timestamp - invalid values become NULL
        .withColumn(
            "event_timestamp",
            F.coalesce(
                F.to_timestamp(F.col("event_timestamp"), "yyyy-MM-dd HH:mm:ssXXX"),
                F.to_timestamp(F.col("event_timestamp"), "yyyy-MM-dd HH:mm:ss.SSSSSSXXX"),
            )
        )
        
        # Normalize interaction_type
        .withColumn(
            "interaction_type",
            F.lower(F.trim(F.col("interaction_type")))
        )
        
        # Normalize device_type
        .withColumn(
            "device_type",
            F.lower(F.trim(F.col("device_type")))
        )
        .withColumn(
            "device_type",
            F.coalesce(
            F.create_map([F.lit(k) for pair in DEVICE_TYPE_MAP.items() for k in pair])[F.col("device_type")],
            F.col("device_type")
        ))
        
        # Ensure watch_time_sec is integer (handle string input)
        .withColumn(
            "watch_time_sec",
            F.col("watch_time_sec").cast("int")
        )
        
        # ==================== DATA QUALITY FLAGS ====================
        
        # Missing user_id
        .withColumn(
            "dq_missing_user_id",
            F.col("user_id").isNull() | (F.trim(F.col("user_id")) == "")
        )
        
        # Missing video_id
        .withColumn(
            "dq_missing_video_id",
            F.col("video_id").isNull() | (F.trim(F.col("video_id")) == "")
        )
        
        # Missing watch_time for ALL interaction types (expanded from play/complete only)
        .withColumn(
            "dq_missing_watch_time",
            F.col("watch_time_sec").isNull()
        )
        
        # Missing watch_time for play/complete events (semantic subset - original rule)
        .withColumn(
            "dq_missing_watch_time_play_complete",
            F.col("interaction_type").isin("play", "complete") &
            F.col("watch_time_sec").isNull()
        )
        
        # Unexpected watch_time for like/pause events
        .withColumn(
            "dq_unexpected_watch_time",
            F.col("interaction_type").isin("like", "pause") &
            F.col("watch_time_sec").isNotNull()
        )
        
        # Invalid watch_time values (<=0 or >120)
        .withColumn(
            "dq_invalid_watch_time",
            F.col("watch_time_sec").isNotNull() &
            ((F.col("watch_time_sec") <= 0) | (F.col("watch_time_sec") > 120))
        )
        
        # Explicit negative or zero watch_time
        .withColumn(
            "dq_watch_time_negative_or_zero",
            F.col("watch_time_sec").isNotNull() & (F.col("watch_time_sec") <= 0)
        )
        
        # Watch time exceeds video duration (play events with >7200s)
        .withColumn(
            "dq_watch_time_exceeds_duration",
            (F.col("interaction_type") == "play") & 
            F.col("watch_time_sec").isNotNull() & 
            (F.col("watch_time_sec") > MAX_WATCH_TIME_SEC)
        )
        
        # Invalid interaction type
        .withColumn(
            "dq_invalid_interaction",
            F.col("interaction_type").isNull() |
            ~F.col("interaction_type").isin(*VALID_INTERACTIONS)
        )
        
        # Invalid device type
        .withColumn(
            "dq_invalid_device",
            F.col("device_type").isNull() |
            ~F.col("device_type").isin(*VALID_DEVICES)
        )
        
        # Invalid timestamp (failed to parse)
        .withColumn(
            "dq_invalid_timestamp",
            F.col("event_timestamp").isNull()
        )
        
        # Timestamp before simulation start
        .withColumn(
            "dq_timestamp_pre_simulation",
            F.col("event_timestamp").isNotNull() & 
            (F.col("event_timestamp") < F.lit(SIMULATION_START))
        )
        
        # Timestamp after simulation end
        .withColumn(
            "dq_timestamp_post_simulation",
            F.col("event_timestamp").isNotNull() & 
            (F.col("event_timestamp") > F.lit(SIMULATION_END))
        )
        
        # ==================== DERIVED ANALYTICAL FIELDS ====================
        
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("event_hour", F.hour("event_timestamp"))
        
        # ==================== METADATA ====================
        
        .withColumn("ingestion_date", F.current_date())
        .withColumn(
            "batch_id",
            F.concat(
                F.date_format(F.current_timestamp(), "yyyyMMdd_HHmmss"),
                F.lit("_kafka")
            )
        )
        
        # Enforce schema contract
        .select(*[f.name for f in EVENTS_SILVER_SCHEMA.fields])
    )
# =============================================================================
# SILVER TRANSFORMER: kafka.py — FOOTER (Execution Block)
# =============================================================================
def _run_kafka_silver_job() -> DataFrame:
    """
    Orchestration entry point: Read Bronze -> Transform -> Write Silver.
    Returns events_silver DataFrame.
    """
    if not INGESTION_DATE:
        raise ValueError("INGESTION_DATE environment variable is required")

    app_name = f"silver_kafka_{INGESTION_DATE.replace('-', '_')}"

    with SparkSessionManager(app_name, env=DEPLOY_ENV) as spark:
        # 1. Read Bronze events (Kafka Bronze uses streaming path pattern)
        # Note: Kafka Bronze may be written as streaming micro-batches.
        # For batch backfill, we read the partitioned path.
        #df_events_bronze = read_bronze_table(spark, SourceSystem.KAFKA, "events", INGESTION_DATE)
        df_events_bronze = spark.read.parquet(f"{PATHS.bronze_base(SourceSystem.KAFKA)}/ingestion_date={INGESTION_DATE}")

        # 2. Transform (pure function)
        events_silver = transform_events(df_events_bronze)

        # 3. Write Silver (flat table for events)
        write_table(
            events_silver,
            table_name="events",
            layer=Layer.SILVER,
            source=SourceSystem.KAFKA,
            mode="overwrite",
        )

        return events_silver


if __name__ == "__main__":
    _run_kafka_silver_job()
