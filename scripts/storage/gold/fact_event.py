# scripts/storage/gold/fact_event.py
"""
Gold Layer: Atomic Event Fact Table.

Reads Kafka Events Silver data, applies strict Entity Quality Gate
(filtering rows where ANY dq_* flag is True), maps dynamic surrogate keys
(user_sk, video_sk, device_sk, date_sk), and enforces daily partitioning.
Z-order ready: sorted by event_date, user_sk, video_sk.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DateType,
    IntegerType, BooleanType, LongType
)
from pyspark.sql.window import Window
from scripts.storage.config import SparkSessionManager, Layer, SourceSystem
from scripts.storage.config.io_utils import write_table, read_silver_table, read_gold_dimension


# ==================== GOLD SCHEMA CONTRACT ====================

FACT_EVENT_SCHEMA = StructType([
    # Surrogate keys
    StructField("event_sk", LongType(), False),
    StructField("user_sk", IntegerType(), True),
    StructField("video_sk", IntegerType(), True),
    StructField("device_sk", IntegerType(), True),
    StructField("date_sk", IntegerType(), False),  # YYYYMMDD from dim_date
    # Natural keys
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("video_id", StringType(), True),
    # Event attributes
    StructField("interaction_type", StringType(), True),
    StructField("watch_time_sec", IntegerType(), True),
    StructField("device_type", StringType(), True),
    # Temporal
    StructField("event_timestamp", TimestampType(), True),
    StructField("event_date", DateType(), True),
    StructField("event_hour", IntegerType(), True),
    # Metadata
    StructField("gold_processed_at", TimestampType(), False),
    StructField("batch_id", StringType(), False),
])


# ==================== ENTITY QUALITY GATE ====================
# All dq_* columns from Silver Events that must be False for a row to pass

EVENT_DQ_FLAGS = [
    "dq_missing_user_id",
    "dq_missing_video_id",
    "dq_missing_watch_time_play_complete",
    "dq_unexpected_watch_time",
    "dq_watch_time_negative_or_zero",
    "dq_watch_time_exceeds_duration",
    "dq_invalid_interaction",
    "dq_invalid_device",
    "dq_invalid_timestamp",
    "dq_timestamp_pre_simulation",
    "dq_timestamp_post_simulation",
]


def apply_event_quality_gate(df_silver_events: DataFrame) -> DataFrame:
    """
    Apply the Phase 4 Entity Quality Gate for Events: filter out rows
    where ANY dq_* data quality flag is True.

    Args:
        df_silver_events: Silver events DataFrame with dq_* columns

    Returns:
        Filtered DataFrame containing only clean rows
    """
    filter_expr = F.lit(True)
    for flag in EVENT_DQ_FLAGS:
        if flag in df_silver_events.columns:
            filter_expr = filter_expr & (~F.col(flag))
        # If flag column doesn't exist, treat as passed (defensive)

    return df_silver_events.filter(filter_expr)


def generate_fact_event(
    df_silver_events: DataFrame,
    df_dim_user: DataFrame,
    df_dim_video: DataFrame,
    df_dim_device: DataFrame,
    df_dim_date: DataFrame
) -> DataFrame:
    """
    Build atomic event fact from Silver Events.

    Steps:
    1. Apply Event Entity Quality Gate (drop rows with any dq_* = True)
    2. Map surrogate keys: user_sk, video_sk, device_sk, date_sk
    3. Assign event_sk (monotonically increasing for partitioning)
    4. Add metadata
    5. Enforce schema contract
    6. Sort for Z-order optimization (event_date, user_sk, video_sk)

    Args:
        df_silver_events: Silver events DataFrame (output of silver.transform_events)
        df_dim_user: Gold dim_user DataFrame
        df_dim_video: Gold dim_video DataFrame
        df_dim_device: Gold dim_device DataFrame
        df_dim_date: Gold dim_date DataFrame

    Returns:
        DataFrame conforming to FACT_EVENT_SCHEMA
    """
    # Step 1: Entity Quality Gate
    df_clean = apply_event_quality_gate(df_silver_events)

    # Step 2: Map surrogate keys
    # user_sk
    user_lookup = df_dim_user.select("user_id", "user_sk")
    df = df_clean.join(user_lookup, on="user_id", how="left")

    # video_sk
    video_lookup = df_dim_video.select("video_id", "video_sk")
    df = df.join(video_lookup, on="video_id", how="left")

    # device_sk
    device_lookup = df_dim_device.select("device_type", "device_sk")
    df = df.join(device_lookup, on="device_type", how="left")

    # date_sk from event_date
    date_lookup = df_dim_date.select("date", "date_sk").withColumnRenamed("date", "event_date")
    df = df.join(date_lookup, on="event_date", how="left")

    # Step 3: Assign event_sk (monotonically increasing for partitioning)
    # Use row_number over ordered event_id for determinism
    df = df.withColumn(
        "event_sk",
        F.row_number().over(Window.orderBy("event_id"))
    ).withColumn("event_sk", F.col("event_sk").cast("long"))

    # Step 4: Add metadata
    df = (
        df
        .withColumn("gold_processed_at", F.current_timestamp())
        .withColumn(
            "batch_id",
            F.concat(
                F.date_format(F.current_timestamp(), "yyyyMMdd_HHmmss"),
                F.lit("_gold_fact_event")
            )
        )
    )

    # Step 5: Enforce schema contract
    df = df.select(*[f.name for f in FACT_EVENT_SCHEMA.fields])

    # Step 6: Sort for Z-order optimization (event_date, user_sk, video_sk)
    # Note: In production, this would be handled by Delta Lake ZORDER or
    # by repartitioning before write. Here we order for deterministic output.
    return df.orderBy("event_date", "user_sk", "video_sk")


# ==================== HIGH-LEVEL ORCHESTRATION ====================

def build_fact_event(
    df_silver_events: DataFrame,
    df_dim_user: DataFrame,
    df_dim_video: DataFrame,
    df_dim_device: DataFrame,
    df_dim_date: DataFrame
) -> DataFrame:
    """
    High-level entry point to build fact_event.

    Args:
        df_silver_events: Silver events DataFrame
        df_dim_user: Gold dim_user DataFrame
        df_dim_video: Gold dim_video DataFrame
        df_dim_device: Gold dim_device DataFrame
        df_dim_date: Gold dim_date DataFrame

    Returns:
        DataFrame conforming to FACT_EVENT_SCHEMA
    """
    return generate_fact_event(
        df_silver_events, df_dim_user, df_dim_video, df_dim_device, df_dim_date
    )

def run_fact_event(ingestion_date: str = None) -> None:
    """Build and write Gold fact_event from Silver events + all dimensions."""
    import os
    ingestion_date = ingestion_date or os.getenv("INGESTION_DATE")
    if not ingestion_date:
        raise ValueError("INGESTION_DATE required (env var or argument)")

    with SparkSessionManager("gold_fact_event") as spark:
        df_silver_events = read_silver_table(spark, SourceSystem.KAFKA, "events")
        df_dim_user = read_gold_dimension(spark, "user")
        df_dim_video = read_gold_dimension(spark, "video")
        df_dim_device = read_gold_dimension(spark, "device")
        df_dim_date = read_gold_dimension(spark, "date")
        df = build_fact_event(df_silver_events, df_dim_user, df_dim_video, df_dim_device, df_dim_date)
        write_table(
            df,
            table_name="fact_event",
            layer=Layer.GOLD,
            partition_cols=["date_sk"],
            mode="append",
        )


if __name__ == "__main__":
    run_fact_event()
