# scripts/storage/gold/fact_user_daily.py
"""
Gold Layer: Daily User Snapshot Aggregate Fact Table.

Compiles daily user snapshot aggregate from fact_event to drive retention
metrics. Calculates:
- total_events (count of all interactions)
- total_watch_time_sec (sum of watch_time_sec)
- distinct videos_viewed (countDistinct video_sk)
- boolean active status markers (is_active_day)

Partitioned by event_date for lightning-fast retention dashboards.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    IntegerType, LongType, BooleanType
)
from pyspark.sql.types import TimestampType
from scripts.storage.config import SparkSessionManager, Layer
from scripts.storage.config.io_utils import write_table, read_gold_fact

# ==================== GOLD SCHEMA CONTRACT ====================

FACT_USER_DAILY_SCHEMA = StructType([
    # Foreign keys
    StructField("user_sk", IntegerType(), False),
    StructField("date_sk", IntegerType(), False),
    # Natural keys
    StructField("user_id", StringType(), False),
    StructField("event_date", DateType(), False),
    # Additive measures
    StructField("total_events", LongType(), False),
    StructField("total_watch_time_sec", LongType(), False),
    StructField("videos_viewed", LongType(), False),       # distinct video_sk
    StructField("distinct_interaction_types", IntegerType(), False),
    # Engagement breakdown
    StructField("plays", LongType(), False),
    StructField("completes", LongType(), False),
    StructField("likes", LongType(), False),
    StructField("pauses", LongType(), False),
    # Active status markers
    StructField("is_active_day", BooleanType(), False),    # total_events > 0
    StructField("is_watch_day", BooleanType(), False),     # total_watch_time_sec > 0
    StructField("is_complete_day", BooleanType(), False),  # completes > 0
    StructField("is_like_day", BooleanType(), False),      # likes > 0
    # Metadata
    StructField("gold_processed_at", TimestampType(), False),
    StructField("batch_id", StringType(), False),
])


def generate_fact_user_daily(df_fact_event: DataFrame) -> DataFrame:
    """
    Build daily user snapshot aggregate from fact_event.

    Steps:
    1. Filter fact_event to valid surrogate keys (user_sk not null)
    2. Group by user_sk, date_sk, user_id, event_date
    3. Compute additive measures and engagement breakdown
    4. Calculate boolean active status markers
    5. Add metadata
    6. Enforce schema contract

    Args:
        df_fact_event: Gold fact_event DataFrame

    Returns:
        DataFrame conforming to FACT_USER_DAILY_SCHEMA
    """
    # Filter to rows with valid user_sk (events linked to known users)
    df_valid = df_fact_event.filter(F.col("user_sk").isNotNull())

    # Conditional aggregation for engagement breakdown
    df_agg = df_valid.groupBy("user_sk", "date_sk", "user_id", "event_date").agg(
        # Total events
        F.count("*").alias("total_events"),
        # Total watch time
        F.coalesce(F.sum("watch_time_sec"), F.lit(0)).alias("total_watch_time_sec"),
        # Distinct videos viewed
        F.countDistinct("video_sk").alias("videos_viewed"),
        # Distinct interaction types (cast to int to match FACT_USER_DAILY_SCHEMA IntegerType)
        F.countDistinct("interaction_type").cast("int").alias("distinct_interaction_types"),
        # Engagement breakdown
        F.count(F.when(F.col("interaction_type") == "play", True)).alias("plays"),
        F.count(F.when(F.col("interaction_type") == "complete", True)).alias("completes"),
        F.count(F.when(F.col("interaction_type") == "like", True)).alias("likes"),
        F.count(F.when(F.col("interaction_type") == "pause", True)).alias("pauses"),
    )

    # Boolean active status markers
    df_flags = (
        df_agg
        .withColumn("is_active_day", F.col("total_events") > 0)
        .withColumn("is_watch_day", F.col("total_watch_time_sec") > 0)
        .withColumn("is_complete_day", F.col("completes") > 0)
        .withColumn("is_like_day", F.col("likes") > 0)
    )

    # Add metadata
    return (
        df_flags
        .withColumn("gold_processed_at", F.current_timestamp())
        .withColumn(
            "batch_id",
            F.concat(
                F.date_format(F.current_timestamp(), "yyyyMMdd_HHmmss"),
                F.lit("_gold_fact_usr_daily")
            )
        )
        .select(*[f.name for f in FACT_USER_DAILY_SCHEMA.fields])
    )


# ==================== HIGH-LEVEL ORCHESTRATION ====================

def build_fact_user_daily(df_fact_event: DataFrame) -> DataFrame:
    """
    High-level entry point to build fact_user_daily.

    Args:
        df_fact_event: Gold fact_event DataFrame

    Returns:
        DataFrame conforming to FACT_USER_DAILY_SCHEMA
    """
    return generate_fact_user_daily(df_fact_event)

def run_fact_user_daily() -> None:
    """Build and write Gold fact_user_daily from fact_event."""
    with SparkSessionManager("gold_fact_user_daily") as spark:
        df_fact_event = read_gold_fact(spark, "event")
        df = build_fact_user_daily(df_fact_event)
        write_table(
            df,
            table_name="fact_user_daily",
            layer=Layer.GOLD,
            partition_cols=["date_sk"],
            mode="overwrite",
        )


if __name__ == "__main__":
    run_fact_user_daily()
