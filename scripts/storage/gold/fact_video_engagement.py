# scripts/storage/gold/fact_video_engagement.py
"""
Gold Layer: Daily Video Engagement Pre-Aggregated Fact Table.

Compiles high-performance daily pre-aggregated fact from fact_event.
Formulates explicit additive measures:
- views (count of play)
- completes (count of complete)
- likes (count of like)
- pauses (count of pause)
- total_watch_time_sec (sum of watch_time_sec)
- completion_rate (completes / views)
- like_rate (likes / views)

Partitioned by event_date for lightning-fast Power BI queries.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    IntegerType, LongType, DecimalType, TimestampType
)
from pyspark.sql.window import Window
from scripts.storage.config import SparkSessionManager, Layer
from scripts.storage.config.io_utils import write_table, read_gold_fact

# ==================== GOLD SCHEMA CONTRACT ====================

FACT_VIDEO_ENGAGEMENT_SCHEMA = StructType([
    # Foreign keys
    StructField("video_sk", IntegerType(), False),
    StructField("date_sk", IntegerType(), False),
    # Natural keys (for debugging/join)
    StructField("video_id", StringType(), False),
    StructField("event_date", DateType(), False),
    # Additive measures
    StructField("views", LongType(), False),              # count of 'play'
    StructField("completes", LongType(), False),          # count of 'complete'
    StructField("likes", LongType(), False),              # count of 'like'
    StructField("pauses", LongType(), False),             # count of 'pause'
    StructField("total_watch_time_sec", LongType(), False),
    StructField("unique_viewers", LongType(), False),     # distinct user_sk
    StructField("unique_sessions", LongType(), False),    # approximated distinct
    # Calculated semantic rates
    StructField("completion_rate", DecimalType(5, 4), True),  # completes / views
    StructField("like_rate", DecimalType(5, 4), True),        # likes / views
    StructField("avg_watch_time_sec", DecimalType(10, 2), True),  # total_watch_time_sec / views
    # Metadata
    StructField("gold_processed_at", TimestampType(), False),
    StructField("batch_id", StringType(), False),
])

from pyspark.sql.types import TimestampType


def generate_fact_video_engagement(df_fact_event: DataFrame) -> DataFrame:
    """
    Build daily video engagement aggregate from fact_event.

    Steps:
    1. Filter fact_event to valid surrogate keys (video_sk not null)
    2. Group by video_sk, date_sk, video_id, event_date
    3. Compute additive measures via conditional aggregation
    4. Calculate semantic rates
    5. Add metadata
    6. Enforce schema contract

    Args:
        df_fact_event: Gold fact_event DataFrame

    Returns:
        DataFrame conforming to FACT_VIDEO_ENGAGEMENT_SCHEMA
    """
    # Filter to rows with valid video_sk (events linked to known videos)
    df_valid = df_fact_event.filter(F.col("video_sk").isNotNull())

    # Conditional aggregation
    df_agg = df_valid.groupBy("video_sk", "date_sk", "video_id", "event_date").agg(
        # Views: count of 'play' interactions
        F.count(F.when(F.col("interaction_type") == "play", True)).alias("views"),
        # Completes: count of 'complete' interactions
        F.count(F.when(F.col("interaction_type") == "complete", True)).alias("completes"),
        # Likes: count of 'like' interactions
        F.count(F.when(F.col("interaction_type") == "like", True)).alias("likes"),
        # Pauses: count of 'pause' interactions
        F.count(F.when(F.col("interaction_type") == "pause", True)).alias("pauses"),
        # Total watch time (sum across all interaction types)
        F.coalesce(F.sum("watch_time_sec"), F.lit(0)).alias("total_watch_time_sec"),
        # Unique viewers (distinct user_sk)
        F.countDistinct("user_sk").alias("unique_viewers"),
        # Unique sessions approximated by distinct user_sk per day per video
        # (In production, would use session_id if available)
        F.countDistinct("user_sk").alias("unique_sessions"),
    )

    # Calculate semantic rates
    df_rates = (
        df_agg
        .withColumn(
            "completion_rate",
            F.when(
                F.col("views") > 0,
                (F.col("completes") / F.col("views")).cast("decimal(5,4)")
            ).otherwise(F.lit(None).cast("decimal(5,4)"))
        )
        .withColumn(
            "like_rate",
            F.when(
                F.col("views") > 0,
                (F.col("likes") / F.col("views")).cast("decimal(5,4)")
            ).otherwise(F.lit(None).cast("decimal(5,4)"))
        )
        .withColumn(
            "avg_watch_time_sec",
            F.when(
                F.col("views") > 0,
                (F.col("total_watch_time_sec") / F.col("views")).cast("decimal(10,2)")
            ).otherwise(F.lit(None).cast("decimal(10,2)"))
        )
    )

    # Add metadata
    return (
        df_rates
        .withColumn("gold_processed_at", F.current_timestamp())
        .withColumn(
            "batch_id",
            F.concat(
                F.date_format(F.current_timestamp(), "yyyyMMdd_HHmmss"),
                F.lit("_gold_fact_vid_eng")
            )
        )
        .select(*[f.name for f in FACT_VIDEO_ENGAGEMENT_SCHEMA.fields])
    )


# ==================== HIGH-LEVEL ORCHESTRATION ====================

def build_fact_video_engagement(df_fact_event: DataFrame) -> DataFrame:
    """
    High-level entry point to build fact_video_engagement.

    Args:
        df_fact_event: Gold fact_event DataFrame

    Returns:
        DataFrame conforming to FACT_VIDEO_ENGAGEMENT_SCHEMA
    """
    return generate_fact_video_engagement(df_fact_event)


def run_fact_video_engagement() -> None:
    """Build and write Gold fact_video_engagement from fact_event."""
    with SparkSessionManager("gold_fact_video_engagement") as spark:
        df_fact_event = read_gold_fact(spark, "event")
        df = build_fact_video_engagement(df_fact_event)
        write_table(
            df,
            table_name="fact_video_engagement",
            layer=Layer.GOLD,
            partition_cols=["date_sk"],
            mode="overwrite",
        )


if __name__ == "__main__":
    run_fact_video_engagement()
