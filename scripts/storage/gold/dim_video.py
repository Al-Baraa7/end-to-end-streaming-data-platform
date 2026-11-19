# scripts/storage/gold/dim_video.py
"""
Gold Layer: Video Dimension.

Extracts full content parameters from MongoDB Silver, applies strict Entity
Quality Gate (filters rows where ANY flag_* data quality flag is True),
joins dynamically with dim_match via match_sk, and generates surrogate key video_sk.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DateType,
    IntegerType, BooleanType, ArrayType
)
from pyspark.sql.window import Window
from scripts.storage.config import SparkSessionManager, Layer, SourceSystem
from scripts.storage.config.io_utils import write_table, read_silver_table, read_gold_dimension

# ==================== GOLD SCHEMA CONTRACT ====================

DIM_VIDEO_SCHEMA = StructType([
    # Surrogate key
    StructField("video_sk", IntegerType(), False),
    # Natural key
    StructField("video_id", StringType(), False),
    # Foreign key
    StructField("match_sk", IntegerType(), True),
    # Core metadata
    StructField("content_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("duration_sec", IntegerType(), True),
    StructField("duration_min", IntegerType(), True),
    StructField("latent_popularity", IntegerType(), True),
    # Temporal fields
    StructField("live_start_at", TimestampType(), True),
    StructField("live_end_at", TimestampType(), True),
    StructField("kickoff_at", TimestampType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("last_updated", TimestampType(), True),
    # Date-level for partitioning
    StructField("created_date", DateType(), True),
    StructField("kickoff_date", DateType(), True),
    StructField("live_start_date", DateType(), True),
    StructField("live_end_date", DateType(), True),
    # Tags & competition (Silver only has competition_id; no 'competition' column)
    StructField("tags", ArrayType(StringType()), True),
    StructField("competition_id", StringType(), True),
    # Metadata
    StructField("gold_processed_at", TimestampType(), False),
    StructField("batch_id", StringType(), False),
])




# ==================== ENTITY QUALITY GATE ====================
# All flag_* columns from Silver Videos that must be False for a row to pass

VIDEO_DQ_FLAGS = [
    "flag_invalid_content_type",
    "flag_invalid_status",
    "flag_invalid_competition",
    "flag_invalid_duration",
    "flag_invalid_live_temporal_order",
    "flag_kickoff_live_misalignment",
    "flag_created_after_live_start",
    "flag_last_updated_before_created",
    "flag_status_contradiction",
    "flag_status_contradiction_live",
    "flag_empty_tags",
    "flag_empty_competition_struct",
]


def apply_video_quality_gate(df_silver_videos: DataFrame) -> DataFrame:
    """
    Apply the Phase 2 Entity Quality Gate for Videos: filter out rows
    where ANY flag_* data quality flag is True.

    Args:
        df_silver_videos: Silver videos DataFrame with flag_* columns

    Returns:
        Filtered DataFrame containing only clean rows
    """
    filter_expr = F.lit(True)
    for flag in VIDEO_DQ_FLAGS:
        if flag in df_silver_videos.columns:
            filter_expr = filter_expr & (~F.col(flag))
        # If flag column doesn't exist, treat as passed (defensive)

    return df_silver_videos.filter(filter_expr)


def generate_dim_video(
    df_silver_videos: DataFrame,
    df_dim_match: DataFrame
) -> DataFrame:
    """
    Transform Silver Videos to Gold dim_video.

    Steps:
    1. Apply Video Entity Quality Gate (drop rows with any flag_* = True)
    2. Join with dim_match to get match_sk
    3. Assign deterministic surrogate key video_sk via row_number over ordered video_id
    4. Add Gold metadata columns
    5. Enforce schema contract

    Args:
        df_silver_videos: Silver videos DataFrame (output of silver.transform_videos)
        df_dim_match: Gold dim_match DataFrame (output of build_dim_match)

    Returns:
        DataFrame conforming to DIM_VIDEO_SCHEMA
    """
    # Step 1: Entity Quality Gate
    df_clean = apply_video_quality_gate(df_silver_videos)

    # Step 2: Join with dim_match to get match_sk
    # Prepare dim_match lookup: match_id -> match_sk
    match_lookup = df_dim_match.select("match_id", "match_sk")

    df_joined = df_clean.join(
        match_lookup,
        on="match_id",
        how="left"
    )

    # Step 3: Deterministic surrogate key (ordered by video_id for reproducibility)
    df_with_sk = df_joined.withColumn(
        "video_sk",
        F.row_number().over(Window.orderBy("video_id"))
    )

    # Step 4: Add Gold metadata
    return (
        df_with_sk
        .withColumn("gold_processed_at", F.current_timestamp())
        .withColumn(
            "batch_id",
            F.concat(
                F.date_format(F.current_timestamp(), "yyyyMMdd_HHmmss"),
                F.lit("_gold_video")
            )
        )
        # Enforce schema contract - order columns per schema
        .select(*[f.name for f in DIM_VIDEO_SCHEMA.fields])
    )


# ==================== HIGH-LEVEL ORCHESTRATION ====================

def build_dim_video(
    df_silver_videos: DataFrame,
    df_dim_match: DataFrame
) -> DataFrame:
    """
    High-level entry point to build dim_video from Silver Videos and dim_match.

    Args:
        df_silver_videos: Silver videos DataFrame
        df_dim_match: Gold dim_match DataFrame

    Returns:
        DataFrame conforming to DIM_VIDEO_SCHEMA
    """
    return generate_dim_video(df_silver_videos, df_dim_match)

def run_dim_video(ingestion_date: str = None) -> None:
    """Build and write Gold dim_video from Silver videos + dim_match."""
    import os
    ingestion_date = ingestion_date or os.getenv("INGESTION_DATE")
    if not ingestion_date:
        raise ValueError("INGESTION_DATE required (env var or argument)")

    with SparkSessionManager("gold_dim_video") as spark:
        df_silver_videos = read_silver_table(spark, SourceSystem.MONGO, "videos")
        df_dim_match = read_gold_dimension(spark, "match")
        df = build_dim_video(df_silver_videos, df_dim_match)
        write_table(df, table_name="dim_video", layer=Layer.GOLD, mode="overwrite")


if __name__ == "__main__":
    run_dim_video()
