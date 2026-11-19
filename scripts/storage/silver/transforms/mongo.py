# =============================================================================
# SILVER TRANSFORMER: mongo.py — HEADER (Top Imports)
# =============================================================================
"""
MongoDB Silver Layer Transformations.

Pure, deterministic transformation functions for MongoDB-sourced video metadata.
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
    IntegerType, BooleanType, DateType, ArrayType
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



# ==================== CONSTANTS (Factory-Aligned) ====================

# Valid content types from VideoSimulation.CONTENT_TYPES
VALID_CONTENT_TYPES = frozenset(["live", "highlight", "replay"])

# Valid statuses from VideoSimulation.STATUSES
VALID_STATUSES = frozenset(["streaming", "ended", "archived"])

# Valid competitions from VideoSimulation.COMPETITIONS keys
VALID_COMPETITIONS = frozenset(["RSL", "Kings_Cup"])

# Legacy vocab mapping (Bronze source → Silver analytical)
CONTENT_TYPE_MAP = {
    "live": "live",
    "highlight": "highlight",
    "replay": "replay",
    "vod": "replay",
    "clip": "highlight",
}

# Per-content-type duration bounds from VideoSimulation (R18 + match duration reality)
# Live: match duration 105-120 min → 6300-7200 sec
# Highlight: clip of match 180-600 sec
# Replay: match duration 105-120 min → 6300-7200 sec
DURATION_BOUNDS = {
    "live":      (6300, 7200),
    "highlight": (180,  600),
    "replay":    (6300, 7200),
}

# Per-content-type watch time bounds from VideoSimulation.WATCH_TIME_BOUNDS (R18)
WATCH_TIME_BOUNDS = {
    "live":      (1, 7200),
    "highlight": (1, 600),
    "replay":    (1, 7200),
}

# Valid status per content type from VideoSimulation.VALID_STATUS_BY_CONTENT_TYPE
VALID_STATUS_BY_CONTENT_TYPE = {
    "live":      frozenset(["streaming", "ended"]),
    "highlight": frozenset(["archived"]),
    "replay":    frozenset(["archived"]),
}

# Temporal tolerance for kickoff vs live_start alignment (seconds)
KICKOFF_LIVE_TOLERANCE_SEC = 300

# Like-to-view ratio bounds from VideoSimulation
LIKE_RATIO_MIN = 0.005
LIKE_RATIO_MAX = 0.03


# ==================== SILVER SCHEMA CONTRACT ====================

VIDEOS_SILVER_SCHEMA = StructType([
    # Primary identifiers
    StructField("video_id", StringType(), False),
    StructField("match_id", StringType(), True),
    StructField("content_type", StringType(), True),
    StructField("status", StringType(), True),

    # Core metadata
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("duration_sec", IntegerType(), True),
    StructField("latent_popularity", IntegerType(), True),

    # Temporal fields (flattened)
    StructField("live_start_at", TimestampType(), True),
    StructField("live_end_at", TimestampType(), True),
    StructField("kickoff_at", TimestampType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("last_updated", TimestampType(), True),

    # Competition block (flattened)
    StructField("competition_id", StringType(), True),
    StructField("match_duration_sec", IntegerType(), True),

    # Engagement stats (flattened)
    StructField("stats_views", IntegerType(), True),
    StructField("stats_likes", IntegerType(), True),


    # Tags array
    StructField("tags", ArrayType(StringType()), True),

    # Data Quality Flags (all 17 flags)
    StructField("flag_invalid_views_likes", BooleanType(), False),
    StructField("flag_missing_live_match", BooleanType(), False),
    StructField("flag_invalid_content_type", BooleanType(), False),
    StructField("flag_invalid_status", BooleanType(), False),
    StructField("flag_invalid_competition", BooleanType(), False),
    StructField("flag_likes_gt_views", BooleanType(), False),
    StructField("flag_invalid_duration", BooleanType(), False),
    StructField("flag_missing_title", BooleanType(), False),
    StructField("flag_missing_description", BooleanType(), False),
    StructField("flag_missing_live_temporal", BooleanType(), False),
    StructField("flag_invalid_live_temporal_order", BooleanType(), False),
    StructField("flag_kickoff_live_misalignment", BooleanType(), False),
    StructField("flag_created_after_live_start", BooleanType(), False),
    StructField("flag_last_updated_before_created", BooleanType(), False),
    StructField("flag_status_contradiction", BooleanType(), False),
    StructField("flag_empty_tags", BooleanType(), False),
    StructField("flag_empty_competition_struct", BooleanType(), False),
    StructField("flag_status_contradiction_live", BooleanType(), False),

    # Derived analytical fields
    StructField("created_date", DateType(), True),
    StructField("last_updated_date", DateType(), True),
    StructField("kickoff_date", DateType(), True),
    StructField("live_start_date", DateType(), True),
    StructField("live_end_date", DateType(), True),
    StructField("duration_min", IntegerType(), True),

    # Metadata
    StructField("silver_processed_at", TimestampType(), False),
    StructField("batch_id", StringType(), False),
])


# ==================== TRANSFORMATION FUNCTION ====================

def transform_videos(df_bronze: DataFrame) -> DataFrame:
    """
    Transform Bronze MongoDB videos to Silver.

    Business Rules:
    - Flatten nested structures: competition, engagement_stats, temporal
    - Normalize content_type via CONTENT_TYPE_MAP (legacy vocab support)
    - All data quality flags are ADDITIVE: preserve original values, flag anomalies
    - Compute temporal consistency flags per R5 rules
    - Derive analytical date fields and duration_min
    - Enforce explicit schema contract with all 42 fields

    Args:
        df_bronze: Bronze videos DataFrame with nested structures

    Returns:
        Silver videos DataFrame conforming to VIDEOS_SILVER_SCHEMA
    """
    # Flatten nested structures first
    df_flat = (
        df_bronze
        # Flatten competition struct
        .withColumn("competition_id", F.col("competition.id"))
        .withColumn("match_duration_sec", F.col("match_duration_sec"))

        # Flatten engagement_stats struct
        .withColumn("stats_views", F.col("stats.views"))
        .withColumn("stats_likes", F.col("stats.likes"))
    



        # Normalize content_type via legacy map
        .withColumn(
            "content_type",
            F.coalesce(
                F.col("content_type"),
                F.when(F.col("content_type").isin(*CONTENT_TYPE_MAP.keys()),
                       F.create_map([F.lit(k) for pair in CONTENT_TYPE_MAP.items() for k in pair]).getItem(F.col("content_type")))
                .otherwise(F.col("content_type"))
            )
        )
    )

    return (
        df_flat
        # --- Data Quality Flags ---

        # Invalid views/likes (negative values)
        .withColumn(
            "flag_invalid_views_likes",
            (F.col("stats_views").isNotNull() & (F.col("stats_views") < 0)) |
            (F.col("stats_likes").isNotNull() & (F.col("stats_likes") < 0))
        )

        # Missing match_id for live content
        .withColumn(
            "flag_missing_live_match",
            (F.col("content_type") == "live") &
            (F.col("match_id").isNull() | (F.trim(F.col("match_id")) == ""))
        )

        # Invalid content_type
        .withColumn(
            "flag_invalid_content_type",
            F.col("content_type").isNotNull() &
            ~F.col("content_type").isin(*VALID_CONTENT_TYPES)
        )

        # Invalid status
        .withColumn(
            "flag_invalid_status",
            F.col("status").isNotNull() &
            ~F.col("status").isin(*VALID_STATUSES)
        )

        # Invalid competition_id
        .withColumn(
            "flag_invalid_competition",
            F.col("competition_id").isNotNull() &
            ~F.col("competition_id").isin(*VALID_COMPETITIONS)
        )

        # Likes greater than views
        .withColumn(
            "flag_likes_gt_views",
            F.col("stats_likes").isNotNull() &
            F.col("stats_views").isNotNull() &
            (F.col("stats_likes") > F.col("stats_views"))
        )

        # Invalid duration per content type (using DURATION_BOUNDS)
        .withColumn(
            "flag_invalid_duration",
            F.when(
                (F.col("content_type") == "live") & F.col("duration_sec").isNotNull(),
                (F.col("duration_sec") < DURATION_BOUNDS["live"][0]) | (F.col("duration_sec") > DURATION_BOUNDS["live"][1])
            ).when(
                (F.col("content_type") == "highlight") & F.col("duration_sec").isNotNull(),
                (F.col("duration_sec") < DURATION_BOUNDS["highlight"][0]) | (F.col("duration_sec") > DURATION_BOUNDS["highlight"][1])
            ).when(
                (F.col("content_type") == "replay") & F.col("duration_sec").isNotNull(),
                (F.col("duration_sec") < DURATION_BOUNDS["replay"][0]) | (F.col("duration_sec") > DURATION_BOUNDS["replay"][1])
            ).otherwise(F.lit(False))
        )

        # Missing title
        .withColumn(
            "flag_missing_title",
            F.col("title").isNull() | (F.trim(F.col("title")) == "")
        )

        # Missing description
        .withColumn(
            "flag_missing_description",
            F.col("description").isNull() | (F.trim(F.col("description")) == "")
        )

        # Missing live temporal bounds
        .withColumn(
            "flag_missing_live_temporal",
            (F.col("content_type") == "live") &
            (F.col("live_start_at").isNull() | F.col("live_end_at").isNull())
        )

        # Invalid live temporal order
        .withColumn(
            "flag_invalid_live_temporal_order",
            F.col("live_start_at").isNotNull() &
            F.col("live_end_at").isNotNull() &
            (F.col("live_start_at") >= F.col("live_end_at"))
        )

        # Kickoff / live_start misalignment (live only)
        .withColumn(
            "flag_kickoff_live_misalignment",
            (F.col("content_type") == "live") &
            F.col("kickoff_at").isNotNull() &
            F.col("live_start_at").isNotNull() &
            (F.abs(F.unix_timestamp("kickoff_at") - F.unix_timestamp("live_start_at")) > KICKOFF_LIVE_TOLERANCE_SEC)
        )

        # Created after live_start
        .withColumn(
            "flag_created_after_live_start",
            F.col("created_at").isNotNull() &
            F.col("live_start_at").isNotNull() &
            (F.col("created_at") > F.col("live_start_at"))
        )

        # Last_updated before created_at
        .withColumn(
            "flag_last_updated_before_created",
            F.col("last_updated").isNotNull() &
            F.col("created_at").isNotNull() &
            (F.col("last_updated") < F.col("created_at"))
        )

        # Status contradiction (status not valid for content_type)
        .withColumn(
            "flag_status_contradiction",
            F.col("content_type").isNotNull() &
            F.col("status").isNotNull() &
            ~(
                (F.col("content_type") == "live") & F.col("status").isin("streaming", "ended") |
                (F.col("content_type") == "highlight") & (F.col("status") == "archived") |
                (F.col("content_type") == "replay") & (F.col("status") == "archived")
            )
        )

        # Empty tags array
        .withColumn(
            "flag_empty_tags",
            F.size(F.col("tags")) == 0
        )

        # Empty competition struct (competition_id missing)
        .withColumn(
            "flag_empty_competition_struct",
            F.col("competition_id").isNull() | (F.trim(F.col("competition_id")) == "")
        )

        # Status contradiction live: ended but live_start_at > simulation_horizon (deterministic)
        # Uses silver_processed_at as the execution reference window
        .withColumn(
            "flag_status_contradiction_live",
            (F.col("content_type") == "live") &
            (F.col("status") == "ended") &
            (F.col("live_start_at") > F.current_timestamp())
        )

        # ==================== DERIVED ANALYTICAL FIELDS ====================

        .withColumn("created_date", F.to_date("created_at"))
        .withColumn("last_updated_date", F.to_date("last_updated"))
        .withColumn("kickoff_date", F.to_date("kickoff_at"))
        .withColumn("live_start_date", F.to_date("live_start_at"))
        .withColumn("live_end_date", F.to_date("live_end_at"))
        .withColumn("duration_min", (F.col("duration_sec") / 60).cast("int"))

        # ==================== METADATA ====================

        .withColumn("silver_processed_at", F.current_timestamp())
        .withColumn(
            "batch_id",
            F.concat(
                F.date_format(F.current_timestamp(), "yyyyMMdd_HHmmss"),
                F.lit("_mongo")
            )
        )

        # ==================== ENFORCE SCHEMA CONTRACT ====================

        .select(*[f.name for f in VIDEOS_SILVER_SCHEMA.fields])
    )
# =============================================================================
# SILVER TRANSFORMER: mongo.py — FOOTER (Execution Block)
# =============================================================================
def _run_mongo_silver_job() -> DataFrame:
    """
    Orchestration entry point: Read Bronze -> Transform -> Write Silver.
    Returns videos_silver DataFrame.
    """
    if not INGESTION_DATE:
        raise ValueError("INGESTION_DATE environment variable is required")

    app_name = f"silver_mongo_{INGESTION_DATE.replace('-', '_')}"

    with SparkSessionManager(app_name, env=DEPLOY_ENV) as spark:
        # 1. Read Bronze videos
        df_videos_bronze = read_bronze_table(spark, SourceSystem.MONGO, "videos", INGESTION_DATE)

        # 2. Transform (pure function)
        videos_silver = transform_videos(df_videos_bronze)

        # 3. Write Silver
        write_table(
            videos_silver,
            table_name="videos",
            layer=Layer.SILVER,
            source=SourceSystem.MONGO,
            mode="overwrite",
        )

        return videos_silver


if __name__ == "__main__":
    _run_mongo_silver_job()
