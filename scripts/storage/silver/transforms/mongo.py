"""
MongoDB Silver Layer Transformations.

Pure, deterministic transformation functions for MongoDB-sourced video metadata.
Each function takes a Bronze DataFrame and returns a Silver DataFrame with
explicit schema contracts. No I/O, no side effects, fully testable.
"""
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    IntegerType, BooleanType, DateType, ArrayType
)


# ==================== SILVER SCHEMA CONTRACTS ====================

VALID_CONTENT_TYPES = frozenset(["vod", "live", "clip"])
VALID_STATUSES = frozenset(["published", "draft", "archived", "streaming", "ended"])
VALID_COMPETITIONS = frozenset(["World_Cup", "Champions_League", "Europa_League", "Kings_Cup", "Queens_Cup", "RSL"])

# Legacy vocab mapping (Bronze → Silver)
CONTENT_TYPE_MAP = {
    "highlight": "clip",
    "replay": "vod",
    "vod": "vod",
    "live": "live",
    "clip": "clip",
}

VIDEOS_SILVER_SCHEMA = StructType([
    StructField("video_id", StringType(), False),
    StructField("match_id", StringType(), True),
    StructField("content_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("duration_sec", IntegerType(), True),
    StructField("live_start_at", TimestampType(), True),
    StructField("live_end_at", TimestampType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("last_updated", TimestampType(), True),
    StructField("competition_id", StringType(), True),
    StructField("competition_name", StringType(), True),
    StructField("competition_teams", ArrayType(StringType()), True),
    StructField("technical_max_resolution", StringType(), True),
    StructField("technical_codec", StringType(), True),
    StructField("stats_views", IntegerType(), True),
    StructField("stats_likes", IntegerType(), True),
    StructField("languages", ArrayType(StringType()), True),
    StructField("tags", ArrayType(StringType()), True),
    # Data Quality Flags
    StructField("flag_invalid_views_likes", BooleanType(), False),
    StructField("flag_missing_live_match", BooleanType(), False),
    StructField("flag_invalid_content_status", BooleanType(), False),
    StructField("flag_invalid_status", BooleanType(), False),
    StructField("flag_invalid_competition", BooleanType(), False),
    StructField("flag_likes_gt_views", BooleanType(), False),
    StructField("flag_invalid_duration", BooleanType(), False),
    StructField("flag_missing_title", BooleanType(), False),
    StructField("flag_last_updated_before_created", BooleanType(), False),
    # Derived analytical fields
    StructField("created_date", DateType(), True),
    StructField("last_updated_date", DateType(), True),
    # Metadata
    StructField("silver_processed_at", TimestampType(), False),
])
# ==================== TRANSFORMATION FUNCTIONS ====================

def transform_videos(df_bronze: DataFrame) -> DataFrame:
    """
    Transform Bronze MongoDB videos to Silver.
    
    Business Rules:
    - Exclude MongoDB technical _id field
    - video_id is the analytical primary key
    - Flatten nested structures (competition, technical, stats) into columns
    - Preserve arrays (competition_teams, languages, tags) without exploding
    - duration_sec NULLs preserved; invalid values flagged
    - Missing title preserved and flagged
    - Live video without match_id preserved and flagged
    - Likes > views preserved and flagged
    - Invalid timestamps preserved and flagged
    - created_at / last_updated preserved with date derivatives
    - Add comprehensive data quality flags
    - Add processing metadata
    
    Args:
        df_bronze: Bronze videos DataFrame with nested MongoDB structure
        
    Returns:
        Silver videos DataFrame conforming to VIDEOS_SILVER_SCHEMA
    """
    df = df_bronze
    
    # Flatten competition struct if present
    if "competition" in df.columns:
        df = df.withColumn("competition_id", F.col("competition.id")) \
               .withColumn("competition_name", F.col("competition.name")) \
               .withColumn("competition_teams", F.col("competition.teams"))
    
    # Flatten technical struct if present
    if "technical" in df.columns:
        df = df.withColumn("technical_max_resolution", F.col("technical.max_resolution")) \
               .withColumn("technical_codec", F.col("technical.codec"))
    
    # Flatten stats struct if present
    if "stats" in df.columns:
        df = df.withColumn("stats_views", F.col("stats.views")) \
               .withColumn("stats_likes", F.col("stats.likes"))
    
    return (
        df
        .drop("_id") if "_id" in df.columns else df
        .withColumn("content_type", F.lower(F.trim(F.col("content_type"))))
        .withColumn("content_type", F.create_map([F.lit(k) for pair in CONTENT_TYPE_MAP.items() for k in pair])[F.col("content_type")].otherwise(F.col("content_type")))
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        .withColumn("title", F.trim(F.col("title")))
        .withColumn("competition_id", F.trim(F.col("competition_id")))
        .withColumn("competition_name", F.trim(F.col("competition_name")))
        .withColumn("technical_max_resolution", F.lower(F.trim(F.col("technical_max_resolution"))))
        .withColumn("technical_codec", F.lower(F.trim(F.col("technical_codec"))))
        .withColumn("duration_sec", F.col("duration_sec").cast("int"))
        .withColumn("stats_views", F.col("stats_views").cast("int"))
        .withColumn("stats_likes", F.col("stats_likes").cast("int"))
        .withColumn(
            "flag_invalid_views_likes",
            (F.col("stats_views").isNotNull() & (F.col("stats_views") < 0)) |
            (F.col("stats_likes").isNotNull() & (F.col("stats_likes") < 0))
        )
        .withColumn(
            "flag_missing_live_match",
            (F.col("content_type") == "live") &
            (F.col("match_id").isNull() | (F.trim(F.col("match_id")) == ""))
        )
        .withColumn(
            "flag_invalid_content_status",
            F.col("content_type").isNotNull() & ~F.col("content_type").isin(*VALID_CONTENT_TYPES)
        )
        .withColumn(
            "flag_invalid_status",
            F.col("status").isNotNull() & ~F.col("status").isin(*VALID_STATUSES)
        )
        .withColumn(
            "flag_likes_gt_views",
            F.col("stats_likes").isNotNull() & F.col("stats_views").isNotNull() &
            (F.col("stats_likes") > F.col("stats_views"))
        )
        .withColumn(
            "flag_invalid_duration",
            F.col("duration_sec").isNotNull() & (
                (F.col("duration_sec") <= 0) |
                ((F.col("content_type") != "live") & (F.col("duration_sec") > 86400))
            )
        )
        .withColumn(
            "flag_missing_title",
            F.col("title").isNull() | (F.trim(F.col("title")) == "")
        )
        .withColumn(
            "flag_last_updated_before_created",
            F.col("last_updated").isNotNull() & F.col("created_at").isNotNull() &
            (F.col("last_updated") < F.col("created_at"))
        )
        .withColumn("created_date", F.to_date("created_at"))
        .withColumn("last_updated_date", F.to_date("last_updated"))
        .withColumn("silver_processed_at", F.current_timestamp())
        .select(*[f.name for f in VIDEOS_SILVER_SCHEMA.fields if f.name in df.columns])
    )
def transform_videos_clean(df_bronze: DataFrame) -> DataFrame:
    """
    Alternative transform for already-flat Bronze data.
    
    Use when Bronze layer has already flattened nested structures.
    """
    return (
        df_bronze
        .drop("_id") if "_id" in df_bronze.columns else df_bronze
        
        # Normalize string fields
        .withColumn("content_type", F.lower(F.trim(F.col("content_type"))))
        .withColumn("content_type", F.create_map([F.lit(k) for pair in CONTENT_TYPE_MAP.items() for k in pair])[F.col("content_type")].otherwise(F.col("content_type")))
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        .withColumn("title", F.trim(F.col("title")))
        .withColumn("competition_id", F.trim(F.col("competition_id")))
        .withColumn("competition_name", F.trim(F.col("competition_name")))
        .withColumn("technical_max_resolution", F.lower(F.trim(F.col("technical_max_resolution"))))
        .withColumn("technical_codec", F.lower(F.trim(F.col("technical_codec"))))
        
        # Ensure numeric types
        .withColumn("duration_sec", F.col("duration_sec").cast("int"))
        .withColumn("stats_views", F.col("stats_views").cast("int"))
        .withColumn("stats_likes", F.col("stats_likes").cast("int"))
        
        # ==================== DATA QUALITY FLAGS ====================
        
        .withColumn(
            "flag_invalid_views_likes",
            (F.col("stats_views").isNotNull() & (F.col("stats_views") < 0)) |
            (F.col("stats_likes").isNotNull() & (F.col("stats_likes") < 0))
        )
        
        .withColumn(
            "flag_missing_live_match",
            (F.col("content_type") == "live") &
            (F.col("match_id").isNull() | (F.trim(F.col("match_id")) == ""))
        )
        
        .withColumn(
            "flag_invalid_content_status",
            F.col("content_type").isNotNull() & ~F.col("content_type").isin(*VALID_CONTENT_TYPES)
        )
        
        .withColumn(
            "flag_invalid_status",
            F.col("status").isNotNull() & ~F.col("status").isin(*VALID_STATUSES)
        )
        
.withColumn(
            "flag_invalid_competition",
            F.col("competition_id").isNotNull() & ~F.col("competition_id").isin(*VALID_COMPETITIONS)
        )
        .withColumn(
            "flag_likes_gt_views",
            F.col("stats_likes").isNotNull() & F.col("stats_views").isNotNull() &
            (F.col("stats_likes") > F.col("stats_views"))
        )
        
        .withColumn(
            "flag_invalid_duration",
            F.col("duration_sec").isNotNull() & (
                (F.col("duration_sec") <= 0) |
                ((F.col("content_type") != "live") & (F.col("duration_sec") > 86400))
            )
        )
        
        .withColumn(
            "flag_missing_title",
            F.col("title").isNull() | (F.trim(F.col("title")) == "")
        )
        
        .withColumn(
            "flag_last_updated_before_created",
            F.col("last_updated").isNotNull() & F.col("created_at").isNotNull() &
            (F.col("last_updated") < F.col("created_at"))
        )
        
        # ==================== DERIVED ANALYTICAL FIELDS ====================
        
        .withColumn("created_date", F.to_date("created_at"))
        .withColumn("last_updated_date", F.to_date("last_updated"))
        
        # ==================== METADATA ====================
        
        .withColumn("silver_processed_at", F.current_timestamp())
        
        # Enforce schema contract
        .select(*[f.name for f in VIDEOS_SILVER_SCHEMA.fields])
    )
