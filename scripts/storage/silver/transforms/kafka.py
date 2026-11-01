"""
Kafka Silver Layer Transformations.

Pure, deterministic transformation functions for Kafka-sourced event data.
Each function takes a Bronze DataFrame and returns a Silver DataFrame with
explicit schema contracts. No I/O, no side effects, fully testable.
"""
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    IntegerType, BooleanType, DateType
)


# ==================== SILVER SCHEMA CONTRACTS ====================

VALID_INTERACTIONS = frozenset([
    "play", "pause", "complete", "like", "skip", "seek"
])

VALID_DEVICES = frozenset([
    "mobile", "desktop", "tablet", "tv", "console", "wearable"
])

# Legacy vocab mapping (Bronze → Silver)
DEVICE_TYPE_MAP = {
    "web": "desktop",
    "mobile": "mobile",
    "desktop": "desktop",
    "tablet": "tablet",
    "tv": "tv",
    "console": "console",
    "wearable": "wearable",
}

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
    StructField("dq_missing_watch_time", BooleanType(), False),
    StructField("dq_unexpected_watch_time", BooleanType(), False),
    StructField("dq_invalid_watch_time", BooleanType(), False),
    StructField("dq_invalid_interaction", BooleanType(), False),
    StructField("dq_invalid_device", BooleanType(), False),
    StructField("dq_invalid_timestamp", BooleanType(), False),
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
    - Normalize device_type: trim + lower; unknown values preserved + flagged
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
            F.to_timestamp(F.col("event_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
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
            F.create_map([F.lit(k) for pair in DEVICE_TYPE_MAP.items() for k in pair])[F.col("device_type")].otherwise(F.col("device_type"))
        )
        
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
        
        # Missing watch_time for play/complete events
        .withColumn(
            "dq_missing_watch_time",
            F.col("interaction_type").isin("play", "complete") &
            F.col("watch_time_sec").isNull()
        )
        
        # Unexpected watch_time for like/pause events
        .withColumn(
            "dq_unexpected_watch_time",
            F.col("interaction_type").isin("like", "pause") &
            F.col("watch_time_sec").isNotNull()
        )
        
        # Invalid watch_time values
        .withColumn(
            "dq_invalid_watch_time",
            F.col("watch_time_sec").isNotNull() &
            ((F.col("watch_time_sec") <= 0) | (F.col("watch_time_sec") > 120))
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