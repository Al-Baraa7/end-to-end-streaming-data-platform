"""
Kafka Silver Layer Validators.

Validation rules for Kafka-sourced Silver events table.
Silver validation ONLY asserts data quality - no Gold construction logic.
"""
from pyspark.sql import DataFrame
from scripts.storage.silver.validation.assertions import (
    ValidationSuite,
    assert_pk_unique,
    assert_no_nulls,
    assert_column_values_in_set,
    assert_flag_consistency,
    assert_schema_conformance,
)
from scripts.storage.silver.transforms.kafka import (
    EVENTS_SILVER_SCHEMA,
    VALID_INTERACTIONS,
    VALID_DEVICES,
    DEVICE_TYPE_MAP,
)


def validate_events_silver(df: DataFrame) -> ValidationSuite:
    """
    Validate Silver Kafka events table.
    
    Checks:
    - PK uniqueness on event_id
    - Required fields not null (event_id, ingestion_date, batch_id)
    - Data quality flag consistency for all DQ flags
    - Schema conformance
    - Value set validation for interaction_type, device_type
    """
    suite = ValidationSuite()
    
    # Primary key uniqueness
    suite.add(assert_pk_unique(df, ["event_id"], "events_pk_unique"))
    
    # Required non-null fields (metadata at minimum)
    suite.add(assert_no_nulls(df, ["event_id", "ingestion_date", "batch_id"], "events_required_not_null"))
    
    # Value set validation (allowing NULL for raw fields, but flagged by DQ)
    suite.add(assert_column_values_in_set(df, "interaction_type", VALID_INTERACTIONS, "events_interaction_valid", allow_null=True))
    suite.add(assert_column_values_in_set(df, "device_type", VALID_DEVICES, "events_device_valid", allow_null=True))
    
    # DQ flag consistency checks
    # dq_missing_user_id
    suite.add(assert_flag_consistency(
        df, "dq_missing_user_id", "user_id",
        "user_id IS NULL OR trim(user_id) = ''", "events_dq_missing_user_id"
    ))
    
    # dq_missing_video_id
    suite.add(assert_flag_consistency(
        df, "dq_missing_video_id", "video_id",
        "video_id IS NULL OR trim(video_id) = ''", "events_dq_missing_video_id"
    ))
    
    # dq_missing_watch_time (for play/complete)
    suite.add(assert_flag_consistency(
        df, "dq_missing_watch_time", "interaction_type",
        "interaction_type IN ('play', 'complete') AND watch_time_sec IS NULL", "events_dq_missing_watch_time"
    ))
    
    # dq_unexpected_watch_time (for like/pause)
    suite.add(assert_flag_consistency(
        df, "dq_unexpected_watch_time", "interaction_type",
        "interaction_type IN ('like', 'pause') AND watch_time_sec IS NOT NULL", "events_dq_unexpected_watch_time"
    ))
    
    # dq_invalid_watch_time
    suite.add(assert_flag_consistency(
        df, "dq_invalid_watch_time", "watch_time_sec",
        "watch_time_sec IS NOT NULL AND (watch_time_sec <= 0 OR watch_time_sec > 120)", "events_dq_invalid_watch_time"
    ))
    
    # dq_invalid_interaction
    suite.add(assert_flag_consistency(
        df, "dq_invalid_interaction", "interaction_type",
        f"interaction_type IS NULL OR interaction_type NOT IN ({','.join(repr(i) for i in VALID_INTERACTIONS)})", "events_dq_invalid_interaction"
    ))
    
    # dq_invalid_device (uses DEVICE_TYPE_MAP normalized values)
    suite.add(assert_flag_consistency(
        df, "dq_invalid_device", "device_type",
        f"device_type IS NULL OR device_type NOT IN ({','.join(repr(d) for d in VALID_DEVICES)})", "events_dq_invalid_device"
    ))
    
    # dq_invalid_timestamp
    suite.add(assert_flag_consistency(
        df, "dq_invalid_timestamp", "event_timestamp",
        "event_timestamp IS NULL", "events_dq_invalid_timestamp"
    ))
    
    # Schema conformance
    suite.add(assert_schema_conformance(df, EVENTS_SILVER_SCHEMA, "events_schema", strict=True))
    
    return suite