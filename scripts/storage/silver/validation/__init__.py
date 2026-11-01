"""
Silver Layer Validation Package.

Provides reusable validation assertions and source-specific validators.
Silver validation ONLY asserts data quality metrics - no Gold construction logic.
"""
from .assertions import (
    ValidationResult,
    ValidationSuite,
    assert_pk_unique,
    assert_no_nulls,
    assert_column_values_in_set,
    assert_no_timestamp_drift,
    assert_flag_consistency,
    assert_row_count_match,
    assert_schema_conformance,
    assert_date_bounds,
)

from .postgres_validator import (
    validate_users_silver,
    validate_subscriptions_silver,
    validate_payments_silver,
)

from .kafka_validator import (
    validate_events_silver,
)

from .mongo_validator import (
    validate_videos_silver,
)

__all__ = [
    # Core assertions
    "ValidationResult",
    "ValidationSuite",
    "assert_pk_unique",
    "assert_no_nulls",
    "assert_column_values_in_set",
    "assert_no_timestamp_drift",
    "assert_flag_consistency",
    "assert_row_count_match",
    "assert_schema_conformance",
    "assert_date_bounds",
    # Source validators
    "validate_users_silver",
    "validate_subscriptions_silver",
    "validate_payments_silver",
    "validate_events_silver",
    "validate_videos_silver",
]