"""
Gold Layer Validation Package.

Provides comprehensive validation for all 11 Gold tables:
- 6 Dimensions: dim_user, dim_plan_pricing, dim_device, dim_date, dim_match, dim_video
- 5 Facts: fact_subscription, fact_payment, fact_event, fact_video_engagement, fact_user_daily

Run full validation suite:
    python -m scripts.storage.gold.validation.run_validation
"""

from scripts.storage.gold.validation.assertions import (
    ValidationResult,
    ValidationSuite,
    assert_pk_unique,
    assert_no_nulls,
    assert_column_values_in_set,
    assert_schema_conformance,
    assert_date_bounds,
    assert_fk_integrity,
    assert_aggregate_reconciliation,
    assert_no_orphans,
)

from scripts.storage.gold.validation.schema_and_counts import (
    validate_all_schemas_and_counts,
)

from scripts.storage.gold.validation.keys_and_integrity import (
    validate_all_keys_and_integrity,
)

from scripts.storage.gold.validation.business_rules import (
    validate_all_business_rules,
)

__all__ = [
    # Core assertions
    "ValidationResult",
    "ValidationSuite",
    "assert_pk_unique",
    "assert_no_nulls",
    "assert_column_values_in_set",
    "assert_schema_conformance",
    "assert_date_bounds",
    "assert_fk_integrity",
    "assert_aggregate_reconciliation",
    "assert_no_orphans",
    # Validation modules
    "validate_all_schemas_and_counts",
    "validate_all_keys_and_integrity",
    "validate_all_business_rules",
]