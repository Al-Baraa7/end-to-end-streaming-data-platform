"""
Gold Layer: Schema & Row Count Validation.

Validates all 11 Gold tables for:
1. Table existence (readable)
2. Schema conformance to declared schema contracts
3. Row count > 0
"""

from pyspark.sql import SparkSession
from scripts.storage.gold.dim_user import DIM_USER_SCHEMA
from scripts.storage.gold.dim_plan_pricing import DIM_PLAN_PRICING_SCHEMA
from scripts.storage.gold.dim_device import DIM_DEVICE_SCHEMA
from scripts.storage.gold.dim_date import DIM_DATE_SCHEMA
from scripts.storage.gold.dim_match import DIM_MATCH_SCHEMA
from scripts.storage.gold.dim_video import DIM_VIDEO_SCHEMA
from scripts.storage.gold.fact_subscription import FACT_SUBSCRIPTION_SCHEMA
from scripts.storage.gold.fact_payment import FACT_PAYMENT_SCHEMA
from scripts.storage.gold.fact_event import FACT_EVENT_SCHEMA
from scripts.storage.gold.fact_video_engagement import FACT_VIDEO_ENGAGEMENT_SCHEMA
from scripts.storage.gold.fact_user_daily import FACT_USER_DAILY_SCHEMA

from scripts.storage.config import Layer, SparkSessionManager
from scripts.storage.config.io_utils import read_gold_dimension, read_gold_fact
from scripts.storage.gold.validation.assertions import (
    ValidationSuite,
    assert_schema_conformance,
)


# Table -> Schema mapping
GOLD_TABLE_SCHEMAS = {
    "dim_user": DIM_USER_SCHEMA,
    "dim_plan_pricing": DIM_PLAN_PRICING_SCHEMA,
    "dim_device": DIM_DEVICE_SCHEMA,
    "dim_date": DIM_DATE_SCHEMA,
    "dim_match": DIM_MATCH_SCHEMA,
    "dim_video": DIM_VIDEO_SCHEMA,
    "fact_subscription": FACT_SUBSCRIPTION_SCHEMA,
    "fact_payment": FACT_PAYMENT_SCHEMA,
    "fact_event": FACT_EVENT_SCHEMA,
    "fact_video_engagement": FACT_VIDEO_ENGAGEMENT_SCHEMA,
    "fact_user_daily": FACT_USER_DAILY_SCHEMA,
}


def validate_table_schema_and_count(
    spark: SparkSession,
    table_name: str,
    expected_schema
) -> ValidationSuite:
    """
    Validate a single Gold table for schema conformance and row count > 0.
    
    Returns ValidationSuite with results.
    """
    suite = ValidationSuite()
    
    try:
        if table_name.startswith("dim_"):
            df = read_gold_dimension(spark, table_name.replace("dim_", ""))
        else:
            df = read_gold_fact(spark, table_name.replace("fact_", ""))
    except Exception as e:
        from scripts.storage.gold.validation.assertions import _make_result
        suite.add(_make_result(
            f"table_exists_{table_name}", False,
            f"Failed to read table '{table_name}': {e}"
        ))
        return suite
    
    # Check row count > 0
    row_count = df.count()
    if row_count == 0:
        from scripts.storage.gold.validation.assertions import _make_result
        suite.add(_make_result(
            f"row_count_{table_name}", False,
            f"Table '{table_name}' has 0 rows",
            row_count=0
        ))
    else:
        from scripts.storage.gold.validation.assertions import _make_result
        suite.add(_make_result(
            f"row_count_{table_name}", True,
            f"Table '{table_name}' has {row_count} rows",
            row_count=row_count
        ))
    
    # Schema conformance
    result = assert_schema_conformance(df, expected_schema, check_name=f"schema_{table_name}")
    suite.add(result)
    
    return suite


def validate_all_schemas_and_counts(spark: SparkSession) -> ValidationSuite:
    """
    Validate all 11 Gold tables for schema conformance and row count > 0.
    
    Returns combined ValidationSuite with all results.
    """
    master_suite = ValidationSuite()
    
    for table_name, schema in GOLD_TABLE_SCHEMAS.items():
        table_suite = validate_table_schema_and_count(spark, table_name, schema)
        for result in table_suite.results:
            master_suite.add(result)
    
    return master_suite


def run_schema_and_counts_validation() -> ValidationSuite:
    """Standalone entry point for schema and count validation."""
    with SparkSessionManager("gold_validate_schema_counts") as spark:
        return validate_all_schemas_and_counts(spark)


if __name__ == "__main__":
    suite = run_schema_and_counts_validation()
    print(f"Gold Schema & Count Validation: {suite.passed} passed, {suite.failed} failed")
    for r in suite.results:
        status = "PASS" if r.passed else "FAIL"
        print(f"  [{status}] {r.check_name}: {r.message}")
    if not suite.all_passed:
        exit(1)