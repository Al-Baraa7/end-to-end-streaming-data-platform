"""
Gold Layer: Primary Key Uniqueness & Foreign Key Referential Integrity.

Validates all 11 Gold tables for:
1. Primary key uniqueness (11 tables)
2. Foreign key referential integrity (15+ FK relationships)
"""

from pyspark.sql import SparkSession
from scripts.storage.gold.validation.assertions import (
    ValidationSuite,
    assert_pk_unique,
    assert_fk_integrity,
)
from scripts.storage.config import SparkSessionManager
from scripts.storage.config.io_utils import read_gold_dimension, read_gold_fact


# Primary key definitions for all Gold tables
GOLD_PK_DEFINITIONS = {
    "dim_user": ["user_sk"],
    "dim_plan_pricing": ["plan_pricing_sk"],
    "dim_device": ["device_sk"],
    "dim_date": ["date_sk"],
    "dim_match": ["match_sk"],
    "dim_video": ["video_sk"],
    "fact_subscription": ["subscription_sk"],
    "fact_payment": ["payment_sk"],
    "fact_event": ["event_sk"],
    "fact_video_engagement": ["video_sk", "date_sk"],
    "fact_user_daily": ["user_sk", "date_sk"],
}


# Foreign key relationships: (child_table, fk_column, parent_table, pk_column)
# All FKs allow NULL (left join semantics in fact tables)
GOLD_FK_RELATIONSHIPS = [
    # fact_subscription -> dim_user
    ("fact_subscription", "user_sk", "dim_user", "user_sk"),
    # fact_subscription -> dim_plan_pricing
    ("fact_subscription", "plan_pricing_sk", "dim_plan_pricing", "plan_pricing_sk"),
    # fact_payment -> dim_user
    ("fact_payment", "user_sk", "dim_user", "user_sk"),
    # fact_payment -> fact_subscription
    ("fact_payment", "subscription_sk", "fact_subscription", "subscription_sk"),
    # fact_payment -> dim_plan_pricing
    ("fact_payment", "plan_pricing_sk", "dim_plan_pricing", "plan_pricing_sk"),
    # fact_event -> dim_user
    ("fact_event", "user_sk", "dim_user", "user_sk"),
    # fact_event -> dim_video
    ("fact_event", "video_sk", "dim_video", "video_sk"),
    # fact_event -> dim_device
    ("fact_event", "device_sk", "dim_device", "device_sk"),
    # fact_event -> dim_date
    ("fact_event", "date_sk", "dim_date", "date_sk"),
    # fact_video_engagement -> dim_video
    ("fact_video_engagement", "video_sk", "dim_video", "video_sk"),
    # fact_video_engagement -> dim_date
    ("fact_video_engagement", "date_sk", "dim_date", "date_sk"),
    # fact_user_daily -> dim_user
    ("fact_user_daily", "user_sk", "dim_user", "user_sk"),
    # fact_user_daily -> dim_date
    ("fact_user_daily", "date_sk", "dim_date", "date_sk"),
    # dim_video -> dim_match
    ("dim_video", "match_sk", "dim_match", "match_sk"),
]


def validate_all_pk_uniqueness(spark: SparkSession) -> ValidationSuite:
    """Validate primary key uniqueness for all 11 Gold tables."""
    suite = ValidationSuite()
    
    for table_name, pk_columns in GOLD_PK_DEFINITIONS.items():
        try:
            if table_name.startswith("dim_"):
                df = read_gold_dimension(spark, table_name.replace("dim_", ""))
            else:
                df = read_gold_fact(spark, table_name.replace("fact_", ""))
            result = assert_pk_unique(df, pk_columns, check_name=f"pk_unique_{table_name}")
            suite.add(result)
        except Exception as e:
            from scripts.storage.gold.validation.assertions import _make_result
            suite.add(_make_result(
                f"pk_unique_{table_name}", False,
                f"Failed to read/validate table '{table_name}': {e}"
            ))
    
    return suite


def validate_all_fk_integrity(spark: SparkSession) -> ValidationSuite:
    """Validate foreign key referential integrity for all FK relationships."""
    suite = ValidationSuite()
    
    # Pre-load all tables to avoid re-reading
    table_cache = {}
    
    def get_table(name: str):
        if name not in table_cache:
            if name.startswith("dim_"):
                table_cache[name] = read_gold_dimension(spark, name.replace("dim_", ""))
            else:
                table_cache[name] = read_gold_fact(spark, name.replace("fact_", ""))
        return table_cache[name]
    
    for child_table, fk_col, parent_table, pk_col in GOLD_FK_RELATIONSHIPS:
        try:
            df_child = get_table(child_table)
            df_parent = get_table(parent_table)
            
            result = assert_fk_integrity(
                df_child, fk_col, df_parent, pk_col,
                check_name=f"fk_{child_table}_{fk_col}_to_{parent_table}_{pk_col}",
                allow_null_fk=True  # FKs are nullable in fact tables
            )
            suite.add(result)
        except Exception as e:
            from scripts.storage.gold.validation.assertions import _make_result
            suite.add(_make_result(
                f"fk_{child_table}_{fk_col}_to_{parent_table}_{pk_col}", False,
                f"FK validation failed for {child_table}.{fk_col} -> {parent_table}.{pk_col}: {e}"
            ))
    
    return suite


def validate_all_keys_and_integrity(spark: SparkSession) -> ValidationSuite:
    """Validate all PK uniqueness and FK integrity for Gold layer."""
    master_suite = ValidationSuite()
    
    pk_suite = validate_all_pk_uniqueness(spark)
    for r in pk_suite.results:
        master_suite.add(r)
    
    fk_suite = validate_all_fk_integrity(spark)
    for r in fk_suite.results:
        master_suite.add(r)
    
    return master_suite


def run_keys_and_integrity_validation() -> ValidationSuite:
    """Standalone entry point for keys and integrity validation."""
    with SparkSessionManager("gold_validate_keys_integrity") as spark:
        return validate_all_keys_and_integrity(spark)


if __name__ == "__main__":
    suite = run_keys_and_integrity_validation()
    print(f"Gold Keys & Integrity Validation: {suite.passed} passed, {suite.failed} failed")
    for r in suite.results:
        status = "PASS" if r.passed else "FAIL"
        print(f"  [{status}] {r.check_name}: {r.message}")
    if not suite.all_passed:
        exit(1)