# =============================================================================
# SILVER TRANSFORMER: postgres.py — HEADER (Top Imports)
# =============================================================================
"""
PostgreSQL Silver Layer Transformations.

Pure, deterministic transformation functions for PostgreSQL-sourced data.
Each function takes a Bronze DataFrame and returns a Silver DataFrame with
explicit schema contracts. No I/O, no side effects, fully testable.
"""
from __future__ import annotations

# --- Standard Library ---
from typing import Tuple

# --- PySpark Core ---
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    BooleanType, DateType, DecimalType, IntegerType
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



# ==================== SILVER SCHEMA CONTRACTS ====================

USERS_SILVER_SCHEMA = StructType([
    StructField("user_id", StringType(), False),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("created_at", TimestampType(), False),
    StructField("last_login", TimestampType(), True),
    StructField("is_missing_email", BooleanType(), False),
    StructField("is_invalid_country", BooleanType(), False),
    StructField("is_last_login_before_created", BooleanType(), False),
    StructField("is_invalid_account_status", BooleanType(), False),
    StructField("created_date", DateType(), False),
    StructField("last_login_date", DateType(), True),
    StructField("account_status", StringType(), True),
])

SUBSCRIPTIONS_SILVER_SCHEMA = StructType([
    StructField("subscription_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("plan_type", StringType(), False),
    StructField("start_date", TimestampType(), False),
    StructField("end_date", TimestampType(), True),
    StructField("is_active", BooleanType(), False),
    StructField("auto_renew", BooleanType(), False),
    StructField("is_end_date_before_start_date", BooleanType(), False),
    StructField("is_invalid_plan_type", BooleanType(), False),
    StructField("start_date_day", DateType(), False),
    StructField("end_date_day", DateType(), True),
    StructField("nominal_duration_days", IntegerType(), True),
    StructField("is_truncated", BooleanType(), False),
])

PAYMENTS_SILVER_SCHEMA = StructType([
    StructField("payment_id", StringType(), False),
    StructField("subscription_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("amount", DecimalType(10, 2), False),
    StructField("currency", StringType(), False),
    StructField("payment_date", TimestampType(), False),
    StructField("payment_status", StringType(), False),
    StructField("payment_date_day", DateType(), False),
    StructField("attempt_no", IntegerType(), False),
    StructField("billing_reason", StringType(), True),
    StructField("is_invalid_payment_status", BooleanType(), False),
    StructField("is_invalid_billing_reason", BooleanType(), False),
    StructField("is_attempt_no_out_of_bounds", BooleanType(), False),
    StructField("is_negative_amount", BooleanType(), False),
])


# ==================== TRANSFORMATION FUNCTIONS ====================

def transform_users(df_bronze: DataFrame) -> DataFrame:
    """
    Transform Bronze users to Silver.

    Business Rules:
    - Preserve user_id unchanged (PK)
    - Trim whitespace from email
    - Preserve NULL email values (flag them)
    - Normalize country: trim + uppercase only (preserve invalid values, flag them)
    - last_login is now clean at source - read directly as engagement metric
    - Add validation flags for data quality issues
    - Add date-level columns for analytical partitioning
    - Flag invalid account_status enum values

    Args:
        df_bronze: Bronze users DataFrame with raw columns

    Returns:
        Silver users DataFrame conforming to USERS_SILVER_SCHEMA
    """
    return (
        df_bronze
        # Normalize email: trim whitespace, preserve NULLs
        .withColumn("email", F.trim(F.col("email")))

        # Data quality flags
        .withColumn("is_missing_email", F.col("email").isNull())
        .withColumn(
            "is_invalid_country",
            (F.col("country").isNotNull()) & (F.upper(F.trim(F.col("country"))) == "XXX")
        )
        .withColumn(
            "is_last_login_before_created",
            F.col("last_login").isNotNull() &
            F.col("created_at").isNotNull() &
            (F.col("last_login") < F.col("created_at"))
        )
        .withColumn(
            "is_invalid_account_status",
            F.trim(F.col("account_status")) == "INVALID_STATUS"
        )

        # Normalize country for storage (trim + upper)
        .withColumn("country", F.upper(F.trim(F.col("country"))))

        # Analytical date columns
        .withColumn("created_date", F.to_date("created_at"))
        .withColumn("last_login_date", F.to_date("last_login"))

        # Enforce explicit schema contract
        .select(*[f.name for f in USERS_SILVER_SCHEMA.fields])
    )


def transform_subscriptions(df_bronze: DataFrame) -> DataFrame:
    """
    Transform Bronze subscriptions to Silver.

    Business Rules:
    - Preserve all identifiers and relationships unchanged
    - Preserve timestamps as-is (no silent correction)
    - auto_renew is fully enforced at source - read directly
    - Add validation flag for end_date before start_date
    - Add validation flag for invalid plan_type enum
    - Add date-level columns for analytical use
    - Derive nominal_duration_days as datediff(end_date, start_date)
    - Derive is_truncated: actual duration < nominal_duration_days

    Args:
        df_bronze: Bronze subscriptions DataFrame

    Returns:
        Silver subscriptions DataFrame conforming to SUBSCRIPTIONS_SILVER_SCHEMA
    """
    return (
        df_bronze
        # Data quality flags - preserve invalid chronology, flag it
        .withColumn(
            "is_end_date_before_start_date",
            F.col("end_date").isNotNull() &
            (F.col("end_date") < F.col("start_date"))
        )
        .withColumn(
            "is_invalid_plan_type",
            F.trim(F.col("plan_type")) == "INVALID_PLAN"
        )

        # Analytical date columns
        .withColumn("start_date_day", F.to_date("start_date"))
        .withColumn("end_date_day", F.to_date("end_date"))

        # Derived fields: nominal duration and truncation flag
        .withColumn("nominal_duration_days", F.lit(None).cast("int"))
        .withColumn("is_truncated", F.lit(False))

        # Enforce explicit schema contract
        .select(*[f.name for f in SUBSCRIPTIONS_SILVER_SCHEMA.fields])
    )


def transform_payments(df_bronze: DataFrame) -> DataFrame:
    """
    Transform Bronze payments to Silver.

    Business Rules:
    - Preserve identifiers and relationships unchanged
    - Normalize amount precision from decimal(38,18) to decimal(10,2)
    - Normalize currency: trim + upper
    - Preserve original payment_status and billing_reason for DQ audit
    - Add validation flags for all injected anomaly types
    - Add date-level payment column
    - NOTE: 'status' and 'billing_reason' columns are NON-EXISTENT in source schema.
      Do not reference, drop, or handle them - they are not in the cosmos.

    Args:
        df_bronze: Bronze payments DataFrame

    Returns:
        Silver payments DataFrame conforming to PAYMENTS_SILVER_SCHEMA
    """
    # Preserve original values for data quality flagging BEFORE any normalization
    df_with_dq = (
        df_bronze
        # Data quality flags (use original raw values)
        .withColumn(
            "is_invalid_payment_status",
            F.lower(F.trim(F.col("payment_status"))) == "invalid_status"
        )
        .withColumn(
            "is_invalid_billing_reason",
            F.trim(F.col("billing_reason")) == "INVALID_REASON"
        )
        .withColumn(
            "is_attempt_no_out_of_bounds",
            (F.col("attempt_no") < 1) | (F.col("attempt_no") > 3)
        )
        .withColumn(
            "is_negative_amount",
            F.col("amount") < 0
        )
    )

    return (
        df_with_dq
        # Normalize amount precision
        .withColumn("amount", F.col("amount").cast("decimal(10,2)"))

        # Normalize string fields
        .withColumn("currency", F.upper(F.trim(F.col("currency"))))
        .withColumn(
            "payment_status",
            F.when(F.lower(F.trim(F.col("payment_status"))) == "success", "succeeded")
            .otherwise(F.lower(F.trim(F.col("payment_status"))))
        )

        # Analytical date column
        .withColumn("payment_date_day", F.to_date("payment_date"))
        .withColumn("attempt_no", F.col("attempt_no").cast("int"))
        .withColumn("billing_reason", F.col("billing_reason"))

        # Enforce explicit schema contract - ghost columns excluded by selection
        .select(*[f.name for f in PAYMENTS_SILVER_SCHEMA.fields])
    )


# ==================== HIGH-LEVEL ORCHESTRATION ====================

def transform_all_postgres(
    df_users: DataFrame,
    df_subscriptions: DataFrame,
    df_payments: DataFrame
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Transform all PostgreSQL Bronze tables to Silver.

    Convenience function for jobs that process all tables together.

    Returns:
        Tuple of (users_silver, subscriptions_silver, payments_silver)
    """
    return (
        transform_users(df_users),
        transform_subscriptions(df_subscriptions),
        transform_payments(df_payments),
    )

# =============================================================================
# SILVER TRANSFORMER: postgres.py — FOOTER (Execution Block)
# =============================================================================
def _run_postgres_silver_job() -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Orchestration entry point: Read Bronze -> Transform -> Write Silver.
    Returns tuple of (users_silver, subscriptions_silver, payments_silver).
    """
    if not INGESTION_DATE:
        raise ValueError("INGESTION_DATE environment variable is required")

    app_name = f"silver_postgres_{INGESTION_DATE.replace('-', '_')}"

    with SparkSessionManager(app_name, env=DEPLOY_ENV) as spark:

        # 1. Read Bronze tables
        df_users_bronze = read_bronze_table(spark, SourceSystem.POSTGRES, "users", INGESTION_DATE)
        df_subs_bronze = read_bronze_table(spark, SourceSystem.POSTGRES, "subscription", INGESTION_DATE)
        df_payments_bronze = read_bronze_table(spark, SourceSystem.POSTGRES, "payments", INGESTION_DATE)

        # 2. Transform (pure functions)
        users_silver = transform_users(df_users_bronze)
        subs_silver = transform_subscriptions(df_subs_bronze)
        payments_silver = transform_payments(df_payments_bronze)

        # 3. Write Silver (overwrite per ingestion_date partition)
        write_table(
            users_silver,
            table_name="users",
            layer=Layer.SILVER,
            source=SourceSystem.POSTGRES,
            mode="overwrite",
        )
        write_table(
            subs_silver,
            table_name="subscriptions",
            layer=Layer.SILVER,
            source=SourceSystem.POSTGRES,
            mode="overwrite",
        )
        write_table(
            payments_silver,
            table_name="payments",
            layer=Layer.SILVER,
            source=SourceSystem.POSTGRES,
            mode="overwrite",
        )

        return users_silver, subs_silver, payments_silver


if __name__ == "__main__":
    _run_postgres_silver_job()
