# scripts/storage/gold/dim_user.py
"""
Gold Layer: User Dimension (SCD Type 1).

Reads Silver Users, applies the Entity Quality Gate (drops rows where
any critical is_* data quality flag is True), assigns deterministic
surrogate key user_sk, and writes Gold conformed dimension.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DateType,
    BooleanType, IntegerType
)
from pyspark.sql.window import Window
from scripts.storage.config import SparkSessionManager, Layer, SourceSystem
from scripts.storage.config.io_utils import write_table, read_silver_table


# ==================== GOLD SCHEMA CONTRACT ====================

DIM_USER_SCHEMA = StructType([
    # Surrogate key
    StructField("user_sk", IntegerType(), False),
    # Natural key
    StructField("user_id", StringType(), False),
    # Attributes
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("created_at", TimestampType(), False),
    StructField("last_login", TimestampType(), True),
    StructField("account_status", StringType(), True),
    # Date-level columns for partitioning
    StructField("created_date", DateType(), False),
    StructField("last_login_date", DateType(), True),
    # Metadata
    StructField("gold_processed_at", TimestampType(), False),
    StructField("batch_id", StringType(), False),
])


# ==================== ENTITY QUALITY GATE ====================
# Critical flags from Silver Users that must ALL be False for a row to pass

CRITICAL_USER_DQ_FLAGS = [
    "is_missing_email",
    "is_invalid_country",
    "is_last_login_before_created",
    "is_invalid_account_status",
]


def apply_entity_quality_gate(df_silver_users: DataFrame) -> DataFrame:
    """
    Apply the Phase 1 Entity Quality Gate: filter out rows where ANY
    critical is_* flag is True.

    Args:
        df_silver_users: Silver users DataFrame with is_* flag columns

    Returns:
        Filtered DataFrame containing only clean rows
    """
    # Build combined filter: all critical flags must be False
    filter_expr = F.lit(True)
    for flag in CRITICAL_USER_DQ_FLAGS:
        filter_expr = filter_expr & (~F.col(flag))

    return df_silver_users.filter(filter_expr)


def generate_dim_user(df_silver_users: DataFrame) -> DataFrame:
    """
    Transform Silver Users to Gold dim_user (SCD Type 1).

    Steps:
    1. Apply Entity Quality Gate (drop rows with any critical is_* = True)
    2. Assign deterministic surrogate key user_sk via row_number over ordered user_id
    3. Add Gold metadata columns
    4. Enforce schema contract

    Args:
        df_silver_users: Silver users DataFrame (output of silver.transform_users)

    Returns:
        DataFrame conforming to DIM_USER_SCHEMA
    """
    # Step 1: Entity Quality Gate
    df_clean = apply_entity_quality_gate(df_silver_users)

    # Step 2: Deterministic surrogate key (SCD Type 1 - latest snapshot wins)
    # Order by user_id for deterministic assignment; if multiple rows per user_id
    # (should not happen in Silver), take the latest by created_at
    window_spec = Window.partitionBy("user_id").orderBy(F.col("created_at").desc())
    df_deduped = (
        df_clean
        .withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Assign global user_sk ordered by user_id for determinism
    df_with_sk = df_deduped.withColumn(
        "user_sk",
        F.row_number().over(Window.orderBy("user_id"))
    )

    # Step 3: Add Gold metadata
    return (
        df_with_sk
        .withColumn("gold_processed_at", F.current_timestamp())
        .withColumn(
            "batch_id",
            F.concat(
                F.date_format(F.current_timestamp(), "yyyyMMdd_HHmmss"),
                F.lit("_gold_user")
            )
        )
        # Enforce schema contract - order columns per schema
        .select(*[f.name for f in DIM_USER_SCHEMA.fields])
    )


# ==================== HIGH-LEVEL ORCHESTRATION ====================

def build_dim_user(df_silver_users: DataFrame) -> DataFrame:
    """
    High-level entry point to build dim_user from Silver.

    Args:
        df_silver_users: Silver users DataFrame

    Returns:
        DataFrame conforming to DIM_USER_SCHEMA
    """
    return generate_dim_user(df_silver_users)

def run_dim_user(ingestion_date: str = None) -> None:
    """Build and write Gold dim_user from Silver users."""
    import os
    ingestion_date = ingestion_date or os.getenv("INGESTION_DATE")
    if not ingestion_date:
        raise ValueError("INGESTION_DATE required (env var or argument)")

    with SparkSessionManager("gold_dim_user") as spark:
        df_silver_users = read_silver_table(spark, SourceSystem.POSTGRES, "users")
        df = build_dim_user(df_silver_users)
        write_table(df, table_name="dim_user", layer=Layer.GOLD, mode="overwrite")


if __name__ == "__main__":
    run_dim_user()
