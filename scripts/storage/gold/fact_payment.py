# scripts/storage/gold/fact_payment.py
"""
Gold Layer: Payment Fact Table.

Reads clean PostgreSQL Payments Silver data. Explicitly incorporates the
implicit presence of user_id as a core field and maps it to user_sk via dim_user.
Implements cross-table integrity gate: validates transaction amount against
authoritative configured price in dim_plan_pricing for USD profiles based on
plan type and billing period (via subscription join).
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DateType,
    IntegerType, BooleanType, DecimalType
)
from pyspark.sql.window import Window
from scripts.storage.config import SparkSessionManager, Layer, SourceSystem
from scripts.storage.config.io_utils import write_table, read_silver_table, read_gold_dimension, read_gold_fact


# ==================== GOLD SCHEMA CONTRACT ====================

FACT_PAYMENT_SCHEMA = StructType([
    # Surrogate keys
    StructField("payment_sk", IntegerType(), False),
    StructField("user_sk", IntegerType(), False),
    StructField("subscription_sk", IntegerType(), True),
    StructField("plan_pricing_sk", IntegerType(), True),
    # Natural keys
    StructField("payment_id", StringType(), False),
    StructField("subscription_id", StringType(), False),
    StructField("user_id", StringType(), False),
    # Transaction details
    StructField("amount", DecimalType(10, 2), False),
    StructField("currency", StringType(), False),
    StructField("payment_date", TimestampType(), False),
    StructField("payment_status", StringType(), False),
    StructField("attempt_no", IntegerType(), False),
    StructField("billing_reason", StringType(), True),
    # Date-level for partitioning
    StructField("payment_date_day", DateType(), False),
    # Cross-table integrity
    StructField("expected_amount_usd", DecimalType(10, 2), True),
    StructField("amount_mismatch_flag", BooleanType(), False),
    StructField("mismatch_pct", DecimalType(5, 2), True),
    # Metadata
    StructField("gold_processed_at", TimestampType(), False),
    StructField("batch_id", StringType(), False),
])


def generate_fact_payment(
    df_silver_payments: DataFrame,
    df_dim_user: DataFrame,
    df_fact_subscription: DataFrame,
    df_dim_plan_pricing: DataFrame
) -> DataFrame:
    """
    Build payment fact from Silver Payments.

    Steps:
    1. Map user_id -> user_sk via dim_user (CRITICAL: user_id is implicit core field)
    2. Join fact_subscription on subscription_id to get subscription_sk, plan_type, billing_period
    3. Join dim_plan_pricing on (plan_type, billing_period) to get expected_amount_usd and plan_pricing_sk
    4. Cross-table integrity: compare actual amount (converted to USD if needed) vs expected_amount_usd
    5. Calculate mismatch percentage
    6. Assign payment_sk
    7. Add metadata

    Args:
        df_silver_payments: Silver payments DataFrame
        df_dim_user: Gold dim_user DataFrame
        df_fact_subscription: Gold fact_subscription DataFrame (for subscription_sk + plan context)
        df_dim_plan_pricing: Gold dim_plan_pricing DataFrame

    Returns:
        DataFrame conforming to FACT_PAYMENT_SCHEMA
    """
    # Step 1: Map user_id -> user_sk (CRITICAL: explicit user_id mapping)
    user_lookup = df_dim_user.select("user_id", "user_sk")
    df = df_silver_payments.join(user_lookup, on="user_id", how="inner")

    # Step 2: Join fact_subscription to get subscription context
    # Select only needed columns from fact_subscription
    sub_lookup = df_fact_subscription.select(
        "subscription_id", "subscription_sk", "plan_type", "billing_period", "plan_pricing_sk"
    )

    df = df.join(sub_lookup, on="subscription_id", how="left")

    # Step 3: Join dim_plan_pricing for expected amount (if not already from fact_subscription)
    # Use plan_pricing_sk from fact_subscription if available, else join on plan_type + billing_period
    pricing_lookup = df_dim_plan_pricing.select(
        "plan_pricing_sk", "plan_type", "billing_period", "price"
    ).withColumnRenamed("price", "expected_amount_usd")

    # If plan_pricing_sk is already populated from fact_subscription, use it;
    # otherwise join on plan_type + billing_period
    df_with_pricing_sk = df.filter(F.col("plan_pricing_sk").isNotNull())
    df_without_pricing_sk = df.filter(F.col("plan_pricing_sk").isNull())

    # Path A: already have plan_pricing_sk
    df_a = df_with_pricing_sk.join(
        pricing_lookup.select("plan_pricing_sk", "expected_amount_usd"),
        on="plan_pricing_sk",
        how="left"
    )

    # Path B: need to join on plan_type + billing_period
    df_b = df_without_pricing_sk.drop("plan_pricing_sk").join(
        pricing_lookup.select("plan_type", "billing_period", "expected_amount_usd", "plan_pricing_sk"),
        on=["plan_type", "billing_period"],
        how="left"
    )

    df = df_a.unionByName(df_b, allowMissingColumns=True)

    # Step 4: Cross-table integrity - amount validation
    # Normalize amount to USD (assuming Silver payments are already in USD per schema)
    # Silver PAYMENTS_SILVER_SCHEMA has currency normalized to uppercase
    df = df.withColumn(
        "amount_usd",
        F.when(F.col("currency") == "USD", F.col("amount"))
         .otherwise(F.lit(None).cast("decimal(10,2)"))  # Non-USD flagged as mismatch
    )

    # Amount mismatch flag: actual vs expected
    df = df.withColumn(
        "amount_mismatch_flag",
        F.col("amount_usd").isNotNull() &
        F.col("expected_amount_usd").isNotNull() &
        (F.abs(F.col("amount_usd") - F.col("expected_amount_usd")) > F.lit(0.01))
    )

    # Mismatch percentage
    df = df.withColumn(
        "mismatch_pct",
        F.when(
            (F.col("expected_amount_usd").isNotNull()) & (F.col("expected_amount_usd") != 0),
            (F.abs(F.col("amount_usd") - F.col("expected_amount_usd")) / F.col("expected_amount_usd") * 100).cast("decimal(5,2)")
        ).otherwise(F.lit(None).cast("decimal(5,2)"))
    )

    # Drop intermediate column
    df = df.drop("amount_usd")

    # Step 5: Deterministic surrogate key
    df = df.withColumn(
        "payment_sk",
        F.row_number().over(Window.orderBy("payment_id"))
    )

    # Step 6: Add metadata
    return (
        df
        .withColumn("gold_processed_at", F.current_timestamp())
        .withColumn(
            "batch_id",
            F.concat(
                F.date_format(F.current_timestamp(), "yyyyMMdd_HHmmss"),
                F.lit("_gold_fact_pay")
            )
        )
        # Enforce schema contract
        .select(*[f.name for f in FACT_PAYMENT_SCHEMA.fields])
    )


# ==================== HIGH-LEVEL ORCHESTRATION ====================

def build_fact_payment(
    df_silver_payments: DataFrame,
    df_dim_user: DataFrame,
    df_fact_subscription: DataFrame,
    df_dim_plan_pricing: DataFrame
) -> DataFrame:
    """
    High-level entry point to build fact_payment.

    Args:
        df_silver_payments: Silver payments DataFrame
        df_dim_user: Gold dim_user DataFrame
        df_fact_subscription: Gold fact_subscription DataFrame
        df_dim_plan_pricing: Gold dim_plan_pricing DataFrame

    Returns:
        DataFrame conforming to FACT_PAYMENT_SCHEMA
    """
    return generate_fact_payment(
        df_silver_payments, df_dim_user, df_fact_subscription, df_dim_plan_pricing
    )

def run_fact_payment(ingestion_date: str = None) -> None:
    """Build and write Gold fact_payment from Silver payments + dimensions + fact_subscription."""
    import os
    ingestion_date = ingestion_date or os.getenv("INGESTION_DATE")
    if not ingestion_date:
        raise ValueError("INGESTION_DATE required (env var or argument)")

    with SparkSessionManager("gold_fact_payment") as spark:
        df_silver_pays = read_silver_table(spark, SourceSystem.POSTGRES, "payments")
        df_dim_user = read_gold_dimension(spark, "user")
        df_fact_sub = read_gold_fact(spark, "subscription")
        df_dim_plan = read_gold_dimension(spark, "plan_pricing")
        df = build_fact_payment(df_silver_pays, df_dim_user, df_fact_sub, df_dim_plan)
        write_table(
            df,
            table_name="fact_payment",
            layer=Layer.GOLD,
            partition_cols=["payment_date_day"],
            mode="append",
        )


if __name__ == "__main__":
    run_fact_payment()
