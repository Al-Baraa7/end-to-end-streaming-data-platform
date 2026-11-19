# scripts/storage/gold/fact_subscription.py
"""
Gold Layer: Subscription Fact Table.

Reads clean PostgreSQL Subscriptions Silver data, calculates MRR and ARR
by dynamically referencing dim_plan_pricing, maps foreign keys to surrogate
keys (user_sk, plan_pricing_sk), and generates subscription_sk.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DateType,
    IntegerType, BooleanType, DecimalType
)
from pyspark.sql.window import Window

from scripts.storage.config import SparkSessionManager, Layer, SourceSystem
from scripts.storage.config.io_utils import write_table, read_silver_table, read_gold_dimension
# ==================== GOLD SCHEMA CONTRACT ====================

FACT_SUBSCRIPTION_SCHEMA = StructType([
    # Surrogate keys
    StructField("subscription_sk", IntegerType(), False),
    StructField("user_sk", IntegerType(), False),
    StructField("plan_pricing_sk", IntegerType(), True),
    # Natural keys
    StructField("subscription_id", StringType(), False),
    StructField("user_id", StringType(), False),
    # Attributes
    StructField("plan_type", StringType(), False),
    StructField("billing_period", StringType(), True),  # 'monthly', 'quarterly', 'annual'
    StructField("start_date", TimestampType(), False),
    StructField("end_date", TimestampType(), True),
    StructField("is_active", BooleanType(), False),
    StructField("auto_renew", BooleanType(), False),
    # Date-level for partitioning
    StructField("start_date_day", DateType(), False),
    StructField("end_date_day", DateType(), True),
    # Revenue metrics (calculated)
    StructField("nominal_duration_days", IntegerType(), True),
    StructField("mrr_usd", DecimalType(10, 2), True),      # Monthly Recurring Revenue
    StructField("arr_usd", DecimalType(10, 2), True),      # Annual Recurring Revenue
    StructField("price_usd", DecimalType(10, 2), True),    # Base price from dim_plan_pricing
    # Flags
    StructField("is_truncated", BooleanType(), False),
    StructField("price_mismatch_flag", BooleanType(), False),  # Cross-table integrity
    # Metadata
    StructField("gold_processed_at", TimestampType(), False),
    StructField("batch_id", StringType(), False),
])


# ==================== AMOUNT TO (DURATION, BILLING_PERIOD) MAPPING ====================
# Maps (plan_type, amount) to (nominal_duration_days, billing_period) for pricing lookup
# Premium tiers have unique amounts; Free tiers use datediff(end_date, start_date)

def derive_duration_and_billing_period(
    plan_type_col: str,
    amount_col: str,
    start_date_col: str,
    end_date_col: str
):
    """
    Derive nominal_duration_days and billing_period from plan_type and payment amount.
    For premium: amount uniquely identifies tier (9.99=30/monthly, 14.99=90/quarterly, 29.99=365/annual).
    For free: amount is 0.00; use datediff(end_date, start_date) capped to known tiers.
    """
    # Premium: map amount to duration/billing_period
    premium_duration = F.when(F.col(amount_col) == 9.99, 30) \
                       .when(F.col(amount_col) == 14.99, 90) \
                       .when(F.col(amount_col) == 29.99, 365) \
                       .otherwise(F.lit(None))
    
    premium_billing = F.when(F.col(amount_col) == 9.99, "monthly") \
                      .when(F.col(amount_col) == 14.99, "quarterly") \
                      .when(F.col(amount_col) == 29.99, "annual") \
                      .otherwise(F.lit(None))
    
    # Free: compute duration from datediff, snap to nearest known tier (30, 90)
    free_duration = F.when(
        F.col(end_date_col).isNotNull(),
        F.when(F.datediff(F.col(end_date_col), F.col(start_date_col)) <= 60, 30)
         .when(F.datediff(F.col(end_date_col), F.col(start_date_col)) <= 180, 90)
         .otherwise(365)
    ).otherwise(F.lit(30))  # default to monthly if no end_date
    
    free_billing = F.when(free_duration == 30, "monthly") \
                   .when(free_duration == 90, "quarterly") \
                   .when(free_duration == 365, "annual") \
                   .otherwise(F.lit(None))
    
    # Combine: premium uses amount mapping; free uses datediff mapping
    nominal_duration_days = F.when(F.col(plan_type_col) == "premium", premium_duration) \
                             .otherwise(free_duration)
    
    billing_period = F.when(F.col(plan_type_col) == "premium", premium_billing) \
                     .otherwise(free_billing)
    
    return nominal_duration_days, billing_period


def generate_fact_subscription(
    df_silver_subscriptions: DataFrame,
    df_dim_user: DataFrame,
    df_dim_plan_pricing: DataFrame,
    df_silver_payments: DataFrame
) -> DataFrame:
    """
    Build subscription fact from Silver Subscriptions + Payments.

    Steps:
    1. Deduplicate Silver payments per subscription_id (prefer attempt_no=1, else earliest payment_date)
    2. Join payments to subscriptions on subscription_id to get amount
    3. Map user_id -> user_sk via dim_user
    4. Derive nominal_duration_days and billing_period from (plan_type, amount, start_date, end_date)
    5. Join dim_plan_pricing on (plan_type, billing_period) to get price_usd and plan_pricing_sk
    6. Calculate MRR = price_usd / (nominal_duration_days / 30)
    7. Calculate ARR = MRR * 12
    8. Compute is_truncated: end_date < start_date + nominal_duration_days
    9. Cross-table integrity: flag if price_usd is null (no pricing match)
    10. Assign subscription_sk
    11. Add metadata

    Args:
        df_silver_subscriptions: Silver subscriptions DataFrame
        df_dim_user: Gold dim_user DataFrame
        df_dim_plan_pricing: Gold dim_plan_pricing DataFrame
        df_silver_payments: Silver payments DataFrame (for amount lookup)

    Returns:
        DataFrame conforming to FACT_SUBSCRIPTION_SCHEMA
    """
    # Step 1: Deduplicate payments per subscription_id (attempt_no=1 preferred, else earliest payment_date)
    payment_window = Window.partitionBy("subscription_id").orderBy(
        F.col("attempt_no").asc(), F.col("payment_date").asc()
    )
    df_payments_dedup = (
        df_silver_payments
        .withColumn("_rn", F.row_number().over(payment_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select("subscription_id", "amount", "currency")
    )

    # Step 2: Join payments to subscriptions to get amount
    df = df_silver_subscriptions.join(df_payments_dedup, on="subscription_id", how="left")

    # Step 3: Map user_id -> user_sk via dim_user
    user_lookup = df_dim_user.select("user_id", "user_sk")
    df = df.join(user_lookup, on="user_id", how="inner")

    # Step 4: Derive nominal_duration_days and billing_period from (plan_type, amount, start_date, end_date)
    dur_expr, bill_expr = derive_duration_and_billing_period(
        "plan_type", "amount", "start_date", "end_date"
    )
    df = df.withColumn("nominal_duration_days", dur_expr.cast("int"))
    df = df.withColumn("billing_period", bill_expr)

    # Step 5: Join dim_plan_pricing on (plan_type, billing_period)
    pricing_lookup = df_dim_plan_pricing.select(
        "plan_type", "billing_period", "price", "plan_pricing_sk"
    ).withColumnRenamed("price", "price_usd")

    df = df.join(
        pricing_lookup,
        on=["plan_type", "billing_period"],
        how="left"
    )

    # Step 6 & 7: Calculate MRR and ARR
    # MRR = price_usd / (nominal_duration_days / 30)
    # ARR = MRR * 12
    df = df.withColumn(
        "mrr_usd",
        F.when(
            (F.col("price_usd").isNotNull()) & (F.col("nominal_duration_days").isNotNull()) & (F.col("nominal_duration_days") > 0),
            (F.col("price_usd") / (F.col("nominal_duration_days") / 30)).cast("decimal(10,2)")
        ).otherwise(F.lit(None).cast("decimal(10,2)"))
    ).withColumn(
        "arr_usd",
        F.when(
            F.col("mrr_usd").isNotNull(),
            (F.col("mrr_usd") * 12).cast("decimal(10,2)")
        ).otherwise(F.lit(None).cast("decimal(10,2)"))
    )

    # Step 8: Compute is_truncated
    # Truncated if end_date exists and end_date < start_date + nominal_duration_days
    df = df.withColumn(
        "is_truncated",
        F.when(
            F.col("end_date").isNotNull() & F.col("nominal_duration_days").isNotNull(),
            F.col("end_date") < F.date_add(F.col("start_date"), F.col("nominal_duration_days"))
        ).otherwise(F.lit(False))
    )

    # Step 9: Cross-table integrity - price mismatch flag
    df = df.withColumn(
        "price_mismatch_flag",
        F.col("price_usd").isNull()
    )

    # Step 10: Deterministic surrogate key
    df = df.withColumn(
        "subscription_sk",
        F.row_number().over(Window.orderBy("subscription_id"))
    )

    # Step 11: Add metadata
    return (
        df
        .withColumn("gold_processed_at", F.current_timestamp())
        .withColumn(
            "batch_id",
            F.concat(
                F.date_format(F.current_timestamp(), "yyyyMMdd_HHmmss"),
                F.lit("_gold_fact_sub")
            )
        )
        # Enforce schema contract
        .select(*[f.name for f in FACT_SUBSCRIPTION_SCHEMA.fields])
    )


# ==================== HIGH-LEVEL ORCHESTRATION ====================

def build_fact_subscription(
    df_silver_subscriptions: DataFrame,
    df_dim_user: DataFrame,
    df_dim_plan_pricing: DataFrame,
    df_silver_payments: DataFrame
) -> DataFrame:
    """
    High-level entry point to build fact_subscription.

    Args:
        df_silver_subscriptions: Silver subscriptions DataFrame
        df_dim_user: Gold dim_user DataFrame
        df_dim_plan_pricing: Gold dim_plan_pricing DataFrame
        df_silver_payments: Silver payments DataFrame

    Returns:
        DataFrame conforming to FACT_SUBSCRIPTION_SCHEMA
    """
    return generate_fact_subscription(
        df_silver_subscriptions, df_dim_user, df_dim_plan_pricing, df_silver_payments
    )


def run_fact_subscription(ingestion_date: str = None) -> None:
    """Build and write Gold fact_subscription from Silver subscriptions + payments + dimensions."""
    import os
    ingestion_date = ingestion_date or os.getenv("INGESTION_DATE")
    if not ingestion_date:
        raise ValueError("INGESTION_DATE required (env var or argument)")

    with SparkSessionManager("gold_fact_subscription") as spark:
        df_silver_subs = read_silver_table(spark, SourceSystem.POSTGRES, "subscriptions")
        df_silver_pays = read_silver_table(spark, SourceSystem.POSTGRES, "payments")
        df_dim_user = read_gold_dimension(spark, "user")
        df_dim_plan = read_gold_dimension(spark, "plan_pricing")
        df = build_fact_subscription(df_silver_subs, df_dim_user, df_dim_plan, df_silver_pays)
        write_table(
            df,
            table_name="fact_subscription",
            layer=Layer.GOLD,
            partition_cols=["start_date_day"],
            mode="append",
        )



if __name__ == "__main__":
    run_fact_subscription()
