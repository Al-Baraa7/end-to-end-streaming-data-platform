# scripts/storage/gold/dim_plan_pricing.py
"""
Gold Layer: Plan Pricing Reference Dimension.

Authoritative hardcoded reference seed for plan pricing matrix.
No external dependencies, fully deterministic.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, IntegerType
)
from scripts.storage.config import SparkSessionManager, Layer
from scripts.storage.config.io_utils import write_table

# ==================== GOLD SCHEMA CONTRACT ====================

DIM_PLAN_PRICING_SCHEMA = StructType([
    # Surrogate key
    StructField("plan_pricing_sk", IntegerType(), False),
    # Natural key
    StructField("plan_type", StringType(), False),
    StructField("billing_period", StringType(), False),  # 'monthly', 'annual'
    # Pricing
    StructField("price", DecimalType(10, 2), False),
    StructField("currency", StringType(), False),
    # Features (JSON string for flexibility)
    StructField("features_json", StringType(), True),
    # Metadata
    StructField("valid_from", StringType(), False),  # ISO date string
    StructField("valid_to", StringType(), True),     # NULL = current
])


# ==================== AUTHORITATIVE PRICING MATRIX ====================
# 5 exact configurations as specified:
# free: 30-day (monthly=0.00), 90-day (quarterly=0.00)
# premium: 30-day (monthly=9.99), 90-day (quarterly=14.99), 365-day (annual=29.99)

PLAN_PRICING_MATRIX = [
    {
        "plan_type": "free",
        "billing_period": "monthly",
        "price": "0.00",
        "currency": "USD",
        "features_json": '{"max_videos": 10, "hd": false, "offline": false, "concurrent_streams": 1}',
        "valid_from": "2025-01-01",
        "valid_to": None,
    },
    {
        "plan_type": "free",
        "billing_period": "quarterly",
        "price": "0.00",
        "currency": "USD",
        "features_json": '{"max_videos": 10, "hd": false, "offline": false, "concurrent_streams": 1}',
        "valid_from": "2025-01-01",
        "valid_to": None,
    },
    {
        "plan_type": "premium",
        "billing_period": "monthly",
        "price": "9.99",
        "currency": "USD",
        "features_json": '{"max_videos": -1, "hd": true, "offline": true, "concurrent_streams": 4}',
        "valid_from": "2025-01-01",
        "valid_to": None,
    },
    {
        "plan_type": "premium",
        "billing_period": "quarterly",
        "price": "14.99",
        "currency": "USD",
        "features_json": '{"max_videos": -1, "hd": true, "offline": true, "concurrent_streams": 4}',
        "valid_from": "2025-01-01",
        "valid_to": None,
    },
    {
        "plan_type": "premium",
        "billing_period": "annual",
        "price": "29.99",
        "currency": "USD",
        "features_json": '{"max_videos": -1, "hd": true, "offline": true, "concurrent_streams": 4}',
        "valid_from": "2025-01-01",
        "valid_to": None,
    },
]



def generate_dim_plan_pricing(spark: SparkSession) -> DataFrame:
    """
    Generate the authoritative plan pricing reference dimension.

    Args:
        spark: Active SparkSession

    Returns:
        DataFrame conforming to DIM_PLAN_PRICING_SCHEMA
    """
    # Create DataFrame from hardcoded matrix
    raw_schema = StructType([
        StructField("plan_type", StringType(), False),
        StructField("billing_period", StringType(), False),
        StructField("price", StringType(), False),
        StructField("currency", StringType(), False),
        StructField("features_json", StringType(), True),
        StructField("valid_from", StringType(), False),
        StructField("valid_to", StringType(), True),
    ])
    df = spark.createDataFrame(PLAN_PRICING_MATRIX, schema=raw_schema)
    

    return (
        df
        # Cast price to decimal
        .withColumn("price", F.col("price").cast("decimal(10,2)"))
        # Assign deterministic surrogate key: plan_type + billing_period ordered
        .withColumn("plan_pricing_sk",
            F.row_number().over(
                Window.orderBy("plan_type", "billing_period")
            )
        )
        # Ensure valid_to is NULL string for current records
        .withColumn("valid_to", F.when(F.col("valid_to").isNull(), F.lit(None)).otherwise(F.col("valid_to")))
        # Enforce schema contract - order columns per schema
        .select(*[f.name for f in DIM_PLAN_PRICING_SCHEMA.fields])
    )


# ==================== HIGH-LEVEL ORCHESTRATION ====================

def build_dim_plan_pricing(spark: SparkSession) -> DataFrame:
    """
    High-level entry point to build dim_plan_pricing.

    Args:
        spark: Active SparkSession

    Returns:
        DataFrame conforming to DIM_PLAN_PRICING_SCHEMA
    """
    return generate_dim_plan_pricing(spark)



def run_dim_plan_pricing() -> None:
    """Build and write Gold dim_plan_pricing."""
    with SparkSessionManager("gold_dim_plan_pricing") as spark:
        df = build_dim_plan_pricing(spark)
        write_table(df, table_name="dim_plan_pricing", layer=Layer.GOLD, mode="overwrite")


if __name__ == "__main__":
    run_dim_plan_pricing()
