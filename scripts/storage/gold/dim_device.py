# scripts/storage/gold/dim_device.py
"""
Gold Layer: Device Dimension.

Static dimension mapping the three factory devices (mobile, tv, web)
with monotonic surrogate keys device_sk.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)
from scripts.storage.config import SparkSessionManager, Layer
from scripts.storage.config.io_utils import write_table

# ==================== GOLD SCHEMA CONTRACT ====================

DIM_DEVICE_SCHEMA = StructType([
    # Surrogate key
    StructField("device_sk", IntegerType(), False),
    # Natural key
    StructField("device_type", StringType(), False),
    # Label
    StructField("device_label", StringType(), False),
    # Category
    StructField("device_category", StringType(), False),  # 'mobile', 'tv', 'web'
    # Metadata
    StructField("is_active", IntegerType(), False),  # 1 = active
])


# ==================== FACTORY-ALIGNED DEVICE MASTER ====================
# Aligned with InteractionFactory.DEVICES = ("mobile", "tv", "web")

DEVICE_MASTER = [
    {"device_type": "mobile", "device_label": "Mobile Phone", "device_category": "mobile"},
    {"device_type": "tv",     "device_label": "Smart TV",      "device_category": "tv"},
    {"device_type": "web",    "device_label": "Web Browser",   "device_category": "web"},
]


def generate_dim_device(spark: SparkSession) -> DataFrame:
    """
    Generate the static device dimension DataFrame.

    Args:
        spark: Active SparkSession

    Returns:
        DataFrame conforming to DIM_DEVICE_SCHEMA
    """
    df = spark.createDataFrame(DEVICE_MASTER)

    return (
        df
        # Assign deterministic surrogate key ordered by device_type
        .withColumn("device_sk",
            F.row_number().over(Window.orderBy("device_type"))
        )
        .withColumn("is_active", F.lit(1))
        # Enforce schema contract - order columns per schema
        .select(*[f.name for f in DIM_DEVICE_SCHEMA.fields])
    )


# ==================== HIGH-LEVEL ORCHESTRATION ====================

def build_dim_device(spark: SparkSession) -> DataFrame:
    """
    High-level entry point to build dim_device.

    Args:
        spark: Active SparkSession

    Returns:
        DataFrame conforming to DIM_DEVICE_SCHEMA
    """
    return generate_dim_device(spark)

def run_dim_device() -> None:
    """Build and write Gold dim_device."""
    with SparkSessionManager("gold_dim_device") as spark:
        df = build_dim_device(spark)
        write_table(df, table_name="dim_device", layer=Layer.GOLD, mode="overwrite")


if __name__ == "__main__":
    run_dim_device()
