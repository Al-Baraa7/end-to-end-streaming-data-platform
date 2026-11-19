# scripts/storage/gold/dim_date.py
"""
Gold Layer: Conformed Date Dimension.

Pure, deterministic generation of a date dimension table.
No I/O dependencies, fully testable. Generates a continuous range
from 2025-01-01 to 2027-12-31 with comprehensive analytical attributes.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DateType, BooleanType
)
from scripts.storage.config import SparkSessionManager, Layer
from scripts.storage.config.io_utils import write_table

# ==================== GOLD SCHEMA CONTRACT ====================

DIM_DATE_SCHEMA = StructType([
    # Surrogate key: YYYYMMDD as integer
    StructField("date_sk", IntegerType(), False),
    # Natural key
    StructField("date", DateType(), False),
    # Year attributes
    StructField("year", IntegerType(), False),
    StructField("year_month", IntegerType(), False),      # YYYYMM
    StructField("year_quarter", StringType(), False),     # e.g., '2026-Q1'
    # Quarter attributes
    StructField("quarter", IntegerType(), False),         # 1-4
    StructField("quarter_name", StringType(), False),     # 'Q1', 'Q2', etc.
    # Month attributes
    StructField("month", IntegerType(), False),           # 1-12
    StructField("month_name", StringType(), False),       # 'January', 'February', etc.
    StructField("month_short", StringType(), False),      # 'Jan', 'Feb', etc.
    # Week attributes
    StructField("week_of_year", IntegerType(), False),    # 1-53 (ISO week)
    StructField("week_of_month", IntegerType(), False),   # 1-5
    StructField("year_week", IntegerType(), False),       # YYYYWW (ISO)
    # Day attributes
    StructField("day_of_month", IntegerType(), False),    # 1-31
    StructField("day_of_year", IntegerType(), False),     # 1-366
    StructField("day_of_week", IntegerType(), False),     # 1=Monday ... 7=Sunday (ISO)
    StructField("day_name", StringType(), False),         # 'Monday', 'Tuesday', etc.
    StructField("day_short", StringType(), False),        # 'Mon', 'Tue', etc.
    # Flags
    StructField("is_weekend", BooleanType(), False),
    StructField("is_month_start", BooleanType(), False),
    StructField("is_month_end", BooleanType(), False),
    StructField("is_quarter_start", BooleanType(), False),
    StructField("is_quarter_end", BooleanType(), False),
    StructField("is_year_start", BooleanType(), False),
    StructField("is_year_end", BooleanType(), False),
])


# ==================== CONSTANTS ====================

# Date range covering simulation horizon (2026) plus buffer for late-arriving data
DATE_RANGE_START = "2025-01-01"
DATE_RANGE_END = "2027-12-31"


def generate_dim_date(spark: SparkSession) -> DataFrame:
    """
    Generate the complete conformed date dimension DataFrame.

    Args:
        spark: Active SparkSession

    Returns:
        DataFrame conforming to DIM_DATE_SCHEMA
    """
    # Create base date range using sequence
    df = spark.sql(f"""
        SELECT explode(sequence(to_date('{DATE_RANGE_START}'), to_date('{DATE_RANGE_END}'), interval 1 day)) as date
    """)

    return (
        df
        # Surrogate key: YYYYMMDD as integer
        .withColumn("date_sk", F.date_format(F.col("date"), "yyyyMMdd").cast("int"))
        # Year
        .withColumn("year", F.year("date"))
        .withColumn("year_month", (F.year("date") * 100 + F.month("date")).cast("int"))
        .withColumn("year_quarter", F.concat(F.year("date"), F.lit("-Q"), F.quarter("date")))
        # Quarter
        .withColumn("quarter", F.quarter("date"))
        .withColumn("quarter_name", F.concat(F.lit("Q"), F.quarter("date")))
        # Month
        .withColumn("month", F.month("date"))
        .withColumn("month_name", F.expr(
            "CASE month(date) WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March' "
            "WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June' WHEN 7 THEN 'July' "
            "WHEN 8 THEN 'August' WHEN 9 THEN 'September' WHEN 10 THEN 'October' "
            "WHEN 11 THEN 'November' WHEN 12 THEN 'December' END"
        ))
        .withColumn("month_short", F.expr(
            "CASE month(date) WHEN 1 THEN 'Jan' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar' "
            "WHEN 4 THEN 'Apr' WHEN 5 THEN 'May' WHEN 6 THEN 'Jun' WHEN 7 THEN 'Jul' "
            "WHEN 8 THEN 'Aug' WHEN 9 THEN 'Sep' WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' "
            "WHEN 12 THEN 'Dec' END"
        ))
        # Week (ISO week)
        .withColumn("week_of_year", F.weekofyear("date"))
        .withColumn("week_of_month", F.ceil(F.dayofmonth("date") / 7).cast("int"))
        .withColumn("year_week", (F.year("date") * 100 + F.weekofyear("date")).cast("int"))
        # Day
        .withColumn("day_of_month", F.dayofmonth("date"))
        .withColumn("day_of_year", F.dayofyear("date"))
        # ISO day of week: 1=Monday ... 7=Sunday
        .withColumn("day_of_week",
            F.when(F.dayofweek("date") == 1, 7).otherwise(F.dayofweek("date") - 1)
        )
        .withColumn("day_name", F.expr(
            "CASE dayofweek(date) WHEN 2 THEN 'Monday' WHEN 3 THEN 'Tuesday' "
            "WHEN 4 THEN 'Wednesday' WHEN 5 THEN 'Thursday' WHEN 6 THEN 'Friday' "
            "WHEN 7 THEN 'Saturday' WHEN 1 THEN 'Sunday' END"
        ))
        .withColumn("day_short", F.expr(
            "CASE dayofweek(date) WHEN 2 THEN 'Mon' WHEN 3 THEN 'Tue' WHEN 4 THEN 'Wed' "
            "WHEN 5 THEN 'Thu' WHEN 6 THEN 'Fri' WHEN 7 THEN 'Sat' WHEN 1 THEN 'Sun' END"
        ))
        # Flags
        .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))  # Sun=1, Sat=7
        .withColumn("is_month_start", F.dayofmonth("date") == 1)
        .withColumn("is_month_end",
            F.dayofmonth("date") == F.last_day("date").cast("int")
        )
        .withColumn("is_quarter_start",
            (F.month("date").isin(1, 4, 7, 10)) & (F.dayofmonth("date") == 1)
        )
        .withColumn("is_quarter_end",
            (F.month("date").isin(3, 6, 9, 12)) &
            (F.dayofmonth("date") == F.last_day("date").cast("int"))
        )
        .withColumn("is_year_start", (F.month("date") == 1) & (F.dayofmonth("date") == 1))
        .withColumn("is_year_end", (F.month("date") == 12) & (F.dayofmonth("date") == 31))
        # Enforce schema contract - order columns per schema
        .select(*[f.name for f in DIM_DATE_SCHEMA.fields])
    )


# ==================== HIGH-LEVEL ORCHESTRATION ====================

def build_dim_date(spark: SparkSession) -> DataFrame:
    """
    High-level entry point to build dim_date.

    Args:
        spark: Active SparkSession

    Returns:
        DataFrame conforming to DIM_DATE_SCHEMA
    """
    return generate_dim_date(spark)




def run_dim_date() -> None:
    """Build and write Gold dim_date."""
    with SparkSessionManager("gold_dim_date") as spark:
        df = build_dim_date(spark)
        write_table(df, table_name="dim_date", layer=Layer.GOLD, mode="overwrite")


if __name__ == "__main__":
    run_dim_date()

