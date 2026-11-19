# scripts/storage/gold/dim_match.py
"""
Gold Layer: Match Dimension.

Extracts and populates unique match catalog profiles from clean Silver contexts.
Emits match_sk as the auto-incrementing surrogate key.
Sources: Silver Videos (Mongo) for match_id, competition, kickoff_at, etc.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DateType,
    IntegerType, BooleanType
)
from pyspark.sql.window import Window
from scripts.storage.config import SparkSessionManager, Layer, SourceSystem
from scripts.storage.config.io_utils import write_table, read_silver_table

# ==================== GOLD SCHEMA CONTRACT ====================

DIM_MATCH_SCHEMA = StructType([
    # Surrogate key
    StructField("match_sk", IntegerType(), False),
    # Natural key
    StructField("match_id", StringType(), False),
    # Core attributes (only competition_id exists in Silver; others not available)
    StructField("competition_id", StringType(), True),
    StructField("competition", StringType(), True),  # normalized from competition_id
    # Temporal
    StructField("kickoff_at", TimestampType(), True),
    StructField("kickoff_date", DateType(), True),
    StructField("kickoff_hour", IntegerType(), True),
    # Derived (always False since home_team/away_team/season not in Silver)
    StructField("is_derby", BooleanType(), False),
    StructField("is_playoff", BooleanType(), False),
    # Metadata
    StructField("gold_processed_at", TimestampType(), False),
    StructField("batch_id", StringType(), False),
])


# ==================== COMPETITION NORMALIZATION ====================

VALID_COMPETITIONS = frozenset(["RSL", "Kings_Cup"])

COMPETITION_MAP = {
    "rsl": "RSL",
    "RSL": "RSL",
    "kings_cup": "Kings_Cup",
    "kings cup": "Kings_Cup",
    "Kings Cup": "Kings_Cup",
    "KINGS_CUP": "Kings_Cup",
}


def normalize_competition(comp_col):
    """Normalize competition values to canonical set."""
    return F.when(
        F.upper(F.trim(comp_col)).isin([c.upper() for c in VALID_COMPETITIONS]),
        F.upper(F.trim(comp_col))
    ).otherwise(F.lit(None))


def generate_dim_match(df_silver_videos: DataFrame) -> DataFrame:
    """
    Build match dimension from Silver Videos.

    Args:
        df_silver_videos: Silver videos DataFrame (output of silver.transform_videos)

    Returns:
        DataFrame conforming to DIM_MATCH_SCHEMA
    """
    # Extract distinct match profiles from videos (only columns that exist in Silver)
    match_cols = ["match_id", "competition_id", "kickoff_at"]
    
    # Select only columns that exist (defensive)
    available_cols = [c for c in match_cols if c in df_silver_videos.columns]
    df_matches = df_silver_videos.select(*available_cols).dropDuplicates(["match_id"])

    return (
        df_matches
        # Normalize competition from competition_id (Silver has no 'competition' column)
        .withColumn("competition", normalize_competition(F.col("competition_id")))
        # Derived temporal fields
        .withColumn("kickoff_date", F.to_date("kickoff_at"))
        .withColumn("kickoff_hour", F.hour("kickoff_at"))
        # Derived flags: always False since home_team/away_team/season not in Silver
        .withColumn("is_derby", F.lit(False))
        .withColumn("is_playoff", F.lit(False))
        # Deterministic surrogate key ordered by match_id
        .withColumn("match_sk",
            F.row_number().over(Window.orderBy("match_id"))
        )
        # Metadata
        .withColumn("gold_processed_at", F.current_timestamp())
        .withColumn(
            "batch_id",
            F.concat(
                F.date_format(F.current_timestamp(), "yyyyMMdd_HHmmss"),
                F.lit("_gold_match")
            )
        )
        # Enforce schema contract
        .select(*[f.name for f in DIM_MATCH_SCHEMA.fields])
    )



# ==================== HIGH-LEVEL ORCHESTRATION ====================

def build_dim_match(df_silver_videos: DataFrame) -> DataFrame:
    """
    High-level entry point to build dim_match from Silver Videos.

    Args:
        df_silver_videos: Silver videos DataFrame

    Returns:
        DataFrame conforming to DIM_MATCH_SCHEMA
    """
    return generate_dim_match(df_silver_videos)

def run_dim_match(ingestion_date: str = None) -> None:
    """Build and write Gold dim_match from Silver videos."""
    import os
    ingestion_date = ingestion_date or os.getenv("INGESTION_DATE")
    if not ingestion_date:
        raise ValueError("INGESTION_DATE required (env var or argument)")

    with SparkSessionManager("gold_dim_match") as spark:
        df_silver_videos = read_silver_table(spark, SourceSystem.MONGO, "videos")
        df = build_dim_match(df_silver_videos)
        write_table(df, table_name="dim_match", layer=Layer.GOLD, mode="overwrite")


if __name__ == "__main__":
    run_dim_match()
