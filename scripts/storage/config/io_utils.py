# scripts/storage/config/io_utils.py
"""
Centralized I/O Utilities for Storage Pipeline.

Provides unified read/write helpers that leverage the centralized
PathBuilder and Layer configuration. Eliminates hardcoded paths
and write logic scattered across transform modules.
"""
from __future__ import annotations

from typing import Optional, List
from pyspark.sql import DataFrame, SparkSession

from scripts.storage.config import (
    get_path_builder,
    PathBuilder,
    Layer,
    SourceSystem,
)



# =============================================================================
# PATH BUILDER SHORTCUTS (Module-level singletons)
# =============================================================================
_PATHS: PathBuilder = get_path_builder()


# =============================================================================
# WRITE UTILITY
# =============================================================================
def write_table(
    df: DataFrame,
    table_name: str,
    *,
    layer: Layer,
    source: Optional[SourceSystem] = None,
    partition_cols: Optional[List[str]] = None,
    mode: str = "overwrite",
    format: str = "parquet",
    overwrite_schema: bool = True,
    ingestion_date: Optional[str] = None,
) -> str:
    """
    Materialize a DataFrame to the configured storage backend (S3A/MinIO).

    Path Resolution (via PathBuilder):
    - Bronze: s3a://{bronze_bucket}/{source}/ingestion_date={ingestion_date}/{table}.parquet
    - Silver: s3a://{silver_bucket}/{source}/{table}.parquet
    - Gold (dim): s3a://{gold_bucket}/dim_{table}
    - Gold (fact): s3a://{gold_bucket}/fact_{table}

    Args:
        df: DataFrame to persist (must conform to schema contract)
        table_name: Logical table name (e.g., "users", "fact_subscription")
        layer: Target storage layer (Layer.BRONZE | Layer.SILVER | Layer.GOLD)
        source: SourceSystem (required for Bronze/Silver)
        partition_cols: Columns to partition by (e.g., ["event_date", "user_sk"])
        mode: Write mode — "overwrite" (default for dims/silver) or "append" (for fact increments)
        format: File format — "parquet" (default) or "delta"
        overwrite_schema: Allow schema evolution on overwrite (default True)
        ingestion_date: YYYY-MM-DD (required for Bronze; defaults to env INGESTION_DATE)

    Returns:
        The resolved target path that was written to.
    """
    import os

    # Resolve ingestion_date for Bronze
    if layer == Layer.BRONZE:
        if not source:
            raise ValueError("Bronze writes require a SourceSystem")
        ingestion_date = ingestion_date or os.getenv("INGESTION_DATE")
        if not ingestion_date:
            raise ValueError("INGESTION_DATE required for Bronze partitioning (env var or argument)")

    # Resolve target path via PathBuilder
    if layer == Layer.BRONZE:
        target_path = _PATHS.bronze_partitioned(source, ingestion_date, table_name)

    elif layer == Layer.SILVER:
        if not source:
            raise ValueError("Silver writes require a SourceSystem")
        target_path = _PATHS.silver_table(source, table_name)

    elif layer == Layer.GOLD:
        if table_name.startswith("dim_"):
            dim_name = table_name.replace("dim_", "")
            target_path = _PATHS.gold_dimension(dim_name)
        elif table_name.startswith("fact_"):
            fact_name = table_name.replace("fact_", "")
            target_path = _PATHS.gold_fact(fact_name)
        else:
            target_path = f"{_PATHS.gold_base()}/{table_name}"
    else:
        raise ValueError(f"Unsupported layer: {layer}")

    # Execute write
    writer = (
        df.write
        .mode(mode)
        .format(format)
        .option("overwriteSchema", str(overwrite_schema).lower())
    )

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.save(target_path)

    return target_path


# =============================================================================
# READ UTILITIES
# =============================================================================
def read_bronze_table(
    spark: SparkSession,
    source: SourceSystem,
    table: str,
    ingestion_date: str,
    format: str = "parquet",
) -> DataFrame:
    """Read a Bronze table for a specific ingestion date."""
    path = _PATHS.bronze_partitioned(source, ingestion_date, table)
    return spark.read.format(format).load(path)


def read_silver_table(
    spark: SparkSession,
    source: SourceSystem,
    table: str,
    format: str = "parquet",
) -> DataFrame:
    """Read a Silver table."""
    path = _PATHS.silver_table(source, table)
    return spark.read.format(format).load(path)


def read_gold_dimension(
    spark: SparkSession,
    name: str,
    format: str = "parquet",
) -> DataFrame:
    """Read a Gold dimension table."""
    path = _PATHS.gold_dimension(name)
    return spark.read.format(format).load(path)


def read_gold_fact(
    spark: SparkSession,
    name: str,
    format: str = "parquet",
) -> DataFrame:
    """Read a Gold fact table."""
    path = _PATHS.gold_fact(name)
    return spark.read.format(format).load(path)


# =============================================================================
# STREAMING CHECKPOINT HELPERS
# =============================================================================
def get_kafka_bronze_checkpoint_path() -> str:
    """Get the Kafka-to-Bronze streaming checkpoint path."""
    return _PATHS.bronze_kafka_checkpoint()


def get_kafka_bronze_streaming_path(ingestion_date: str) -> str:
    """Get the Kafka Bronze streaming base path for a date."""
    return _PATHS.bronze_kafka_streaming(ingestion_date)
