"""
Configuration package for the storage pipeline.
Provides centralized Spark session management, path builders, and storage configuration.
"""
from .spark import get_spark_session, SparkConfig, SparkSessionManager

from .paths import PathBuilder, Layer, SourceSystem, get_path_builder
from .storage import StorageConfig
from .io_utils import (
    write_table,
    read_bronze_table,
    read_silver_table,
    read_gold_dimension,
    read_gold_fact,
    get_kafka_bronze_checkpoint_path,
    get_kafka_bronze_streaming_path,
)

__all__ = [
    "get_spark_session",
    "SparkConfig",
    "PathBuilder",
    "get_path_builder",
    "Layer",
    "SourceSystem",
    "StorageConfig",
    "write_table",
    "read_bronze_table",
    "read_silver_table",
    "read_gold_dimension",
    "read_gold_fact",
    "get_kafka_bronze_checkpoint_path",
    "get_kafka_bronze_streaming_path",
    "SparkSessionManager",

]
