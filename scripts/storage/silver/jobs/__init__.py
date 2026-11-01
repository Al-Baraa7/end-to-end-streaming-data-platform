"""
Silver Jobs Package - Production Pipeline Entry Points.

Each module provides a parameterized, orchestratable driver for Bronze to Silver transformation.
"""
from .postgres_silver import run_postgres_silver_job, main as run_postgres
from .kafka_silver import run_kafka_silver_job, main as run_kafka
from .mongo_silver import run_mongo_silver_job, main as run_mongo

__all__ = [
    "run_postgres_silver_job",
    "run_kafka_silver_job",
    "run_mongo_silver_job",
    "run_postgres",
    "run_kafka",
    "run_mongo",
]