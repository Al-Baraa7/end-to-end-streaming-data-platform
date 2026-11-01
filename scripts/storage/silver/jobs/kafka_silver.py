"""
Kafka Silver Job - Production Entry Point.

Parameterized, orchestratable pipeline driver for Kafka Bronze to Silver transformation.
Reads Bronze events for a specific ingestion_date, transforms, validates, and writes Silver.

Usage:
    python -m scripts.storage.silver.jobs.kafka_silver --ingestion-date 2026-09-30
    python -m scripts.storage.silver.jobs.kafka_silver --ingestion-date 2026-09-30 --env production
"""
import argparse
import sys
from datetime import datetime
from pyspark.sql import DataFrame

from scripts.storage.config import get_spark_session, get_path_builder
from scripts.storage.silver.transforms.kafka import transform_events
from scripts.storage.silver.validation import validate_events_silver


def read_bronze_events(spark, ingestion_date: str) -> DataFrame:
    """Read Bronze Kafka events for a specific ingestion date."""
    path = get_path_builder().bronze_partitioned("kafka", ingestion_date, "events")
    return spark.read.parquet(path)


def write_silver_events(df: DataFrame, mode: str = "overwrite") -> None:
    """Write Silver events to flat parquet."""
    path = get_path_builder().silver_events()
    df.write.mode(mode).parquet(path)


def run_kafka_silver_job(
    ingestion_date: str,
    env: str = "development",
    write_mode: str = "overwrite",
    skip_validation: bool = False
) -> dict:
    """
    Execute the complete Kafka Bronze to Silver pipeline.
    """
    app_name = f"kafka_silver_{ingestion_date.replace('-', '_')}"
    
    spark = get_spark_session(app_name, env=env)
    results = {"tables": {}, "overall_status": "success"}
    
    try:
        print(f"[INFO] Processing Kafka events for {ingestion_date}")
        bronze_events = read_bronze_events(spark, ingestion_date)
        bronze_count = bronze_events.count()
        print(f"[INFO] Read {bronze_count} bronze events")
        
        silver_events = transform_events(bronze_events)
        
        if not skip_validation:
            validation = validate_events_silver(silver_events)
            validation.raise_if_failed("events_silver_validation")
            print(f"[INFO] Events validation passed: {validation.passed} checks")
            results["tables"]["events"] = {"validation": {"passed": validation.passed, "failed": validation.failed}}
        else:
            print("[WARN] Skipping events validation")
            results["tables"]["events"] = {"validation": "skipped"}
        
        write_silver_events(silver_events, write_mode)
        print(f"[INFO] Wrote {silver_events.count()} silver events")
        results["tables"]["events"]["output_count"] = silver_events.count()
        
    except Exception as e:
        results["overall_status"] = "failed"
        results["error"] = str(e)
        print(f"[ERROR] Job failed: {e}", file=sys.stderr)
        raise
    finally:
        spark.stop()
    
    print(f"[INFO] Kafka Silver job completed: {results['overall_status']}")
    return results


def main():
    parser = argparse.ArgumentParser(description="Kafka Bronze to Silver Pipeline")
    parser.add_argument(
        "--ingestion-date",
        required=True,
        help="Ingestion date partition (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--env",
        default="development",
        choices=["development", "staging", "production"],
        help="Deployment environment"
    )
    parser.add_argument(
        "--write-mode",
        default="overwrite",
        choices=["overwrite", "append"],
        help="Spark write mode"
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip validation checks (not recommended)"
    )
    
    args = parser.parse_args()
    
    try:
        datetime.strptime(args.ingestion_date, "%Y-%m-%d")
    except ValueError:
        print(f"[ERROR] Invalid date format: {args.ingestion_date}. Use YYYY-MM-DD", file=sys.stderr)
        sys.exit(1)
    
    try:
        run_kafka_silver_job(
            ingestion_date=args.ingestion_date,
            env=args.env,
            write_mode=args.write_mode,
            skip_validation=args.skip_validation
        )
    except Exception as e:
        print(f"[FATAL] Job failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()