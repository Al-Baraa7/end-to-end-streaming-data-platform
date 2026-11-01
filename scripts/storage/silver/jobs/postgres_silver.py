"""
PostgreSQL Silver Job - Production Entry Point.

Parameterized, orchestratable pipeline driver for PostgreSQL Bronze to Silver transformation.
Reads Bronze data partitioned by ingestion_date, transforms, validates, and writes Silver.

Usage:
    python -m scripts.storage.silver.jobs.postgres_silver --ingestion-date 2026-09-30
    python -m scripts.storage.silver.jobs.postgres_silver --ingestion-date 2026-09-30 --env production
"""
import argparse
import sys
from datetime import datetime
from pyspark.sql import DataFrame

from scripts.storage.config import get_spark_session, get_path_builder
from scripts.storage.silver.transforms.postgres import (
    transform_users,
    transform_subscriptions,
    transform_payments,
)
from scripts.storage.silver.validation import (
    validate_users_silver,
    validate_subscriptions_silver,
    validate_payments_silver,
)


def read_bronze_users(spark, ingestion_date: str) -> DataFrame:
    """Read Bronze users for a specific ingestion date."""
    path = get_path_builder().bronze_partitioned("postgres", ingestion_date, "users")
    return spark.read.parquet(path)


def read_bronze_subscriptions(spark, ingestion_date: str) -> DataFrame:
    """Read Bronze subscriptions for a specific ingestion date."""
    path = get_path_builder().bronze_partitioned("postgres", ingestion_date, "subscription")
    return spark.read.parquet(path)


def read_bronze_payments(spark, ingestion_date: str) -> DataFrame:
    """Read Bronze payments for a specific ingestion date."""
    path = get_path_builder().bronze_partitioned("postgres", ingestion_date, "payments")
    return spark.read.parquet(path)


def write_silver_users(df: DataFrame, mode: str = "overwrite") -> None:
    """Write Silver users to flat parquet."""
    path = get_path_builder().silver_table("postgres", "users")
    df.write.mode(mode).parquet(path)


def write_silver_subscriptions(df: DataFrame, mode: str = "overwrite") -> None:
    """Write Silver subscriptions to flat parquet."""
    path = get_path_builder().silver_table("postgres", "subscription")
    df.write.mode(mode).parquet(path)


def write_silver_payments(df: DataFrame, mode: str = "overwrite") -> None:
    """Write Silver payments to flat parquet."""
    path = get_path_builder().silver_table("postgres", "payments")
    df.write.mode(mode).parquet(path)
def run_postgres_silver_job(
    ingestion_date: str,
    env: str = "development",
    write_mode: str = "overwrite",
    skip_validation: bool = False
) -> dict:
    """
    Execute the complete PostgreSQL Bronze to Silver pipeline.
    
    Args:
        ingestion_date: Partition date in YYYY-MM-DD format
        env: Deployment environment (development/staging/production)
        write_mode: Spark write mode (overwrite/append)
        skip_validation: Skip validation (not recommended for production)
        
    Returns:
        Dict with job results and validation summaries
    """
    app_name = f"postgres_silver_{ingestion_date.replace('-', '_')}"
    
    spark = get_spark_session(app_name, env=env)
    results = {"tables": {}, "overall_status": "success"}
    
    try:
        # ---- USERS ----
        print(f"[INFO] Processing users for {ingestion_date}")
        bronze_users = read_bronze_users(spark, ingestion_date)
        bronze_count = bronze_users.count()
        print(f"[INFO] Read {bronze_count} bronze users")
        
        silver_users = transform_users(bronze_users)
        
        if not skip_validation:
            validation = validate_users_silver(silver_users)
            validation.raise_if_failed("users_silver_validation")
            print(f"[INFO] Users validation passed: {validation.passed} checks")
            results["tables"]["users"] = {"validation": {"passed": validation.passed, "failed": validation.failed}}
        else:
            print("[WARN] Skipping users validation")
            results["tables"]["users"] = {"validation": "skipped"}
        
        write_silver_users(silver_users, write_mode)
        print(f"[INFO] Wrote {silver_users.count()} silver users")
        results["tables"]["users"]["output_count"] = silver_users.count()
        
        # ---- SUBSCRIPTIONS ----
        print(f"[INFO] Processing subscriptions for {ingestion_date}")
        bronze_subs = read_bronze_subscriptions(spark, ingestion_date)
        bronze_count = bronze_subs.count()
        print(f"[INFO] Read {bronze_count} bronze subscriptions")
        
        silver_subs = transform_subscriptions(bronze_subs)
        
        if not skip_validation:
            validation = validate_subscriptions_silver(silver_subs)
            validation.raise_if_failed("subscriptions_silver_validation")
            print(f"[INFO] Subscriptions validation passed: {validation.passed} checks")
            results["tables"]["subscriptions"] = {"validation": {"passed": validation.passed, "failed": validation.failed}}
        else:
            print("[WARN] Skipping subscriptions validation")
            results["tables"]["subscriptions"] = {"validation": "skipped"}
        
        write_silver_subscriptions(silver_subs, write_mode)
        print(f"[INFO] Wrote {silver_subs.count()} silver subscriptions")
        results["tables"]["subscriptions"]["output_count"] = silver_subs.count()
        
        # ---- PAYMENTS ----
        print(f"[INFO] Processing payments for {ingestion_date}")
        bronze_payments = read_bronze_payments(spark, ingestion_date)
        bronze_count = bronze_payments.count()
        print(f"[INFO] Read {bronze_count} bronze payments")
        
        silver_payments = transform_payments(bronze_payments)
        
        if not skip_validation:
            validation = validate_payments_silver(silver_payments)
            validation.raise_if_failed("payments_silver_validation")
            print(f"[INFO] Payments validation passed: {validation.passed} checks")
            results["tables"]["payments"] = {"validation": {"passed": validation.passed, "failed": validation.failed}}
        else:
            print("[WARN] Skipping payments validation")
            results["tables"]["payments"] = {"validation": "skipped"}
        
        write_silver_payments(silver_payments, write_mode)
        print(f"[INFO] Wrote {silver_payments.count()} silver payments")
        results["tables"]["payments"]["output_count"] = silver_payments.count()
        
    except Exception as e:
        results["overall_status"] = "failed"
        results["error"] = str(e)
        print(f"[ERROR] Job failed: {e}", file=sys.stderr)
        raise
    finally:
        spark.stop()
    
    print(f"[INFO] PostgreSQL Silver job completed: {results['overall_status']}")
    return results


def main():
    parser = argparse.ArgumentParser(description="PostgreSQL Bronze to Silver Pipeline")
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
    
    # Validate date format
    try:
        datetime.strptime(args.ingestion_date, "%Y-%m-%d")
    except ValueError:
        print(f"[ERROR] Invalid date format: {args.ingestion_date}. Use YYYY-MM-DD", file=sys.stderr)
        sys.exit(1)
    
    try:
        run_postgres_silver_job(
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