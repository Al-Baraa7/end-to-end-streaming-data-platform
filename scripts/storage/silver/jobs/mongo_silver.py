"""
MongoDB Silver Job - Production Entry Point.

Parameterized, orchestratable pipeline driver for MongoDB Bronze to Silver transformation.
Reads Bronze videos for a specific ingestion_date, transforms, validates, and writes Silver.

Usage:
    python -m scripts.storage.silver.jobs.mongo_silver --ingestion-date 2026-09-30
    python -m scripts.storage.silver.jobs.mongo_silver --ingestion-date 2026-09-30 --env production
"""
import argparse
import sys
from datetime import datetime
from pyspark.sql import DataFrame

from scripts.storage.config import get_spark_session, get_path_builder
from scripts.storage.silver.transforms.mongo import transform_videos
from scripts.storage.silver.validation import validate_videos_silver


def read_bronze_videos(spark, ingestion_date: str) -> DataFrame:
    """Read Bronze MongoDB videos for a specific ingestion date."""
    path = get_path_builder().bronze_partitioned("mongo", ingestion_date, "videos")
    return spark.read.parquet(path)


def write_silver_videos(df: DataFrame, mode: str = "overwrite") -> None:
    """Write Silver videos to flat parquet."""
    path = get_path_builder().silver_videos()
    df.write.mode(mode).parquet(path)


def run_mongo_silver_job(
    ingestion_date: str,
    env: str = "development",
    write_mode: str = "overwrite",
    skip_validation: bool = False
) -> dict:
    """
    Execute the complete MongoDB Bronze to Silver pipeline.
    """
    app_name = f"mongo_silver_{ingestion_date.replace('-', '_')}"
    
    spark = get_spark_session(app_name, env=env)
    results = {"tables": {}, "overall_status": "success"}
    
    try:
        print(f"[INFO] Processing MongoDB videos for {ingestion_date}")
        bronze_videos = read_bronze_videos(spark, ingestion_date)
        bronze_count = bronze_videos.count()
        print(f"[INFO] Read {bronze_count} bronze videos")
        
        silver_videos = transform_videos(bronze_videos)
        
        if not skip_validation:
            validation = validate_videos_silver(silver_videos)
            validation.raise_if_failed("videos_silver_validation")
            print(f"[INFO] Videos validation passed: {validation.passed} checks")
            results["tables"]["videos"] = {"validation": {"passed": validation.passed, "failed": validation.failed}}
        else:
            print("[WARN] Skipping videos validation")
            results["tables"]["videos"] = {"validation": "skipped"}
        
        write_silver_videos(silver_videos, write_mode)
        print(f"[INFO] Wrote {silver_videos.count()} silver videos")
        results["tables"]["videos"]["output_count"] = silver_videos.count()
        
    except Exception as e:
        results["overall_status"] = "failed"
        results["error"] = str(e)
        print(f"[ERROR] Job failed: {e}", file=sys.stderr)
        raise
    finally:
        spark.stop()
    
    print(f"[INFO] MongoDB Silver job completed: {results['overall_status']}")
    return results


def main():
    parser = argparse.ArgumentParser(description="MongoDB Bronze to Silver Pipeline")
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
        run_mongo_silver_job(
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