# Project Notes

## Stack & Setup
- Goal: E2E data engineering portfolio (Sposts streaming platform simulation)
- Local (Phase 1): PostgreSQL, MongoDB, Kafka (Aiven), fake-gcs, Spark, Docker
- Cloud (Phase 2): Migrate infrastructure to a cloud platform (TBD) & connect BI dashboard

## Data Design & Bronze Layer
- Postgres: `users`, `subscription`, `payments` tables
- Mongo: `videos` collection (50k docs) with nested structure (competition, technical, engagement stats)
- Intentional dirty data injected for Silver layer testing:
  - Users: 3% null emails, 2% unknown country, 5% login before created_at
  - Subs: 3% end_date before start_date
  - Payments: 2% negative amounts
- Bronze format: Parquet partitioned by `ingestion_date` saved to `fake-gcs` bucket

## Infrastructure & Kafka
- Moved Phase 1 entirely to local Docker due to cloud free-tier time limits
- Migrated Kafka from local Docker to Aiven Cloud to avoid Zookeeper setup and match real production
- Zero local Parquet file storage; uploaded directly from memory (BytesIO) to fake-gcs
- Used Docker named volumes to persist data between runs

## Spark & Docker Compose Setup
- Pure Docker setup for Spark (Master/Worker) to keep host machine clean
- Jupyter running on Docker to solve Spark connectivity issues on Windows host
- Used official `apache/spark:latest` image instead of Bitnami (since Bitnami went paid)
- Switched from Bitnami's `SPARK_MODE` env vars to explicit commands:
  - Master: `spark-class org.apache.spark.deploy.master.Master`
  - Worker: `spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077`
- Enforced worker resource limits: `--memory 2G --cores 2` for local dev

## Technical Fixes (June 2026)
- Fixed `/data/...` Permission Denied by resolving Linux UID conflict between root and jovyan user
- Fixed `PATH_NOT_FOUND` error by mounting the shared volume to `spark-master` container (not just workers)
- Fixed `IllegalParquetTypeError` (nanoseconds conflict) by disabling Spark's Vectorized Reader for the schema
