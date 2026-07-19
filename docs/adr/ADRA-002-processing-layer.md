# ADR-002: Processing Layer Re-Engineering — Pandas to PySpark Migration and Containerized Environment Optimization

## Status
Accepted

---

## Context

Following the storage layer stabilization on MinIO (see ADR-001), work progressed on building the ingestion pipeline for raw data from live sources: PostgreSQL, MongoDB, and Kafka event streams (Aiven Cloud).

At this stage, Bronze-layer ingestion scripts (including the Kafka Consumer) were still built on Pandas, a single-node, in-memory processing library. This design predates the project's full containerization and was carried over from early development.

---

## Problem

### 1. Pandas–Spark Timestamp Incompatibility

Pandas and Spark differ in their default timestamp precision: Pandas persists datetime values at nanosecond resolution, while Spark's Parquet handling defaults to microsecond resolution.

This mismatch surfaced twice, independently, at different pipeline stages:

- **During Silver-layer reads:** Spark failed to parse Parquet files written by Pandas-based Bronze scripts due to unsupported nanosecond timestamps.
- **During Silver-layer writes, after the MinIO migration:** the same underlying incompatibility resurfaced when writing processed Silver output, confirming the issue was systemic rather than isolated to a single read or write path.

The recurrence of the same root cause in two unrelated contexts indicated that the problem was not a configuration edge case, but a structural mismatch between the two processing libraries.

### 2. Dockerfile Layering and Dependency Fragmentation

As the project moved toward running the full stack inside Docker (Spark Master and Worker, Jupyter, PySpark), the Dockerfile's layered structure led to dependency fragmentation. Required libraries and JARs for PostgreSQL, MongoDB, and Kafka connectivity were inconsistently distributed across image layers, resulting in module import errors and broken paths between derived containers (Master, Worker, and Jupyter).

### 3. SSL Certificate Handling Inside Containers

The Kafka Consumer previously ran on the host machine, where it read the Aiven SSL certificate directly via a local file path. Once containerized, this approach broke: the host-based path was no longer valid inside the container's isolated filesystem, and Spark's Kafka client required the certificate content itself, passed as a string, rather than a filesystem reference.

---

## Decisions

### Decision 1: Full Migration from Pandas to PySpark

All Bronze and Silver processing logic, including ingestion, transformation, and the Kafka Consumer, is migrated entirely from Pandas to PySpark.

**Rationale:**
- Eliminates the structural timestamp incompatibility at its root, rather than patching it at each occurrence.
- Aligns the processing layer with a distributed-computing model suitable for scaling beyond single-node memory constraints.
- Consolidates the project on a single processing engine, reducing long-term maintenance complexity.

### Decision 2: Containerized Environment via Base Layer Consolidation


As part of the broader move to run the entire pipeline inside Docker, with no external, host-dependent tooling required during execution, all required libraries, JARs, and system dependencies for Spark, PostgreSQL, MongoDB, and Kafka connectors are installed in the earliest, base layer of the Dockerfile, from which all derived containers (Spark Master, Worker, and Jupyter) inherit.

**Rationale:**
- Docker layer caching means earlier layers change less frequently and are reused across rebuilds. Placing the heaviest, most stable dependencies at the base minimizes redundant downloads when only lightweight, frequently-changing layers, such as Jupyter configuration, are modified.
- This ordering was adopted deliberately in light of limited local bandwidth, making efficient cache reuse a practical engineering constraint rather than a purely theoretical optimization.
- Resolves prior cross-container dependency mismatches by ensuring a single, consistent dependency baseline, and ensures the full pipeline can run end-to-end from within the containerized environment alone.

### Decision 3: Certificate Delivery via In-Memory String Content

The Kafka Consumer now reads the SSL certificate path from a centralized configuration file, opens it in text read mode, and passes its content directly to the Spark Kafka client as a string, rather than referencing a filesystem path.

**Rationale:**
- Spark's Kafka client expects certificate content, not a file reference, for SSL authentication in this context.
- Centralizing the certificate path in configuration, rather than hardcoding a host-specific path, keeps the setup portable across environments.

---

## Consequences

### Positive
- Single, consistent processing engine (PySpark) across the entire pipeline, eliminating the Pandas/Spark compatibility class of bugs entirely.
- Stable, reproducible container builds with no runtime dependency fetching or external volume workarounds.
- Kafka Consumer now runs reliably inside the containerized environment, with no host-dependent paths.
- The full pipeline, from ingestion scripts through Spark processing, can run entirely within the containerized environment without external setup.

### Trade-offs
- Base Docker image size increased due to consolidating all Java dependencies and JARs into a single early layer — a thicker base image in exchange for build stability and cache efficiency.
- Full Pandas removal required rewriting existing Bronze-layer scripts, representing additional short-term development effort.

---

## Lessons Learned

This decision reinforced the root-cause analysis principle established in ADR-001: rather than addressing each symptom independently, investigating the structural origin of recurring errors leads to more durable solutions. Here, two distinct failure points — a read-time error and a write-time error — were resolved through a single architectural change, confirming that surface-level symptoms can mask a deeper, unified cause.

It also highlighted that infrastructure decisions, such as Docker layer ordering, are not purely about correctness, but must account for real operating constraints, including local resource and bandwidth limitations.

---

*Decision Date: Jul 2026*