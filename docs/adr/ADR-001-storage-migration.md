# ADR-001: Storage & Analytics Layer Migration — Fake GCS to MinIO, BigQuery Emulator to DuckDB

## Status
Accepted

---

## Context

The initial architecture used Fake GCS (fake-gcs-server) as a local emulator for Google Cloud Storage.

The goal was to simulate a cloud-native storage environment locally, keeping the architecture aligned with a future GCS → BigQuery production setup.

The planned storage flow was:

```
Sources → Spark → Fake GCS → Bronze / Silver / Gold
```

---

## Problem

During the implementation phase, a critical incompatibility was discovered:

- The **Python GCS SDK** worked correctly with Fake GCS.
- **Apache Spark**, however, repeatedly failed to read from or write to Fake GCS via the GCS Connector.

The connector consistently attempted to reach the real **Google Cloud Metadata Server** instead of staying within the local environment — regardless of configuration.

### Investigation Scope

The following areas were investigated extensively:

| Area | Approaches Tried |
|------|-----------------|
| Connector versions | Multiple GCS Connector JAR versions |
| Authentication modes | Various credential and auth configurations |
| Hadoop configuration | Multiple Hadoop config parameters |
| Spark configuration | SparkSession build-time vs runtime configs |
| Docker networking | Network isolation and hostname resolution |
| Fake GCS configuration | Endpoint and scheme overrides |
| Endpoint configuration | Custom endpoint redirection attempts |

No combination resolved the issue.

---

## Investigation Result

Further research confirmed that the root cause is not specific to this project.

The combination of:

- Apache Spark 3.5.x
- Hadoop 3.3.x
- GCS Connector 2.2.x
- Fake GCS Server

...has been reported as problematic by multiple developers, particularly during write operations. This specific stack combination is rare in practice and lacks stable, well-documented support.

Continuing this investigation would provide little educational value relative to the core goal of this project, which is **Data Engineering**, not third-party compatibility debugging.

---

## Decision

### Storage Layer: Fake GCS → MinIO

MinIO will replace Fake GCS as the local storage emulator.

| Reason | Detail |
|--------|--------|
| Native S3 compatibility | MinIO implements the S3 API natively |
| Official Hadoop S3A support | Spark integrates with S3A reliably and stably |
| Stable Spark integration | Well-documented, widely tested combination |
| Better documentation | Extensive official and community resources |
| Simpler local architecture | Straightforward Docker setup with no auth workarounds |

### Gold Layer: BigQuery Emulator → DuckDB

DuckDB will replace a BigQuery emulator for the Gold layer.

| Reason | Detail |
|--------|--------|
| Lightweight | No heavy infrastructure required |
| Excellent analytical performance | Optimized for OLAP-style queries |
| Easy Power BI integration | Direct connector support |
| Ideal for local PoC | Purpose-built for embedded analytical workloads |

---

## Official Technology Stack

| Component | Version | Validation Method |
|-----------|---------|-------------------|
| Apache Spark | 3.5.7 | Tested directly in project |
| Hadoop | 3.3.4 | Tested directly in project |
| hadoop-aws | 3.3.4 | Official compatibility documentation |
| aws-java-sdk-bundle | 1.12.262 | Official compatibility documentation |
| MinIO | Pinned Docker Tag | Simple by nature; verified via Docker |
| DuckDB | 1.4.5 LTS | Simple by nature; verified via Docker |

---

## Consequences

### Positive
- Stable, well-supported Spark ↔ Storage integration via S3A
- Simplified local architecture with no authentication workarounds
- DuckDB provides a lightweight, high-performance analytical layer
- Clean separation between storage (MinIO) and analytics (DuckDB)

### Trade-offs
- Architecture diverges slightly from the original GCS → BigQuery production vision
- Future cloud migration will require adapting from S3-compatible storage back to GCS or another cloud-native storage service

---

## Lessons Learned

This migration fundamentally changed the engineering methodology of this project.

**Previous workflow:**
```
Design → Implement → Fix compatibility issues
```

**New workflow:**
```
Design → Validate Environment (PoC) → Freeze Versions → Implement
```

**Key insight:** Infrastructure stability and component compatibility must be validated before implementation begins. In team environments, this is often handled by platform or infrastructure teams. In solo or local development environments, this validation step becomes the sole responsibility of the engineer — and skipping it leads to the exact class of issues encountered here.

This lesson is not merely theoretical. It was learned through direct experience within this project, and will be applied to all future architectural decisions.

---

*Decision Date: June 2025*

