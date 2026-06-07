# End-to-End Sports Streaming Data Platform

> **Note:** Under active development. Full documentation will be added progressively.

## Architecture & Setup (Local-First)
This project utilizes a hybrid local-cloud setup to avoid cloud free-tier limitations during development:

- **Streaming & Ingestion:** Workflows connected through **Aiven Cloud Kafka**.
- **Bronze Layer Storage:** Simulated locally using **Fake GCS**.
- **Data Pipeline:** Developed and tested entirely via **Docker & Spark** before full cloud migration.

## Repository Structure
- `docs/`: Contains development notes and technical logs.
- `scripts/`: Ingestion and processing scripts.
- `docker-compose.yml`: Local infrastructure setup (Spark, Databases, Fake GCS).