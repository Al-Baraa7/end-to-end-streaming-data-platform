# End-to-End Sports Streaming Data Platform

> Under active development, Full documentation will be added progressively.

## Local-First Development Approach

Because most real cloud platforms and managed services provide limited free trial periods,
the project is currently being developed using a hybrid local-cloud setup.

Current architecture:
- Streaming and ingestion workflows are connected through Aiven Cloud
- Bronze layer storage is simulated locally using Fake GCS
- Data pipelines are developed and tested locally before full cloud migration

This approach allows continuous development without being restricted by short trial periods,
while keeping the architecture ready for future deployment on real cloud services.