# Project Notes


## Technical Fixes (June 2026)
- Fixed `/data/...` Permission Denied by resolving Linux UID conflict between root and jovyan user
- Fixed `PATH_NOT_FOUND` error by mounting the shared volume to `spark-master` container (not just workers)
- Fixed `IllegalParquetTypeError` (nanoseconds conflict) by disabling Spark's Vectorized Reader for the schema
