# P7 2.0 before and after

| Capability | Before | After (2026-09-03) |
| --- | --- | --- |
| Six-domain CDC | strong implementation and earlier single-table/chain runtime evidence | 133-row six-domain initial snapshot plus 152-event incremental accounting run with zero rejection |
| Source contracts | source tables omitted tenant fields in five domains and consent fields in marketing | source DDL, connector routes, normalized topics, contracts, consumer, and dbt staging aligned; machine-readable manifest and field matrix added |
| CDC numeric semantics | Debezium decimal strings failed normalized numeric contracts | two allowlisted decimal fields normalize at the envelope boundary; arbitrary strings still fail closed |
| Ordering | tested fixture logic; live consumer did not populate the normalized sequence | PostgreSQL LSN now supplies sequence; live A→B→C finished at C and a three-offset replay stayed idempotent |
| DLQ behavior | file ingestion quarantined failures; live consumer could crash during mapping | live consumer now records per-message Debezium normalization failures and continues bounded processing |
| Identity | tenant-scoped graph, deterministic matching, review queue, survivorship | formal 12-case matrix, explicit false-merge/false-split coverage, 133 linked source records, and masked field provenance evidence |
| Privacy | Python was the final authority; dbt exports read Customer 360 directly | dbt computes a governed fail-closed eligibility mart; every dbt export inner-joins it; Python remains the final destination enforcement layer |
| Activation | six exports and four simulators with idempotency/retry logic | current 48-candidate accounting: 39 success, 8 suppression, 1 permanent failure, 4 retries, zero unaccounted |
| dbt | 28 models, 2 snapshots, 56 tests, 41 sources | locally executed 29 models, 2 snapshots, 62 tests, 42 sources, 5 exposures; all passed |
| Airflow | recorded 20-task success and recovery evidence | current 20/20 run in 22.224s plus current breaking-contract failure and corrected rerun |
| Snowflake | recorded live validation and 7-dataset parity | preserved historical claims, added feature-by-feature audit; no cloud connection or mutation; new 29-model graph parses only |
