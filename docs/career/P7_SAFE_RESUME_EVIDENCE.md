# Safe resume evidence

## Bullet candidates

- Built a six-domain synthetic Customer 360 pipeline using PostgreSQL WAL, Debezium, Kafka Connect, and Kafka; executed a 133-row initial snapshot and a 152-event incremental CDC run with zero rejected events after source-contract alignment.
- Implemented normalized CDC with before/after values, transaction IDs, PostgreSQL LSNs, Kafka coordinates, event hashes, per-key ordering, bounded replay, three-way deduplication, and reason-coded DLQ handling.
- Developed tenant-scoped deterministic identity resolution linking 133 source records into 13 bounded golden customers, with review-band stewardship, false-merge protection, survivorship, and masked field-level provenance.
- Built and ran a 29-model dbt Customer 360 graph with 2 snapshots and 62 tests, including SCD2 non-overlap controls and a fail-closed privacy eligibility mart used by all six activation exports.
- Implemented six privacy-filtered activation outputs and four destination simulators with idempotency, bounded retries, permanent-failure accounting, replay, and zero-variance candidate reconciliation.
- Executed a 20-task Airflow LocalExecutor run in 22.224 seconds and validated breaking-contract failure/recovery; exposed 36 tenant-scoped FastAPI routes with authorization tests.
- Recorded live Snowflake validation for dbt, Streams, Tasks, Dynamic Tables, RBAC, row access, masking, tags, and resource controls, with seven PostgreSQL/Snowflake parity datasets showing zero recorded mismatches.

## Keyword safety

Fully safe: CDC, Debezium, Kafka Connect, Kafka, PostgreSQL WAL, Customer 360, identity resolution, golden records, survivorship, dbt, dbt snapshots, SCD Type 2, Airflow, FastAPI, activation pipelines.

Qualified: MDM (portfolio implementation), stewardship (modeled cases), Snowflake/Streams/Tasks/Dynamic Tables/RBAC/masking/row access (recorded controlled validation), reverse ETL (reverse-ETL-style destination simulation), Terraform (blueprint/configuration only).

Not safe: production Customer 360, real customers/PII, GDPR/CCPA compliance, enterprise MDM adoption, production Snowflake workload, continuous production Snowflake replication, real SaaS activation, production reverse ETL, enterprise stewardship team, production SLA, realized lift, Terraform-deployed infrastructure.
