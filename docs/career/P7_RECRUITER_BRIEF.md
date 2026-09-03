# P7 recruiter brief

## 20 seconds

P7 is a synthetic-data Customer 360 platform that turns PostgreSQL WAL changes into explainable golden records, SCD2 history, governed consent eligibility, six activation-ready exports, and fully reconciled simulator outcomes.

## 60 seconds

One PostgreSQL Debezium connector captures six operational domains through Kafka Connect and Kafka. The normalized envelope retains before/after payloads, transaction ID, LSN, topic, partition, offset, hashes, replay state, and source timestamps. Deterministic tenant-scoped identity resolution links 133 fragmented source records in the bounded proof while routing ambiguous evidence into a modeled stewardship queue. dbt builds 29 models, 2 snapshots, and 62 tests, including a fail-closed privacy eligibility mart used by all six exports. Four destination simulators exercise idempotency, retry, permanent failure, replay, and zero-variance reconciliation. Snowflake features have separate recorded live validation; no production or real-customer claim is made.

Best review path: `evidence/runtime/runtime_manifest.json` → `CDC_TRACEABILITY.csv` → `IDENTITY_RESOLUTION_MATRIX.csv` → dbt privacy model → activation reconciliation → Snowflake evidence status.
