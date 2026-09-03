# CDC Design

The CDC layer is designed to preserve source-system intent and support replay-safe, idempotent loading.

## Envelope Fields

Each normalized event includes:

- `event_id`
- `tenant_id`
- `source_system`
- `source_table`
- `operation_type`
- `event_timestamp`
- `record_primary_key`
- `payload_before`
- `payload_after`
- `batch_id`
- `schema_version`
- `event_sequence_number`
- `source_transaction_id`
- `source_lsn`
- `source_commit_timestamp`
- `ingestion_timestamp`
- `kafka_topic`
- `kafka_partition`
- `kafka_offset`
- `event_hash`
- `replay_batch_id`
- `is_replay`

## Contract and quarantine policy

`contracts/cdc_payload_contracts.json` is the payload source of truth. The normalized
Python envelope adds operation, tenant, timestamp, source-key, and supported-version
semantics. The compatibility engine labels required removals/type changes as breaking,
nullable additions as compatible, and consumer-sensitive enum additions as warnings.

Rejected rows retain an event reference plus failure stage, stable category, error
detail, retry eligibility, and rejection time. Categories include `SCHEMA_INVALID`,
`UNKNOWN_OPERATION`, `MISSING_SOURCE_KEY`, `INVALID_TENANT`,
`UNSUPPORTED_SCHEMA_VERSION`, `INVALID_PAYLOAD`, and `DUPLICATE_EVENT` in the dedupe
audit. Raw payloads remain in the local repair path; public proof uses references and
hashed identifiers rather than unnecessary PII.

## Idempotency Strategy

The loader checks event ID, event hash, and Kafka topic/partition/offset. Duplicate
delivery is logged with the matching key and filtered before raw state changes.
PostgreSQL also has uniqueness controls for replay-safe inserts.

## Ordering Strategy

Ordering is evaluated at the tenant/source-record level using:

- event sequence number
- source commit timestamp
- source LSN
- Kafka topic, partition, and offset

Out-of-order or late-arriving records are written to `cdc_ordering_anomalies.csv`.

## Replay Strategy

Replay selection can be performed by:

- timestamp range
- batch ID
- source table

Replay files preserve source events and add:

- `is_replay = true`
- `replay_batch_id`

Checkpoint resets are auditable through replay manifests and alert rows.

## Schema registry mapping

The JSON contract maps to a registry subject per source table and schema version.
Confluent, AWS Glue, or an Event Hubs-compatible registry can store and gate those
versions while Debezium/Kafka Connect publishes the schema identifier with each
message. Local execution keeps the compatibility gate file-backed.

## Debezium-Compatible Local CDC Proof

The repository includes a small technical proof path for the Debezium/Kafka Connect scaffold:

```bash
make cdc-demo
```

The proof validates:

- `connect/debezium/postgres_customer_sources.json`
- `source_systems/postgres/init/01_cdc_demo_changes.sql`
- insert/update/delete Debezium-style messages for `source_customers_cdc_demo`
- conversion from Debezium-style messages into the normalized CDC envelope

The generated report is written under ignored `reports/` runtime output.

## Executed infrastructure and fixture path

The fast smoke path still uses deterministic metadata and CSV/JSON audit artifacts.
Separately, the Docker integration was executed through PostgreSQL logical replication,
Debezium 2.7.3, Kafka Connect, and Kafka. Real create/update/delete records carried
partition 0, offsets 0–2, and PostgreSQL LSNs; a change made while Connect was stopped
resumed at offset 3 with no duplicate warehouse offsets. See
`docs/proof/real_kafka_debezium_execution.md`.

The recorded integration uses the local Docker services; registry and hosted connector
selection remain deployment concerns.
