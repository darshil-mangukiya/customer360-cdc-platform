# CDC interview guide

- **What is captured?** Six PostgreSQL tables: accounts, subscriptions, orders, product engagement, support, and marketing consent. The source manifest is authoritative.
- **Why Debezium?** It reads PostgreSQL logical replication/WAL and emits database changes with source metadata and before/after values through Kafka Connect.
- **Snapshot versus incremental?** The isolated proof connector emitted 133 `r` snapshot records. The continuous connector run accounted for 152 events: one earlier snapshot record and 151 incremental mutations.
- **LSN?** PostgreSQL's log sequence number. P7 stores it and now uses it as the normalized sequence for deterministic same-key ordering.
- **Kafka coordinates?** Topic, partition, and offset are persisted. Ordering is only claimed for a key in its partition, not globally.
- **Operations?** `r/c/u/d` map to insert/insert/update/delete. Updates retain before and after. Deletes retain before and null after; connector tombstones are disabled.
- **Duplicates?** Event ID, content hash, and Kafka position are independent defenses. A three-offset replay attempted the ordering chain twice but left three distinct persisted rows.
- **DLQ?** Contract failures become reason-coded rejected records. The live consumer now catches per-message mapper failures rather than crashing the whole batch.
- **Replay?** Bounded by named group, topic, partition, and record count. The proof moved one consumer group back exactly three offsets.
- **Schema evolution?** Optional additive fields pass; missing required fields, invalid enums, and incompatible types fail. Five current scenarios are recorded in `schema_drift_results.csv`.
- **Restart behavior?** Kafka Connect resumes from its offset storage and the consumer commits only after PostgreSQL persistence; the retained restart proof captured an event committed while Connect was down.
- **System boundaries?** Kafka transports continuous changes; the consumer normalizes and persists; dbt models current/history; Airflow coordinates batch gates and reconciliation.
