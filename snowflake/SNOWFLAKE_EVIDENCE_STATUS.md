# Snowflake evidence status

Audit date: 2026-09-03. No Snowflake connection or mutation was performed during P7 2.0. The statements below are retained recorded evidence from the 2026-08-22 controlled run unless marked configuration-only.

| Feature | Config | Recorded execution | Latest date | Read-only revalidated now | Safe claim | Limitation |
| --- | --- | --- | --- | --- | --- | --- |
| dbt on Snowflake | yes | 28 models, 2 snapshots, 56 tests passed | 2026-08-22 | no credentials used | recorded live validation | P7 2.0 adds a 29th privacy model; only parse was revalidated |
| Stream | `07_stream_task_cdc.sql` | change capture and consumption passed | 2026-08-22 | no | recorded live validation | distinct from Debezium/Kafka CDC |
| Triggered Task | `07_stream_task_cdc.sql` | dependency/trigger behavior passed | 2026-08-22 | no | recorded live validation | left suspended |
| Dynamic Table | `08_dynamic_table.sql` | incremental refresh passed | 2026-08-22 | no | recorded live validation | left suspended; 15-minute target lag in recorded run |
| RBAC and grants | `01_roles.sql`, `04_grants.sql` | seven-role hierarchy tested | 2026-08-22 | no | recorded live validation | not enterprise adoption/certification |
| Row access | `09_governance_policies.sql` | tenant restriction tested | 2026-08-22 | no | recorded live validation | controlled fixture |
| Masking | `09_governance_policies.sql` | analyst masked/steward visible | 2026-08-22 | no | recorded live validation | controlled fixture |
| Tags | `09_governance_policies.sql` | four tags and attachments checked | 2026-08-22 | no | recorded live validation | project classification only |
| Resource monitor | `02_warehouse.sql`, `10_cost_observability.sql` | monitor attached; query history recorded | 2026-08-22 | no | recorded live validation | no cost-savings claim |
| PostgreSQL parity | migration artifacts | 7 datasets, zero missing/extra/mismatched | 2026-08-22 | report re-audited only | recorded zero-mismatch parity validation | not current live parity |
| Kafka to Snowflake connector | example config | not run | n/a | no | configuration-only | plugin and key-pair runtime absent |

Responsibility boundary: PostgreSQL is the primary local execution plane. Snowflake is a separately validated analytical/cloud plane. Snowflake Streams track table changes inside Snowflake; Tasks schedule Snowflake SQL; Dynamic Tables refresh derived state. None replaces PostgreSQL WAL, Debezium, Kafka Connect, Kafka, or Airflow.
