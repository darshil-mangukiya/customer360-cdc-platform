# Technology evidence matrix

| Technology/capability | Implemented | Local executed now | Recorded cloud executed | Currently revalidated | Evidence | Resume-safe qualifier |
| --- | --- | --- | --- | --- | --- | --- |
| Python / SQL | yes | yes | n/a | yes | 132 Python tests; dbt build | fully safe |
| PostgreSQL WAL / logical replication | yes | yes | n/a | yes | runtime manifest and connector logs | fully safe, bounded local runtime |
| Debezium / Kafka Connect / Kafka / CDC | yes | yes | n/a | yes | 133-row snapshot; 152-event run | fully safe, local implementation |
| JSON contracts / DLQ / replay / ordering | yes | yes | n/a | yes | contract gate, replay evidence, LSN trace | fully safe |
| dbt / snapshots / SCD Type 2 | yes | yes | recorded Snowflake run | PostgreSQL build and Snowflake parse | 29 models, 2 snapshots, 62 tests | fully safe locally; qualify Snowflake counts |
| Customer 360 / identity resolution / MDM pattern | yes | yes | n/a | yes | 133 identity links, 13 bounded golden customers | safe as portfolio implementation, not enterprise adoption |
| Stewardship / survivorship / provenance | yes | yes | n/a | yes | 8 modeled cases and provenance CSV | qualify stewardship as modeled |
| Privacy / consent / deletion suppression | yes | yes | prior suppression parity | yes | dbt eligibility plus Python fail-closed gate | portfolio privacy-control model, not legal certification |
| Activation outputs | yes | yes | n/a | yes | 6 exports and reconciliation | fully safe as activation-ready outputs |
| Reverse ETL | pattern only | simulated | no | yes | 4 destination simulators | qualified: reverse-ETL-style simulation |
| Snowflake | yes | no live connection now | yes, 2026-08-22 | evidence audit and parse | Snowflake evidence status | qualified: recorded live validation |
| Streams / Tasks / Dynamic Tables | yes | n/a | yes, 2026-08-22 | evidence audit | Snowflake validation matrix | qualified: recorded live validation |
| RBAC / row access / masking / tags | yes | PostgreSQL controls locally | yes, 2026-08-22 | evidence audit | Snowflake validation matrix | qualified by controlled validation |
| Airflow | yes | yes | n/a | 20/20 current | bounded run and failure/recovery | fully safe, local runtime |
| FastAPI | yes | TestClient smoke and auth tests | n/a | 36 routes | API smoke report | fully safe, local API |
| Docker Compose | yes | 7 services executed; 0 running after final freeze | n/a | config/runtime | docker-compose.yml | fully safe, stopped local stack with persistent state retained |
| Terraform | blueprint | no | no | configuration tests only | infra/terraform | qualified: blueprint only |
