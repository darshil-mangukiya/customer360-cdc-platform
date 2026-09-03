# Snowflake Warehouse Target

Snowflake is a **first-class second warehouse target** alongside PostgreSQL. The same
dbt model graph runs on both adapters, with PostgreSQL as the default local target.

```text
                    +--> PostgreSQL (dev, default) --> dbt --target dev
CDC / Raw Landing --|
                    +--> Snowflake (cloud target)  --> dbt --target snowflake
                                                            |
                                                            v
                                                       Customer 360
                                                            |
                                                            v
                                                    Activation Outputs
```

## What's actually implemented vs documented

| Capability | Status |
|---|---|
| `dbt/profiles.yml` `snowflake` output, env-var credentials | **VERIFIED** — live connection succeeded without storing secrets |
| Cross-database dbt models | **VERIFIED** — same tree passed PostgreSQL and Snowflake builds/tests |
| `dbt parse` / `compile` / `run` / `snapshot` / `test` | **VERIFIED** — 28/28 models, 2/2 snapshots, 56/56 tests |
| Role/warehouse/database/schema/grant bootstrap SQL | **VERIFIED** — all ten ordered SQL files executed |
| Source objects and seed-42 fixture | **VERIFIED** — 151 CDC rows, 12 canonical customers, 132 mappings |
| Kafka → Snowflake sink properties | **CONFIGURATION-ONLY** — environment-backed template; not registered |
| Stream + triggered normalization Task | **VERIFIED** — duplicate, replay, order, and logical-delete behavior executed; Task left suspended |
| One current-state Dynamic Table | **VERIFIED** — incremental refresh, 15-minute lag, 133 rows |
| Row access, masking, tags, and role-to-tenant map | **VERIFIED** — strict analyst/steward/activator tests passed |
| Resource monitor and query-cost view | **VERIFIED** — monitor attached; query IDs/bytes/timing and hourly credits captured |
| PostgreSQL parity | **VERIFIED** — zero missing, extra, or mismatched values across seven comparisons: six modeled warehouse/output datasets plus one suppression-output comparison |
| Cortex | **ACCOUNT-LIMITED** — Snowflake error 399258: `COMPLETE` unavailable on trial accounts |

## Schema layout

| Schema | Populated by | Postgres equivalent |
|---|---|---|
| `RAW` (transient) | Python ingestion (`ingestion/loader.py`) | `raw` |
| `IDENTITY` | Python identity resolution (`identity_resolution/resolver.py`) | `identity` |
| `AUDIT` | Python ingestion/checkpointing | `audit` |
| `GOVERNANCE` | Python privacy module + identity stewardship review queue | `privacy` |
| `ANALYTICS_staging` / `ANALYTICS_intermediate` / `ANALYTICS_mart` / `ANALYTICS_activation` | dbt (auto-created) | `staging` / `intermediate` / `mart` / `activation` |
| `OBSERVABILITY` | Python observability module | `observability` |

dbt's default `generate_schema_name` macro (unmodified — behaves identically on both
adapters) writes model output to `<profile schema>_<custom schema>`. With
`SNOWFLAKE_SCHEMA=ANALYTICS` that gives `ANALYTICS_staging`, `ANALYTICS_mart`, etc. —
the same pattern the Postgres target already uses (`mart_staging`, `mart_mart`, ...).

## Setup

1. Provision a Snowflake account/trial, then run the bootstrap scripts in order as a
   role with `SECURITYADMIN`/`SYSADMIN` privileges:
   ```bash
   snowsql -a <account> -u <admin_user> -f snowflake/sql/01_roles.sql
   snowsql -a <account> -u <admin_user> -f snowflake/sql/02_warehouse.sql
   snowsql -a <account> -u <admin_user> -f snowflake/sql/03_database_schemas.sql
   snowsql -a <account> -u <admin_user> -f snowflake/sql/04_grants.sql
   snowsql -a <account> -u <admin_user> -f snowflake/sql/05_audit_objects.sql
   snowsql -a <account> -u <admin_user> -f snowflake/sql/06_external_source_contracts.sql
   snowsql -a <account> -u <admin_user> -f snowflake/sql/07_stream_task_cdc.sql
   snowsql -a <account> -u <admin_user> -f snowflake/sql/08_dynamic_table.sql
   snowsql -a <account> -u <admin_user> -f snowflake/sql/09_governance_policies.sql
   snowsql -a <account> -u <admin_user> -f snowflake/sql/10_cost_observability.sql
   ```
2. Create (or reuse) a service user for dbt, grant it `C360_TRANSFORMER`, and export
   credentials as environment variables (see `.env.example`) — never commit them:
   ```bash
   export SNOWFLAKE_ACCOUNT=xy12345.us-east-1
   export SNOWFLAKE_USER=dbt_service_user
   export SNOWFLAKE_PASSWORD=...          # or configure key-pair auth in profiles.yml
   export SNOWFLAKE_ROLE=C360_TRANSFORMER
   export SNOWFLAKE_DATABASE=C360
   export SNOWFLAKE_WAREHOUSE=C360_WH
   export SNOWFLAKE_SCHEMA=ANALYTICS
   ```
3. Install the adapter (already listed in `requirements-dev.txt`):
   ```bash
   python3 -m pip install -r requirements-dev.txt
   ```
4. Run dbt against the new target:
   ```bash
   cd dbt
   dbt debug --profiles-dir . --target snowflake
   dbt parse --profiles-dir . --target snowflake
   dbt run   --profiles-dir . --target snowflake   # requires a live account
   dbt test  --profiles-dir . --target snowflake   # requires a live account
   ```

## Live verification status

```bash
cd dbt
dbt parse --profiles-dir . --target snowflake     # resolves profile + adapter, renders Jinja/YAML
```
The 2026-08-22 live run connected to the trial, executed all bootstrap files, loaded
the shared seed-42 fixture, and ran the full graph:

```bash
dbt compile --target snowflake
dbt run --target snowflake
dbt snapshot --target snowflake
dbt test --target snowflake
```

Final results were 28/28 models, 2/2 snapshots, and 56/56 tests. See
`docs/proof/snowflake_validation_matrix.md`, `migration/MIGRATION_PARITY_REPORT.md`,
and `docs/COST_AND_PERFORMANCE_REPORT.md` for live query evidence.

## Least-privilege role design

- `C360_LOADER` — write-only into `RAW`. Mirrors the Postgres `raw_loader` role.
- `C360_TRANSFORMER` — the dbt role; DDL rights on the layers dbt builds, read-only on
  `RAW`/`IDENTITY`/`AUDIT`/`GOVERNANCE`.
- `C360_ANALYST` — read-only on `ANALYTICS`/`ACTIVATION`. No access to `RAW` (may hold
  unmasked PII before the privacy gate) or `GOVERNANCE`.

## Runtime scope

- Kafka → Snowflake is **CONFIGURATION-ONLY**. The current Kafka Connect worker needs
  the Snowflake connector plugin and key-pair environment variables before execution.
- `07`–`10` use native Snowflake statements that SQLFluff's current Snowflake parser
  cannot parse and are listed in `.sqlfluffignore`; those statements were executed
  during live validation.
- The Task is **SUSPENDED** after validation. Resume it only for an ingestion run.
- Cortex is **ACCOUNT-LIMITED** on the trial. Activation uses local adapters.
- PostgreSQL-only operational tables stay on PostgreSQL; dbt owns the modeled
  cross-warehouse relations.
