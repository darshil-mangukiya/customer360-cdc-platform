# Snowflake Validation Matrix

Evidence date: 2026-08-22. Validation used the controlled seed-42 workload. Connection
settings were supplied through the process environment.

| Capability | Result | Status | Evidence / limitation |
| --- | --- | --- | --- |
| Snowflake connectivity | PASS | VERIFIED | `dbt debug` completed with dbt 1.11.11 / adapter 1.11.6 |
| Bootstrap | PASS | EXECUTED | all ten ordered SQL files executed |
| Database / schemas | PASS | VERIFIED | `C360` and 11 prepared schemas returned by live metadata |
| Warehouse | PASS | VERIFIED | `C360_WH`, X-Small, auto-suspend 60, auto-resume; final state suspended |
| Roles / grants | PASS | VERIFIED | seven-role hierarchy plus strict secondary-role tests |
| Resource monitor | PASS | VERIFIED | `C360_MONTHLY_GUARDRAIL` attached to `C360_WH` |
| Fixture load | PASS | VERIFIED | 151 landed CDC events, 4 tenants, 6 source systems, 6 domains; 151 LSNs and offsets |
| dbt parse | PASS | EXECUTED | 28 models, 2 snapshots, 56 tests, 41 sources |
| dbt compile | PASS | EXECUTED | live compilation completed |
| dbt run | PASS | VERIFIED | 28/28 models; final full refresh 15.84 seconds |
| dbt snapshot | PASS | VERIFIED | 2/2; 12 current customer and 12 current subscription rows |
| dbt test | PASS | VERIFIED | 56/56 in 5.30 seconds |
| Customer 360 | PASS | VERIFIED | 12 customers across 4 tenants; health 55–94 |
| PostgreSQL parity | PASS | VERIFIED | zero missing/extra/mismatched values across 7 comparisons: 6 modeled warehouse/output datasets plus 1 suppression output |
| Kafka -> Snowflake | NOT RUN | CONFIGURATION-ONLY | Connect worker lacks Snowflake plugin and required key-pair environment variables |
| Stream | PASS | VERIFIED | live change Stream consumed fixture and controlled test events |
| Triggered Task | PASS | VERIFIED | fresh triggers ran; duplicate/replay/order/delete behaviors verified; left suspended |
| Dynamic Table | PASS | VERIFIED | incremental refresh, 15-minute target lag, 133 live rows, 192-second observed freshness |
| Row Access Policy | PASS | VERIFIED | analyst restricted to 8 `tenant_us` rows; steward saw 12 rows / 4 tenants |
| Masking policies | PASS | VERIFIED | analyst email/phone masked; steward direct values visible |
| Tags | PASS | VERIFIED | four tags created and live object/column attachments applied |
| Performance evidence | PASS | EXECUTED | four tagged X-Small queries with query IDs, elapsed times, and bytes scanned |
| Cost evidence | PASS | EXECUTED | metering returned 0.403779166 credits for the live-run hour; not attributed per query |
| Cortex | ACCOUNT-LIMITED | ACCOUNT-LIMITED | error 399258: `COMPLETE` unavailable for trial accounts |

Representative query IDs are recorded in the migration and cost reports. Query IDs
are operational evidence, not credentials.
