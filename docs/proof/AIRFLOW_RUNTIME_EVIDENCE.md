# Airflow Runtime Evidence

Evidence date: 2026-08-23 UTC. The runs used controlled local fixtures and the Docker
services listed below.

## Environment

| Component | Runtime |
|---|---|
| Airflow | Apache Airflow 2.10.4 container |
| Executor | `LocalExecutor` |
| Metadata database | Local PostgreSQL 16 container |
| Services | `postgres`, `airflow-init`, `airflow-scheduler`, `airflow-webserver` only |
| Web UI | `http://localhost:8080` |
| Project code | Bind-mounted at `/opt/customer-360-cdc-platform` |

The scheduler reported zero DAG import errors. The DAG ID is
`customer_360_cdc_platform`.

## DAG

- DAG ID: `customer_360_cdc_platform`
- Schedule: `@hourly`
- Catchup: disabled; manual logical-date runs are supported
- Maximum active runs: 1
- DAG timeout: 2 hours
- Tasks: 20, including `start` and `end`
- Default retries: 1 with a 30-second delay
- Fixture-readiness retries: 3 with a 10-second delay
- Failure handling: callback emits task/run identifiers; default `all_success`
  dependencies block all downstream stages after a failed gate

Kafka retains ownership of continuous CDC transport. Airflow owns batch/control-plane
coordination over existing project commands and contains no copied business logic.

## Task Groups

| Group | Tasks | Responsibility |
|---|---:|---|
| `source_readiness` | 2 | Generate deterministic fixture and verify readiness |
| `cdc_validation` | 3 | Contract gate, schema drift, raw landing |
| `identity_and_model_validation` | 3 | Identity, stewardship queue, PostgreSQL dbt parse |
| `quality` | 2 | Custom checks and expectation suite |
| `privacy_and_activation` | 4 | Privacy-filtered exports, privacy gate, simulated sync, reconciliation |
| `observability` | 4 | Metrics, lineage, scorecard, health report |

## Dependency Graph

```text
start
  -> source_readiness
  -> cdc_validation
  -> identity_and_model_validation
  -> quality
  -> privacy_and_activation
  -> observability
  -> end
```

Every task in this chain uses the default successful-upstream trigger rule. There are
no `ALL_DONE` escape paths that could activate or report success after a failed gate.

## Successful Run

**EXECUTED / VERIFIED**

| Field | Value |
|---|---|
| Run ID | `scheduled__2026-08-23T00:00:00+00:00` |
| Logical date | `2026-08-23T00:00:00+00:00` |
| Start | `2026-08-23T01:50:28.957136+00:00` |
| End | `2026-08-23T01:50:52.201932+00:00` |
| Duration | 23.245 seconds |
| Final state | `success` |
| Task result | 20/20 `success` |

The longest task was the offline PostgreSQL dbt parse at 3.604 seconds. Final logical
outputs were 152 generated events, 151 landed events, 12 canonical customers, 132
identity mappings, 2 suppressed customers, 10 unique activated customers, 16
reconciliation rows, 0 variance runs, and a 95.0% quality score.

## Failure / Blocking Test

**EXECUTED / VERIFIED**

| Field | Value |
|---|---|
| Run ID | `controlled_failure_20260823T0152Z` |
| Fixture | `airflow/fixtures/breaking_contract.json` |
| Start | `2026-08-23T01:52:18.151536+00:00` |
| End | `2026-08-23T01:53:05.325544+00:00` |
| Final state | `failed` |
| Failed task | `cdc_validation.contract_compatibility_gate` |
| Controlled finding | 7 breaking contract removals |

The source readiness tasks succeeded. The contract gate failed, retried, and failed
again. All 15 subsequent schema-drift, landing, identity, dbt, quality, privacy,
activation, reconciliation, observability, and terminal tasks were
`upstream_failed`; no activation or destination simulation ran.

## Retry Behavior

**EXECUTED / VERIFIED**

The contract task entered `up_for_retry` after its first controlled failure at
`01:52:21Z`, then made its second attempt after the configured 30-second delay at
`01:52:52Z` and reached final `failed`. The separate readiness task also retains a
meaningful three-retry/10-second configuration for transient fixture availability.

## Rerun / Idempotency

**EXECUTED / VERIFIED**

After restoring the normal contract path, run `recovery_idempotency_20260823T0153Z`
completed `success` from `01:53:41.421825Z` to `01:54:03.437489Z` (22.016 seconds),
with all 20 tasks successful. Regeneration preserved the same logical counts: 152
generated, 151 landed, 12 canonical, 132 mappings, 2 suppressed, 10 activated, and
16 reconciliations with zero variance. Files are deterministically replaced and
destination sync state uses stable customer/idempotency keys rather than appending
duplicate logical customers.

## Backfill / Logical-Date Behavior

**EXECUTED / VERIFIED**

Manual historical run `historical_manual_20260821T1200Z` used logical date
`2026-08-21T12:00:00+00:00` and completed all 20 tasks successfully from
`2026-08-23T01:54:43.699988Z` to `01:55:04.898928Z` (21.199 seconds). Automatic
catchup remains disabled to avoid accidental interval floods; historical execution
is an explicit manual control.

## Privacy Gate

**EXECUTED / VERIFIED**

`build_privacy_filtered_exports` applies the existing consent/deletion suppression
logic. `verify_privacy_gate` then independently checks the generated suppression list
against every activation export before destination simulation can begin. Runtime
evidence found 2 suppressed customers, 10 unique exported customers across 6 export
files, and 0 overlaps. The focused negative test also proves the verifier fails when
a suppressed ID is injected into an export.

## Runtime Metrics

| Scenario | Duration | Final state |
|---|---:|---|
| Scheduled successful run | 23.245 s | `success` |
| Controlled failure including retry | 47.174 s | `failed` |
| Recovery/idempotency run | 22.016 s | `success` |
| Historical logical-date run | 21.199 s | `success` |

These timings describe the controlled local orchestration runs above.

## Runtime scope

- The DAG performs dbt parse, not a warehouse build; existing PostgreSQL and
  Snowflake dbt execution evidence remains separate.
- Destination syncs use local adapters.
- Continuous Kafka CDC remains outside Airflow by design.
- The recorded webserver and scheduler use LocalExecutor and local UI authentication.

## Evidence Classification

| Capability | Classification |
|---|---|
| DAG import and dependency graph | VERIFIED |
| Local scheduler/webserver runtime | EXECUTED |
| Successful and historical runs | EXECUTED / VERIFIED |
| Failure blocking and retry | EXECUTED / VERIFIED |
| Idempotent rerun and privacy gate | VERIFIED |
