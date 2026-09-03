# Data Quality and Observability

## Validation Checks

Custom validation checks cover:

- duplicate canonical customers
- broken identity mappings
- missing source keys
- missing CDC operation coverage
- invalid subscription states
- stale activation outputs

CDC payload contracts also validate required fields, primitive types, nullable fields, and accepted source values before raw landing. Contract failures are written to the rejected events path.

Warehouse-backed dbt tests cover these checks when the Docker/Postgres profile is running:

- unique and non-null CDC event IDs
- accepted CDC operations
- accepted subscription states
- unique order and history keys
- relationships from facts to canonical customers
- activation export uniqueness
- exactly one current history record, non-overlapping and valid windows, and deleted
  records never marked current

The verified P7 2.0 PostgreSQL warehouse run executes 62 dbt data tests across 29
models and two snapshots. The earlier Snowflake evidence remains explicitly dated
because that recorded run covered the prior 28-model, 56-test graph.

## Observability Tables

- `observability.validation_failures`
- `observability.quality_summary`
- `observability.pipeline_run_log`
- `observability.freshness_status`
- `observability.pipeline_alert`
- `observability/output/lineage_edges.json`
- `validation/output/quality_scorecard.json`

The Python pipeline can persist validation and observability results to Postgres through `--dsn`, and the full Postgres runner does this by default.

## Reproducible scorecard

The scorecard covers CDC, identity, history, Customer 360, privacy, activation,
freshness, and reconciliation. Each domain score is `passed_checks / total_checks`.
The overall score is the sum of domain score multiplied by fixed weights (15, 15, 15,
10, 15, 15, 5, and 10 percent). Missing evidence scores zero; no arbitrary score is
inserted. The current fixture scores 95% because its historical timestamps are stale
relative to the wall clock, which is explicitly detected rather than hidden.

## Monitored Risks

- CDC lag
- rejected event rates
- missing source loads
- stale activation exports
- orphan identity mappings
- unexpected row count changes
- identity merge anomalies
- pipeline success/failure trend
