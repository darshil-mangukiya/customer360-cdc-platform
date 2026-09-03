# Customer 360 Freshness Runbook

Run `make observe` and inspect `observability/output/freshness_status.csv`. Confirm the
source checkpoint advances, then rerun ingestion and `make postgres-pipeline`. Hold
activation while a required tenant/entity is stale. Verify freshness, history tests,
reconciliation, and the scorecard before resuming destination simulation.
