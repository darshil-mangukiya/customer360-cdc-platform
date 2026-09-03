# CDC Replay Runbook

1. Inspect `ingestion/output/cdc_watermark_checkpoint.csv`, topic-offset checkpoints,
   and ordering anomalies. Identify tenant, source table, and half-open time window.
2. Preview with `python3 -m ingestion.replay --start <ISO> --end <ISO>
   --source-table <table> --dry-run`.
3. Write a replay artifact with the same command minus `--dry-run`. Reset persisted
   checkpoints only for an approved recovery scope.
4. Run `make ingest`, `make identity`, `make exports`, `make reconcile`, and
   `make scorecard`. Confirm downstream grains remain unique.

Escalate cross-tenant mappings, overlapping history, privacy leakage, or variance.
