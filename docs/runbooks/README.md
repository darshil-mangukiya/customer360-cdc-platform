# Runbooks

Operational incident runbooks for the Customer 360 CDC and activation platform.

| Runbook | Scenario |
| --- | --- |
| `schema_drift_incident.md` | breaking CDC payload change |
| `kafka_lag_incident.md` | consumer lag or delayed source ingestion |
| `identity_merge_anomaly.md` | unexpected spike in identity merges |
| `reverse_etl_sync_failure.md` | downstream destination failures |
| `privacy_suppression_failure.md` | suppressed customer appears in export |
| `cdc_replay.md` | scoped replay and idempotency verification |
| `activation_reconciliation.md` | disposition variance and recovery |
| `customer_360_freshness.md` | stale model/output recovery |
| `tenant_isolation.md` | cross-tenant incident containment |
