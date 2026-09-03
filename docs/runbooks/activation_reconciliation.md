# Activation Reconciliation Runbook

Run `make reconcile` and inspect both CSVs under `reverse_etl/reconciliation/`. Repair
missing eligible rows by regenerating the affected export; stop sync immediately for
unexpected or privacy-suppressed rows. Preserve destination idempotency keys and retry
only missing dispositions. Rerun `make sync`, `make reconcile`, and `make scorecard`.
Close only when eligible equals success + failure + suppression + skip + duplicate.
