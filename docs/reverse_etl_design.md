# Reverse ETL Design

The reverse ETL layer turns warehouse customer intelligence into operational activation products.

## Activation Outputs

- `customer_segment_export.csv`
- `churn_risk_export.csv`
- `lifecycle_stage_export.csv`
- `customer_health_score_export.csv`
- `support_priority_export.csv`
- `campaign_target_export.csv`

Each output includes tenant context, canonical customer ID, hashed identifiers, export timestamps, refresh timestamps, and lineage references where useful.

## Privacy Gate

Exports are generated only after the privacy activation policy evaluates:

- marketing consent
- email/SMS/push opt-in
- unsubscribe status
- do-not-contact flag
- deletion request status
- destination identifier availability

Suppressed customers are written to `privacy/output/export_suppressed_customers.csv`.

## Destination Sync Lifecycle

The simulator models Salesforce, HubSpot, Braze, and Zendesk-style syncs:

1. Read activation export.
2. Validate destination contract.
3. Build destination payload.
4. Generate idempotency key and payload hash.
5. Compare against destination sync state.
6. Insert, update, skip unchanged, or fail.
7. Retry transient errors.
8. Write sync logs, failed rows, payload audit, and destination status.

## Idempotency

Idempotency is based on:

- destination name
- export file
- canonical customer ID
- payload hash

Unchanged payloads are skipped. Changed payloads are updated. Missing destination IDs or invalid required fields are rejected.

## Contracts

Destination contracts specify required fields per export. Contract failures are logged as failed records, not silently dropped.

## Reconciliation & UAT

Every sync is reconciled against the warehouse-eligible population, with source-to-target validation and a curated business-rule UAT suite. See `docs/activation_reconciliation.md` for the invariant, the validation checks, and requirements traceability. API: `GET /activation/reconciliation`, `GET /activation/runs`.
