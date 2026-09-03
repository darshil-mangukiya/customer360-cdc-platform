# Activation Reconciliation & UAT

Every reverse-ETL activation run reconciles the warehouse population, export, and
destination outcome. A focused set of UAT scenarios covers the modeled business
rules. Implementation:
`reverse_etl/reconciliation.py`; tests: `tests/test_activation_reconciliation.py`
(reconciliation math + drill-down findings) and `tests/test_activation_uat.py`
(business-rule UAT).

## The reconciliation invariant

For each (destination, tenant, export) reverse-ETL run:

```text
warehouse_eligible_count
=
successful_count + failed_count + suppressed_count + skipped_count + duplicate_count
```

`warehouse_eligible_count` is the *total* canonical customer population considered
for activation in that tenant. Suppression is a disposition rather than a pre-filter.
`duplicate_count` is separate from `skipped_count` because the destination adapter
distinguishes an idempotent re-sync of the same payload from other intentional skips.

When the equation doesn't balance, `status` is `variance_detected` and
`activation_reconciliation_findings.csv` / `GET /activation/runs` names exactly which
canonical customer IDs (or idempotency keys) are responsible — never just a count.

## Source-to-target validation checks

| Check | `finding_type` | Severity |
|---|---|---|
| Eligible customer missing from the export | `missing_eligible_row` | high |
| Export row not backed by any known canonical customer | `unexpected_exported_row` | high |
| Privacy-suppressed customer present in an export | `privacy_suppressed_row_exported` | critical |
| Same customer exported more than once | `duplicate_customer_export` | medium |
| Export row missing both `email_sha256` and `phone_sha256` | `missing_hashed_identifier` | medium |
| Export row's `tenant_id` doesn't match the customer's owning tenant | `wrong_tenant_row` | critical |
| Same `idempotency_key` used more than once in a destination sync | `duplicate_idempotency_key` | medium |

## UAT scenarios

Each case covers a distinct business or operational rule.

| Case ID | Rule | Test |
|---|---|---|
| ACT-UAT-001 | Opted-out customers never appear in any activation export | `test_act_uat_001_opted_out_customer_never_appears_in_any_activation_export` |
| ACT-UAT-002 | An approved deletion request makes a customer activation-ineligible | `test_act_uat_002_deletion_request_makes_customer_activation_ineligible` |
| ACT-UAT-003 | A low-CSAT open support case elevates support priority | `test_act_uat_003_low_csat_support_case_elevates_support_priority` |
| ACT-UAT-004 | A repeated destination sync of unchanged data doesn't create a duplicate successful update | `test_act_uat_004_repeated_sync_run_does_not_duplicate_a_successful_update` |
| ACT-UAT-005 | The same identifier in two tenants never merges identities or leaks activation across tenants | `test_act_uat_005_same_identifier_in_two_tenants_never_merges_or_leaks_activation` |

## Requirements traceability

ACT-UAT-001 traces one modeled rule end to end:

```text
Requirement:      Do not send opted-out customers to any activation destination.
Source:           marketing_engagement CDC events (marketing_consent_status,
                   unsubscribe_status, do_not_contact_flag)
Transformation:    privacy.activation_policy.build_consent_history /
                   suppression_reason
Activation Rule:   marketing_consent_status == "opted_out" ->
                   activation_suppression_reason = "marketing_consent_opted_out"
Export:            every export file (customer_segment, churn_risk, lifecycle_stage,
                   customer_health_score, support_priority, campaign_target)
Validation:        reverse_etl.reconciliation "privacy_suppressed_row_exported" check
                   — zero opted-out customers may appear in any export
UAT:               ACT-UAT-001
Acceptance:        PASS only when the reconciliation run for every destination shows
                   zero privacy_suppressed_row_exported findings
```

The same Requirement -> Source -> Transformation -> Activation Rule -> Export ->
Validation -> UAT chain applies to deletion requests (ACT-UAT-002, using
`privacy.deletion_workflow` + the `--deletion-requests-path` load in
`reverse_etl.exporter.main`) and to tenant isolation (ACT-UAT-005, enforced
structurally by tenant-scoped identifier nodes in `identity_resolution.resolver` and
checked again at the activation layer by the `wrong_tenant_row` reconciliation check).

## Destination coverage

- Reconciliation reads the local CSV outputs from each pipeline stage and the recorded
  per-row outcome from each local destination adapter.
- `lifecycle_stage_export.csv` and `customer_health_score_export.csv` are generated
  without a configured destination in
  `reverse_etl/destinations/simulator.py:DESTINATIONS`, so reconciliation contains the
  four exports with configured adapters.
