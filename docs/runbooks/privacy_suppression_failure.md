# Privacy Suppression Failure

## Symptoms

- Suppressed customer appears in campaign export.
- Deleted or do-not-contact customer appears in destination payload audit.
- Suppression list row count drops unexpectedly.

## Detection Query

```sql
select e.canonical_customer_id, s.activation_suppression_reason
from activation.export_campaign_target e
join privacy.export_suppressed_customers s using (canonical_customer_id);
```

## Likely Root Causes

- Export bypassed privacy policy.
- Consent CDC event arrived late and was not replayed.
- Deletion request was not propagated to suppression list.

## Immediate Actions

- Stop affected destination sync.
- Remove customer from downstream campaign manually if already synced.
- Create privacy audit row.

## Recovery Steps

- Rebuild consent history.
- Regenerate exports.
- Re-run sync after verifying suppressed customers are excluded.

## Prevention

- Make privacy gate mandatory before export materialization.
- Add CI test for suppressed customers excluded from exports.
