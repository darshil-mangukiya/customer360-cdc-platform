# Identity Merge Anomaly

## Symptoms

- Merge events spike above baseline.
- A canonical customer suddenly has many unrelated source records.
- Duplicate canonical customers appear after a source load.

## Detection Query

```sql
select tenant_id, date_trunc('hour', occurred_at) as hour, count(*)
from identity.identity_merge_event
group by 1, 2
order by 2 desc;
```

## Likely Root Causes

- Bad source identifier reused across customers.
- Device ID or weak source reference over-linked records.
- Source system emitted malformed email/phone values.

## Immediate Actions

- Pause identity writes for affected tenant if critical.
- Inspect `identity_link_explanation`.
- Compare match rules before and after the spike.

## Recovery Steps

- Disable weak matching rule temporarily.
- Re-run identity resolution from prior safe raw window.
- Document candidate split/unmerge records.

## Prevention

- Maintain source priority matrix.
- Track merge anomaly rate.
- Require human review for low-confidence rules in production.
