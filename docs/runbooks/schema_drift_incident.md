# Schema Drift Incident

## Symptoms

- Contract gate fails.
- New rejected events appear for one source table.
- Required fields are missing or type checks fail.

## Detection Query

```sql
select source_table, rejection_reason, count(*)
from raw.rejected_events
where rejected_at >= now() - interval '2 hours'
group by 1, 2
order by count(*) desc;
```

## Likely Root Causes

- Source team removed or renamed a required field.
- New enum value was added without contract review.
- Debezium payload type changed after source migration.

## Immediate Actions

- Pause downstream activation for affected tenant/source if critical.
- Inspect rejected payload samples.
- Run `make contract-gate` and `make drift`.

## Recovery Steps

- Add compatible nullable fields or update staging logic.
- For breaking changes, coordinate source rollback or contract version bump.
- Reprocess repaired DLQ events with `make dlq`.

## Prevention

- Enforce contract review before operational schema changes.
- Add source-owner alert routing for contract failures.
