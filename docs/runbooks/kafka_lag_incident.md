# Kafka Lag Incident

## Symptoms

- `freshness_status` is stale for one or more tenant/source pairs.
- Topic offset checkpoint stops advancing.
- Activation exports are missing recent customer changes.

## Detection Query

```sql
select tenant_id, entity_name, lag_minutes, status
from observability.freshness_status
where status <> 'fresh'
order by lag_minutes desc;
```

## Likely Root Causes

- Consumer is down or under-provisioned.
- Kafka Connect task failed.
- Source volume spiked.
- Network issue between Kafka and loader.

## Immediate Actions

- Check Kafka Connect task status.
- Check consumer logs and offset checkpoint.
- Stop activation sync if stale data could trigger harmful outreach.

## Recovery Steps

- Restart failed consumer/connector.
- Scale consumer concurrency.
- Continue from the last committed topic offset.
- Run replay for affected time window if needed.

## Prevention

- Alert on lag by tenant/source.
- Keep replayable raw event archive.
- Size partitions based on source throughput.
