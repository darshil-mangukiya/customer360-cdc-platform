# Reverse ETL Sync Failure

## Symptoms

- Destination sync run status is `partial_success` or `failed`.
- Failed records contain validation errors, 429s, or transient 5xx responses.
- Destination state is not updating.

## Detection Query

```sql
select destination_name, sync_status, failed_count, retry_count, ended_at
from activation.reverse_etl_sync_run_log
where ended_at >= now() - interval '24 hours'
order by ended_at desc;
```

## Likely Root Causes

- Missing destination identifier.
- Destination API rate limit.
- Contract field missing from export.
- Destination outage.

## Immediate Actions

- Inspect `reverse_etl_failed_records`.
- Separate retryable from non-retryable failures.
- Pause only affected destination if possible.

## Recovery Steps

- Retry transient failures.
- Fix missing identifier or contract issue.
- Re-run sync after destination recovers.

## Prevention

- Enforce payload contracts before sync.
- Use idempotency keys and payload hashes.
- Add destination-specific rate limiting.
