{{ config(materialized='table') }}

select
    destination_name,
    count(*) as sync_run_count,
    sum(attempted_count) as attempted_rows,
    sum(success_count) as successful_rows,
    sum(failed_count) as failed_rows,
    sum(inserted_count) as inserted_rows,
    sum(updated_count) as updated_rows,
    sum(skipped_count) as skipped_unchanged_rows,
    sum(retry_count) as retry_count,
    sum(rate_limit_events) as rate_limit_events,
    max(ended_at) as last_sync_completed_at,
    case
        when sum(failed_count) > 0 then 'degraded'
        when sum(rate_limit_events) > 0 then 'rate_limited'
        else 'healthy'
    end as sync_health_status,
    current_timestamp as calculated_at
from {{ source('activation', 'reverse_etl_sync_run_log') }}
group by 1
