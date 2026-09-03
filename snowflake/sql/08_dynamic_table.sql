-- Live-verified 2026-08-22. One Dynamic Table is justified for low-latency current-state
-- noqa: disable=all -- Current SQLFluff Snowflake parsing does not handle these Dynamic Table options cleanly.
-- serving; history and complex business transformations remain dbt-owned.
use role sysadmin;
use database c360;

create or replace dynamic table marts.customer_cdc_current
    target_lag = '15 minutes'
    warehouse = c360_wh
    refresh_mode = incremental
as
select
    tenant_id,
    source_system,
    source_table,
    record_primary_key,
    event_id,
    payload,
    source_commit_timestamp,
    source_lsn,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    normalized_at
from staging.normalized_cdc_events
where not is_deleted;
