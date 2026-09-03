-- Live-verified 2026-08-22 cost guardrails and query observability views.
-- noqa: disable=all -- Current SQLFluff Snowflake parsing does not support this resource monitor DDL.
use role accountadmin;

create resource monitor if not exists c360_monthly_guardrail
    with credit_quota = 25
    frequency = monthly
    start_timestamp = immediately
    triggers on 75 percent do notify
             on 90 percent do suspend
             on 100 percent do suspend_immediate;

alter warehouse c360_wh set resource_monitor = c360_monthly_guardrail;

-- Warehouse-backed Tasks still require the global EXECUTE TASK privilege on their
-- owner role; without it a resumed triggered Task remains started but never runs.
grant execute task on account to role sysadmin;

use role sysadmin;
use database c360;
create or replace view operations.recent_query_cost as
select
    query_id,
    query_tag,
    warehouse_name,
    warehouse_size,
    total_elapsed_time,
    bytes_scanned,
    rows_produced,
    start_time,
    end_time
from snowflake.account_usage.query_history
where start_time >= dateadd('day', -7, current_timestamp())
  and query_tag like '%customer_360%';
