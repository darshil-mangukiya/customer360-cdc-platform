-- Snowflake bootstrap: 02_warehouse.sql
-- A single small, auto-suspending virtual warehouse fits the configured development
-- workloads (thousands-to-low-millions of rows per run, per config/load_profiles.yml).
-- Auto-suspend/auto-resume keep idle credit burn near zero, which matters for a
-- development account.

use role sysadmin;

create warehouse if not exists c360_wh
warehouse_size = 'XSMALL'
auto_suspend = 60            -- seconds idle before suspend
auto_resume = true
initially_suspended = true
comment = 'Customer 360 CDC platform - dbt build/query warehouse';

grant usage on warehouse c360_wh to role c360_transformer;
grant usage on warehouse c360_wh to role c360_analyst;
grant usage on warehouse c360_wh to role c360_activator;
grant usage on warehouse c360_wh to role c360_steward;
grant usage on warehouse c360_wh to role c360_readonly;
grant operate on warehouse c360_wh to role c360_transformer;

-- Optional: a resource monitor caps runaway spend during development. Adjust the
-- credit_quota to match your account's budget before enabling in a shared account.
-- create resource monitor if not exists c360_wh_monitor
--     with credit_quota = 25
--     frequency = monthly
--     triggers on 80 percent do notify
--              on 100 percent do suspend;
-- alter warehouse c360_wh set resource_monitor = c360_wh_monitor;
