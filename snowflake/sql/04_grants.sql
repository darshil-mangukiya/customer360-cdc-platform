-- Snowflake bootstrap: 04_grants.sql
-- Least-privilege grants for the roles created in 01_roles.sql.

use role sysadmin;
use database c360;

-- C360_LOADER: write-only into RAW, no visibility into anything else. Mirrors the
-- Postgres `raw_loader` role in warehouse/sql/06_rbac_security.sql.
grant usage on database c360 to role c360_loader;
grant usage on schema c360.raw to role c360_loader;
grant create table on schema c360.raw to role c360_loader;
grant insert, select on future tables in schema c360.raw to role c360_loader;
grant insert, select on all tables in schema c360.raw to role c360_loader;

-- C360_TRANSFORMER: the dbt role. Owns build/DDL rights across everything dbt
-- materializes into, plus read access on RAW/IDENTITY/AUDIT/GOVERNANCE to build from.
grant usage on database c360 to role c360_transformer;
grant usage on schema c360.raw to role c360_transformer;
grant select on future tables in schema c360.raw to role c360_transformer;
grant select on all tables in schema c360.raw to role c360_transformer;

grant usage on schema c360.identity to role c360_transformer;
grant select on future tables in schema c360.identity to role c360_transformer;
grant select on all tables in schema c360.identity to role c360_transformer;

grant usage on schema c360.audit to role c360_transformer;
grant select on future tables in schema c360.audit to role c360_transformer;
grant select on all tables in schema c360.audit to role c360_transformer;

grant usage on schema c360.governance to role c360_transformer;
grant select on future tables in schema c360.governance to role c360_transformer;
grant select on all tables in schema c360.governance to role c360_transformer;

grant usage, create schema on database c360 to role c360_transformer;
grant all on schema c360.staging to role c360_transformer;
grant all on schema c360.intermediate to role c360_transformer;
grant all on schema c360.analytics to role c360_transformer;
grant all on schema c360.activation to role c360_transformer;
grant all on schema c360.observability to role c360_transformer;
grant all on schema c360.marts to role c360_transformer;
grant all on schema c360.history to role c360_transformer;
grant all on schema c360.operations to role c360_transformer;

-- C360_ANALYST: read-only over the analytics-facing layers. No access to RAW
-- (may contain unmasked PII before the privacy gate) or GOVERNANCE.
grant usage on database c360 to role c360_analyst;
grant usage on schema c360.analytics to role c360_analyst;
grant select on future tables in schema c360.analytics to role c360_analyst;
grant select on future views in schema c360.analytics to role c360_analyst;
grant select on all tables in schema c360.analytics to role c360_analyst;
grant select on all views in schema c360.analytics to role c360_analyst;
grant usage on schema c360.activation to role c360_analyst;
grant select on future tables in schema c360.activation to role c360_analyst;
grant select on all tables in schema c360.activation to role c360_analyst;

-- Activation can read only approved activation products, while stewardship can
-- inspect governed identity evidence. Standard read-only consumers receive marts.
grant usage on database c360 to role c360_activator;
grant usage on schema c360.activation to role c360_activator;
grant select on future tables in schema c360.activation to role c360_activator;
grant select on all tables in schema c360.activation to role c360_activator;

grant usage on database c360 to role c360_steward;
grant usage on schema c360.identity to role c360_steward;
grant usage on schema c360.governance to role c360_steward;
grant select, insert, update on future tables in schema c360.governance to role c360_steward;
grant select on future tables in schema c360.identity to role c360_steward;

grant usage on database c360 to role c360_readonly;
grant usage on schema c360.marts to role c360_readonly;
grant select on future tables in schema c360.marts to role c360_readonly;

-- Warehouse usage (see 02_warehouse.sql) already granted to transformer/analyst.
