-- Snowflake bootstrap: 01_roles.sql
-- Least-privilege role design for the Customer 360 CDC platform.
--
-- Roles:
--   C360_LOADER        - lands raw CDC events into RAW.* only (write-only on raw layer)
--   C360_TRANSFORMER   - the dbt run/build role; owns STAGING/INTERMEDIATE/ANALYTICS/
--                        ACTIVATION/GOVERNANCE/OBSERVABILITY schemas
--   C360_ANALYST       - read-only role for downstream consumers (BI, reverse ETL readers)
--   C360_ADMIN         - schema/warehouse/database owner used only for bootstrap/DDL
--
-- Run this as a role with SECURITYADMIN / USERADMIN privileges. No passwords or account
-- identifiers are set here; provision users and credentials separately via your
-- identity provider / Snowflake user management, and wire them into SNOWFLAKE_USER /
-- SNOWFLAKE_PASSWORD (or key-pair auth) in the environment, never in source control.

use role securityadmin;

create role if not exists c360_admin;
create role if not exists c360_loader;
create role if not exists c360_transformer;
create role if not exists c360_analyst;
create role if not exists c360_activator;
create role if not exists c360_steward;
create role if not exists c360_readonly;

-- Role hierarchy: admin can assume the others for troubleshooting; SYSADMIN owns the
-- warehouse/database objects created in 02/03 so they show up in the standard object
-- ownership tree.
grant role c360_loader to role c360_admin;
grant role c360_transformer to role c360_admin;
grant role c360_analyst to role c360_admin;
grant role c360_activator to role c360_admin;
grant role c360_steward to role c360_admin;
grant role c360_readonly to role c360_admin;
grant role c360_admin to role sysadmin;

-- Attach service/human users to these roles as they are provisioned, e.g.:
--   grant role c360_transformer to user <dbt_service_user>;
--   grant role c360_analyst to user <bi_reader_user>;
