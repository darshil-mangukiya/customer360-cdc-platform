-- Live-verified 2026-08-22 with strict analyst/steward/activator role tests.
-- noqa: disable=all -- Current SQLFluff Snowflake parsing does not support this row access policy DDL.
use role sysadmin;
use database c360;

create table if not exists governance.role_tenant_access (
    role_name string not null,
    tenant_id string not null,
    primary key (role_name, tenant_id)
);

create or replace row access policy governance.tenant_access_policy as
(tenant_id string) returns boolean ->
    current_role() in ('C360_ADMIN', 'C360_STEWARD')
    or exists (
        select 1 from governance.role_tenant_access as access_map
        where access_map.role_name = current_role()
          and access_map.tenant_id = tenant_id
    );

create or replace masking policy governance.email_mask as
(value string) returns string ->
    case when current_role() in ('C360_ADMIN', 'C360_STEWARD', 'C360_ACTIVATOR')
        then value else regexp_replace(value, '(^.).*(@.*$)', '\\1***\\2') end;

create or replace masking policy governance.phone_mask as
(value string) returns string ->
    case when current_role() in ('C360_ADMIN', 'C360_STEWARD', 'C360_ACTIVATOR')
        then value else concat('***-***-', right(value, 4)) end;

create tag if not exists governance.data_classification
    allowed_values 'PII', 'SENSITIVE', 'INTERNAL', 'PUBLIC';
create tag if not exists governance.data_domain;
create tag if not exists governance.data_owner;
create tag if not exists governance.activation_eligibility
    allowed_values 'ELIGIBLE', 'PROHIBITED', 'CONDITIONAL';

-- Post-dbt attachment commands intentionally remain explicit and reviewable because
-- ANALYTICS_MART is created by dbt after the ordered bootstrap:
-- alter table analytics_mart.mart_customer_360_current add row access policy governance.tenant_access_policy on (tenant_id);
-- alter table analytics_mart.mart_customer_360_current modify column primary_email set masking policy governance.email_mask;
