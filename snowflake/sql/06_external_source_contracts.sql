-- Snowflake bootstrap: minimal non-dbt source objects referenced by the shared model tree.
-- dbt owns staging/intermediate/mart/activation model relations; this file only creates
-- landing/control tables populated by ingestion, identity, and observability services.

use role sysadmin;
use database c360;

create table if not exists identity.dim_customer_canonical (
    canonical_customer_id string not null,
    tenant_id string not null,
    business_unit string,
    primary_email string,
    primary_phone string,
    primary_external_account_id string,
    first_name string, -- noqa: RF04
    last_name string, -- noqa: RF04
    source_record_count number not null,
    first_seen_at timestamp_tz,
    last_updated_at timestamp_tz,
    identity_confidence_score float,
    primary key (tenant_id, canonical_customer_id)
);

create table if not exists identity.customer_identity_map (
    tenant_id string not null,
    source_system string not null,
    source_record_id string not null,
    canonical_customer_id string not null,
    matched_on string,
    confidence_score float,
    linked_at timestamp_tz,
    primary key (tenant_id, source_system, source_record_id)
);

create table if not exists audit.ingestion_log (
    batch_id string not null,
    source_system string not null,
    source_table string not null,
    event_count number not null,
    landed_count number not null,
    rejected_count number not null,
    load_start_time timestamp_tz,
    load_end_time timestamp_tz,
    schema_version number,
    load_status string
);

create table if not exists observability.freshness_status (
    tenant_id string not null,
    entity_name string not null,
    max_event_timestamp timestamp_tz,
    observed_at timestamp_tz,
    lag_minutes number,
    status string
);

create table if not exists activation.reverse_etl_sync_run_log (
    sync_run_id string,
    tenant_id string,
    destination_name string,
    export_file string,
    destination_object string,
    attempted_count number,
    success_count number,
    failed_count number,
    inserted_count number,
    updated_count number,
    skipped_count number,
    retry_count number,
    rate_limit_events number,
    sync_status string,
    started_at timestamp_tz,
    ended_at timestamp_tz
);

-- Keep an already-bootstrapped account forward-compatible with the shared dbt model.
-- Snowflake's IF NOT EXISTS form makes this safe to rerun after the original offline
-- contract (which used run_id/completed_at and omitted rate-limit metadata).
alter table activation.reverse_etl_sync_run_log add column if not exists sync_run_id string;
alter table activation.reverse_etl_sync_run_log add column if not exists destination_object string;
alter table activation.reverse_etl_sync_run_log add column if not exists rate_limit_events number;
alter table activation.reverse_etl_sync_run_log add column if not exists ended_at timestamp_tz;

create table if not exists activation.export_suppressed_customers (
    canonical_customer_id string not null,
    tenant_id string,
    activation_suppression_reason string not null,
    suppressed_at timestamp_tz not null,
    primary key (canonical_customer_id, activation_suppression_reason)
);

-- Local PostgreSQL is the primary execution plane for deletion requests. This
-- empty-compatible contract keeps the shared dbt privacy model portable when a
-- controlled fixture is loaded into the separately validated Snowflake plane.
create table if not exists privacy.deletion_request (
    deletion_request_id string not null,
    tenant_id string,
    canonical_customer_id string not null,
    email_sha256 string,
    request_type string not null,
    requested_at timestamp_tz not null,
    status string not null,
    handling_notes string,
    primary key (deletion_request_id)
);
