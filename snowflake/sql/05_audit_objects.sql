-- Snowflake bootstrap: 05_audit_objects.sql
-- Illustrative example of porting a Postgres-native landing table
-- (warehouse/sql/01_raw_audit_tables.sql: raw.raw_cdc_events) to Snowflake. Semi-
-- structured payloads move from Postgres JSONB columns to Snowflake VARIANT columns;
-- everything else (event metadata, CDC envelope fields, hashes) is a direct port.
--
-- This remains a deliberately minimal raw contract rather than a full 1:1 port of
-- every PostgreSQL operational table. It was live-executed on 2026-08-22; dbt owns
-- the modeled mart and activation relations.

use role sysadmin;
use database c360;
use schema raw;

create table if not exists raw_cdc_events (
    event_id string not null,
    tenant_id string not null,
    source_system string not null,
    source_table string not null,
    operation_type string not null,
    event_timestamp timestamp_tz not null,
    record_primary_key string not null,
    payload_before variant,
    payload_after variant,
    batch_id string,
    schema_version string,
    topic_name string,
    envelope_hash string,
    ingested_at timestamp_tz not null default current_timestamp(),
    event_sequence_number number,
    source_transaction_id string,
    source_lsn string,
    source_commit_timestamp timestamp_tz,
    ingestion_timestamp timestamp_tz,
    kafka_topic string,
    kafka_partition number,
    kafka_offset number,
    event_hash string,
    replay_batch_id string,
    is_replay boolean default false
)
comment = 'Snowflake raw CDC envelope using VARIANT payloads; see warehouse/sql/01_raw_audit_tables.sql';

create table if not exists rejected_events (
    event_id string not null,
    tenant_id string,
    source_system string,
    source_table string,
    rejection_reason string not null,
    rejection_category string,
    rejection_stage string,
    error_detail string,
    raw_reference string,
    retry_eligible boolean default false,
    rejected_at timestamp_tz not null default current_timestamp()
)
comment = 'Snowflake port of Postgres raw.rejected_events dead-letter table.';
