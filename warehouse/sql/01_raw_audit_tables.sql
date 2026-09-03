create table if not exists raw.raw_cdc_events (
    event_id text primary key,
    tenant_id text not null default 'tenant_unknown',
    source_system text not null,
    source_table text not null,
    operation_type text not null check (operation_type in ('insert', 'update', 'delete')),
    event_timestamp timestamptz not null,
    record_primary_key text not null,
    payload_before jsonb,
    payload_after jsonb,
    batch_id text not null,
    schema_version int not null,
    topic_name text not null,
    envelope_hash text not null,
    ingested_at timestamptz not null default now(),
    event_sequence_number bigint not null default 0,
    source_transaction_id text,
    source_lsn text,
    source_commit_timestamp timestamptz,
    ingestion_timestamp timestamptz,
    kafka_topic text,
    kafka_partition int,
    kafka_offset bigint,
    event_hash text,
    replay_batch_id text,
    is_replay boolean not null default false
);

create unique index if not exists ux_raw_cdc_events_event_hash
    on raw.raw_cdc_events (event_hash)
    where event_hash is not null;

create index if not exists ix_raw_cdc_events_source_ts
    on raw.raw_cdc_events (tenant_id, source_table, event_timestamp desc);

create index if not exists ix_raw_cdc_events_batch
    on raw.raw_cdc_events (tenant_id, batch_id, source_system, source_table);

create index if not exists ix_raw_cdc_events_record
    on raw.raw_cdc_events (tenant_id, source_system, record_primary_key, event_timestamp desc);

create table if not exists raw.rejected_events (
    rejected_event_id bigserial primary key,
    event_id text,
    source_system text,
    source_table text,
    batch_id text,
    rejection_reason text not null,
    raw_event jsonb not null,
    rejected_at timestamptz not null default now()
);

create table if not exists audit.ingestion_log (
    ingestion_log_id bigserial primary key,
    batch_id text not null,
    source_system text not null,
    source_table text not null,
    event_count int not null,
    landed_count int not null,
    rejected_count int not null,
    load_start_time timestamptz not null,
    load_end_time timestamptz not null,
    schema_version int not null,
    load_status text not null,
    created_at timestamptz not null default now()
);

create table if not exists audit.tenant_config (
    tenant_id text primary key,
    tenant_name text not null,
    business_region text not null,
    is_active boolean not null default true,
    data_residency_region text not null,
    default_timezone text not null,
    created_at timestamptz not null default now()
);

create table if not exists audit.tenant_pipeline_sla (
    tenant_id text not null references audit.tenant_config (tenant_id),
    source_table text not null,
    max_lag_minutes int not null,
    expected_daily_min_events int not null,
    severity_on_breach text not null,
    primary key (tenant_id, source_table)
);

create table if not exists audit.tenant_activation_config (
    tenant_id text not null references audit.tenant_config (tenant_id),
    destination_name text not null,
    is_enabled boolean not null default true,
    sync_cadence_minutes int not null,
    required_identifier text not null,
    primary key (tenant_id, destination_name)
);

insert into audit.tenant_config (
    tenant_id, tenant_name, business_region, data_residency_region, default_timezone
)
values
    ('tenant_us', 'North America SaaS', 'americas', 'us', 'America/Los_Angeles'),
    ('tenant_emea', 'EMEA SaaS', 'emea', 'eu', 'Europe/London'),
    ('tenant_apac', 'APAC SaaS', 'apac', 'apac', 'Asia/Singapore'),
    ('tenant_latam', 'LATAM SaaS', 'latam', 'us', 'America/Mexico_City'),
    ('tenant_unknown', 'Unknown Tenant Fallback', 'unknown', 'us', 'UTC')
on conflict (tenant_id) do update set
    tenant_name = excluded.tenant_name,
    business_region = excluded.business_region,
    data_residency_region = excluded.data_residency_region,
    default_timezone = excluded.default_timezone;

insert into audit.tenant_pipeline_sla (
    tenant_id, source_table, max_lag_minutes, expected_daily_min_events, severity_on_breach
)
select tenant_id, source_table, max_lag_minutes, expected_daily_min_events, severity_on_breach
from (
    values
        ('tenant_us', 'customers', 90, 1, 'high'),
        ('tenant_us', 'subscriptions', 60, 1, 'critical'),
        ('tenant_emea', 'customers', 120, 1, 'high'),
        ('tenant_apac', 'customers', 180, 1, 'medium'),
        ('tenant_latam', 'customers', 180, 1, 'medium')
) as rows(tenant_id, source_table, max_lag_minutes, expected_daily_min_events, severity_on_breach)
on conflict (tenant_id, source_table) do update set
    max_lag_minutes = excluded.max_lag_minutes,
    expected_daily_min_events = excluded.expected_daily_min_events,
    severity_on_breach = excluded.severity_on_breach;

insert into audit.tenant_activation_config (
    tenant_id, destination_name, is_enabled, sync_cadence_minutes, required_identifier
)
select tenant_id, destination_name, is_enabled, sync_cadence_minutes, required_identifier
from (
    values
        ('tenant_us', 'salesforce', true, 60, 'email_or_phone_sha256'),
        ('tenant_us', 'hubspot', true, 60, 'email_sha256'),
        ('tenant_us', 'braze', true, 30, 'email_sha256'),
        ('tenant_us', 'zendesk', true, 60, 'email_or_phone_sha256'),
        ('tenant_emea', 'hubspot', true, 90, 'email_sha256'),
        ('tenant_apac', 'braze', true, 120, 'email_sha256')
) as rows(tenant_id, destination_name, is_enabled, sync_cadence_minutes, required_identifier)
on conflict (tenant_id, destination_name) do update set
    is_enabled = excluded.is_enabled,
    sync_cadence_minutes = excluded.sync_cadence_minutes,
    required_identifier = excluded.required_identifier;

create table if not exists audit.watermark_checkpoint (
    checkpoint_id bigserial primary key,
    tenant_id text not null default 'tenant_unknown',
    source_system text not null,
    source_table text not null,
    last_event_timestamp timestamptz not null,
    last_event_id text not null,
    last_sequence_number bigint not null default 0,
    source_lsn text,
    updated_at timestamptz not null default now(),
    unique (tenant_id, source_system, source_table)
);

create table if not exists audit.cdc_event_deduplication_log (
    deduplication_log_id bigserial primary key,
    event_id text not null,
    tenant_id text not null,
    source_system text not null,
    source_table text not null,
    record_primary_key text not null,
    event_hash text not null,
    dedupe_status text not null,
    duplicate_of_event_id text,
    is_replay boolean not null,
    replay_batch_id text,
    logged_at timestamptz not null default now()
);

create table if not exists audit.cdc_ordering_anomalies (
    ordering_anomaly_id bigserial primary key,
    anomaly_id text not null,
    tenant_id text not null,
    source_system text not null,
    source_table text not null,
    record_primary_key text not null,
    event_id text not null,
    previous_event_id text not null,
    previous_sequence_number bigint not null,
    current_sequence_number bigint not null,
    previous_commit_timestamp timestamptz not null,
    current_commit_timestamp timestamptz not null,
    anomaly_type text not null,
    detected_at timestamptz not null default now()
);

create table if not exists audit.cdc_topic_offset_checkpoint (
    kafka_topic text not null,
    kafka_partition int not null,
    max_kafka_offset bigint not null,
    last_event_id text not null,
    tenant_id text not null,
    source_table text not null,
    updated_at timestamptz not null default now(),
    primary key (kafka_topic, kafka_partition)
);

create table if not exists audit.replay_run_log (
    replay_run_id text primary key,
    source_path text not null,
    output_path text not null,
    selected_count int not null,
    start_timestamp timestamptz,
    end_timestamp timestamptz,
    batch_ids text,
    source_tables text,
    reset_checkpoints boolean not null,
    replayed_at timestamptz not null,
    status text not null
);

create table if not exists audit.dlq_reprocess_log (
    dlq_reprocess_id bigserial primary key,
    original_event_id text,
    repaired_event_id text,
    repair_actions jsonb not null,
    repaired_at timestamptz not null,
    load_status text not null
);
