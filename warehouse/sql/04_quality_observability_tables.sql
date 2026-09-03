create table if not exists observability.validation_failures (
    validation_failure_id bigserial primary key,
    check_name text not null,
    severity text not null,
    entity_key text not null,
    failure_reason text not null,
    observed_value text,
    detected_at timestamptz not null
);

create table if not exists observability.quality_summary (
    quality_summary_id bigserial primary key,
    tenant_id text default 'all',
    check_name text not null,
    severity text not null,
    status text not null,
    failure_count int not null,
    checked_at timestamptz not null
);

create table if not exists observability.pipeline_run_log (
    pipeline_run_log_id bigserial primary key,
    run_id text not null,
    tenant_id text not null default 'all',
    stage_name text not null,
    started_at timestamptz not null,
    ended_at timestamptz not null,
    row_count int not null,
    status text not null,
    details text
);

create table if not exists observability.freshness_status (
    freshness_status_id bigserial primary key,
    tenant_id text not null default 'tenant_unknown',
    entity_name text not null,
    max_event_timestamp timestamptz not null,
    observed_at timestamptz not null,
    lag_minutes int not null,
    status text not null
);

create table if not exists observability.tenant_quality_summary (
    tenant_quality_summary_id bigserial primary key,
    tenant_id text not null,
    checked_at timestamptz not null,
    total_checks int not null,
    failed_checks int not null,
    critical_failures int not null,
    quality_score numeric(5, 2) not null
);

create table if not exists observability.pipeline_alert (
    pipeline_alert_id bigserial primary key,
    alert_name text not null,
    severity text not null,
    alert_status text not null default 'open',
    entity_name text,
    alert_payload jsonb,
    created_at timestamptz not null default now(),
    resolved_at timestamptz
);

create table if not exists observability.data_lineage_event (
    data_lineage_event_id text primary key,
    lineage_run_id text not null,
    tenant_id text,
    source_name text not null,
    target_name text not null,
    operation_name text not null,
    event_type text not null,
    emitted_at timestamptz not null,
    payload jsonb not null
);

create table if not exists observability.model_execution_log (
    model_execution_id text primary key,
    dbt_invocation_id text,
    model_name text not null,
    tenant_id text default 'all',
    started_at timestamptz not null,
    ended_at timestamptz not null,
    runtime_seconds numeric(12, 3) not null,
    row_count int,
    execution_status text not null
);

create table if not exists observability.source_to_target_field_mapping (
    mapping_id text primary key,
    source_system text not null,
    source_table text not null,
    source_field text not null,
    target_schema text not null,
    target_table text not null,
    target_field text not null,
    transformation_rule text not null,
    pii_classification text not null default 'non_pii'
);

create table if not exists observability.export_lineage_audit (
    export_lineage_id text primary key,
    lineage_run_id text not null,
    tenant_id text,
    export_name text not null,
    source_models text not null,
    destination_name text,
    exported_row_count int not null,
    exported_at timestamptz not null
);

create table if not exists observability.pipeline_health_snapshot (
    pipeline_health_snapshot_id text primary key,
    tenant_id text not null,
    observed_at timestamptz not null,
    cdc_lag_minutes int not null,
    rejected_record_rate numeric(8, 4) not null,
    failed_validation_count int not null,
    stale_activation_output_count int not null,
    reverse_etl_failure_count int not null,
    identity_merge_anomaly_rate numeric(8, 4) not null,
    health_score numeric(5, 2) not null
);

create table if not exists observability.quality_scorecard (
    scorecard_id text primary key,
    tenant_id text not null,
    domain_name text not null,
    quality_dimension text not null,
    score numeric(5, 2) not null,
    status text not null,
    measured_at timestamptz not null
);
