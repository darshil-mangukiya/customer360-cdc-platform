create table if not exists mart.mart_customer_360_current (
    canonical_customer_id text primary key,
    tenant_id text,
    business_unit text,
    primary_email text,
    primary_phone text,
    lifecycle_stage text,
    current_plan_name text,
    current_subscription_status text,
    total_orders int,
    total_revenue numeric(14, 2),
    engagement_events_30d int,
    support_cases_90d int,
    open_support_cases int,
    health_score int,
    churn_risk_score int,
    churn_risk_band text,
    last_activity_at timestamptz,
    updated_at timestamptz not null default now()
);

create table if not exists mart.mart_customer_lifecycle_history (
    lifecycle_history_sk text primary key,
    tenant_id text not null,
    canonical_customer_id text not null,
    lifecycle_stage text not null,
    stage_started_at timestamptz not null,
    stage_ended_at timestamptz,
    is_current boolean not null,
    source_event_id text
);

create table if not exists mart.fct_subscription_history (
    subscription_history_sk text primary key,
    tenant_id text not null,
    canonical_customer_id text,
    subscription_id text not null,
    plan_name text,
    subscription_status text,
    mrr numeric(14, 2),
    valid_from timestamptz not null,
    valid_to timestamptz,
    is_current boolean not null
);

create table if not exists mart.fct_order_history (
    tenant_id text not null,
    order_id text not null,
    canonical_customer_id text,
    ordered_at timestamptz not null,
    order_status text not null,
    gross_amount numeric(14, 2) not null,
    currency text not null,
    source_event_id text not null,
    primary key (tenant_id, order_id)
);

create table if not exists mart.mart_customer_health (
    canonical_customer_id text primary key,
    tenant_id text not null,
    health_score int not null,
    churn_risk_score int not null,
    churn_risk_band text not null,
    feature_payload jsonb not null,
    calculated_at timestamptz not null default now()
);

create table if not exists activation.export_customer_segment (
    canonical_customer_id text primary key,
    tenant_id text,
    business_unit text,
    email_sha256 text,
    phone_sha256 text,
    export_timestamp timestamptz not null,
    customer_segment text not null,
    source_lineage_refs text
);

create table if not exists activation.export_suppressed_customers (
    canonical_customer_id text not null,
    tenant_id text,
    activation_suppression_reason text not null,
    suppressed_at timestamptz not null,
    primary key (canonical_customer_id, activation_suppression_reason)
);

create table if not exists activation.export_churn_risk (
    canonical_customer_id text primary key,
    tenant_id text,
    email_sha256 text,
    phone_sha256 text,
    export_timestamp timestamptz not null,
    churn_risk_score int not null,
    churn_risk_band text not null,
    last_refresh_time timestamptz not null,
    source_lineage_refs text
);

create table if not exists activation.export_lifecycle_stage (
    canonical_customer_id text primary key,
    tenant_id text,
    email_sha256 text,
    phone_sha256 text,
    export_timestamp timestamptz not null,
    lifecycle_stage text not null,
    last_refresh_time timestamptz not null
);

create table if not exists activation.export_customer_health_score (
    canonical_customer_id text primary key,
    tenant_id text,
    email_sha256 text,
    phone_sha256 text,
    export_timestamp timestamptz not null,
    health_score int not null,
    last_refresh_time timestamptz not null,
    source_lineage_refs text
);

create table if not exists activation.export_support_priority (
    canonical_customer_id text primary key,
    tenant_id text,
    email_sha256 text,
    phone_sha256 text,
    export_timestamp timestamptz not null,
    support_priority text not null,
    churn_risk_band text not null,
    source_lineage_refs text
);

create table if not exists activation.export_campaign_target (
    canonical_customer_id text primary key,
    tenant_id text,
    business_unit text,
    email_sha256 text,
    phone_sha256 text,
    export_timestamp timestamptz not null,
    campaign_target text not null,
    customer_segment text not null,
    churn_risk_band text not null
);

create table if not exists activation.reverse_etl_sync_run_log (
    sync_run_id text primary key,
    destination_name text not null,
    export_file text not null,
    destination_object text not null,
    started_at timestamptz not null,
    ended_at timestamptz not null,
    attempted_count int not null,
    success_count int not null,
    failed_count int not null,
    inserted_count int not null default 0,
    updated_count int not null default 0,
    skipped_count int not null default 0,
    retry_count int not null,
    rate_limit_events int not null,
    sync_status text not null
);

create table if not exists activation.reverse_etl_sync_failed_row (
    sync_failed_row_id bigserial primary key,
    sync_run_id text not null references activation.reverse_etl_sync_run_log (sync_run_id),
    destination_name text not null,
    export_file text not null,
    canonical_customer_id text,
    tenant_id text,
    idempotency_key text not null,
    failure_reason text not null,
    attempts int not null,
    is_retryable boolean not null,
    failed_at timestamptz not null,
    payload_json jsonb not null
);

create table if not exists activation.reverse_etl_destination_state (
    destination_record_key text primary key,
    destination_name text not null,
    export_file text not null,
    canonical_customer_id text not null,
    tenant_id text,
    idempotency_key text not null,
    payload_hash text not null,
    last_export_timestamp timestamptz,
    last_synced_at timestamptz not null,
    last_sync_run_id text not null,
    sync_action text not null
);

create table if not exists activation.reverse_etl_destination_config (
    destination_name text primary key,
    export_file text not null,
    destination_object text not null,
    max_rows_per_request int not null,
    max_retries int not null,
    required_fields text
);

create table if not exists activation.reverse_etl_payload_audit (
    payload_audit_id bigserial primary key,
    sync_run_id text not null,
    tenant_id text,
    destination_name text not null,
    export_file text not null,
    canonical_customer_id text,
    idempotency_key text not null,
    payload_hash text not null,
    sync_mode text not null,
    sync_status text not null,
    retry_count int not null,
    failure_reason text,
    audited_at timestamptz not null
);

create table if not exists activation.reverse_etl_destination_status (
    destination_name text primary key,
    latest_sync_run_id text not null,
    export_file text not null,
    last_sync_status text not null,
    attempted_count int not null,
    success_count int not null,
    failed_count int not null,
    retry_count int not null,
    last_synced_at timestamptz not null
);

-- Activation reconciliation. One row per (destination, tenant,
-- export) reverse-ETL run, proving warehouse_eligible_count = successful + failed +
-- suppressed + skipped + duplicate. Populated by reverse_etl.reconciliation.reconcile.
create table if not exists activation.activation_reconciliation (
    run_id text primary key,
    tenant_id text not null,
    destination text not null,
    export_name text not null,
    warehouse_eligible_count int not null,
    export_count int not null,
    attempted_count int not null,
    successful_count int not null,
    failed_count int not null,
    suppressed_count int not null,
    skipped_count int not null,
    duplicate_count int not null,
    variance_count int not null,
    variance_pct numeric(6, 2) not null,
    status text not null check (status in ('reconciled', 'variance_detected')),
    started_at timestamptz not null,
    completed_at timestamptz not null
);

create index if not exists ix_activation_reconciliation_tenant_status
on activation.activation_reconciliation (tenant_id, status);

-- Drill-down findings behind a variance_detected reconciliation run: which specific
-- canonical customer IDs (or idempotency keys) caused the mismatch, and why.
create table if not exists activation.activation_reconciliation_finding (
    finding_id text primary key,
    run_id text not null references activation.activation_reconciliation (run_id),
    tenant_id text not null,
    destination text not null,
    export_name text not null,
    finding_type text not null,
    severity text not null check (severity in ('low', 'medium', 'high', 'critical')),
    canonical_customer_id text,
    detail text not null,
    detected_at timestamptz not null
);

create index if not exists ix_activation_reconciliation_finding_run
on activation.activation_reconciliation_finding (run_id);

create index if not exists ix_activation_reconciliation_finding_severity
on activation.activation_reconciliation_finding (severity);
