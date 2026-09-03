create table if not exists privacy.deletion_request (
    deletion_request_id text primary key,
    tenant_id text,
    canonical_customer_id text not null,
    email_sha256 text,
    request_type text not null,
    requested_at timestamptz not null,
    status text not null,
    handling_notes text
);

create table if not exists privacy.activation_suppression_list (
    canonical_customer_id text primary key,
    tenant_id text,
    email_sha256 text,
    suppressed_at timestamptz not null default now(),
    suppression_reason text not null,
    deletion_request_id text references privacy.deletion_request (deletion_request_id)
);

create table if not exists privacy.customer_consent_history (
    consent_history_id bigserial primary key,
    tenant_id text,
    canonical_customer_id text not null,
    marketing_consent_status text not null,
    email_opt_in boolean not null,
    sms_opt_in boolean not null,
    push_opt_in boolean not null,
    unsubscribe_status text not null,
    do_not_contact_flag boolean not null,
    deletion_requested_flag boolean not null,
    deletion_request_timestamp timestamptz,
    last_consent_event_at timestamptz
);

create table if not exists privacy.pii_tokenization_map (
    pii_token_id text primary key,
    tenant_id text,
    canonical_customer_id text,
    pii_field_name text not null,
    pii_value_sha256 text not null,
    token_scope text not null,
    created_at timestamptz not null default now()
);

create table if not exists privacy.export_suppressed_customers (
    tenant_id text,
    canonical_customer_id text not null,
    activation_suppression_reason text not null,
    marketing_consent_status text not null,
    email_opt_in boolean not null,
    sms_opt_in boolean not null,
    push_opt_in boolean not null,
    do_not_contact_flag boolean not null,
    deletion_requested_flag boolean not null,
    suppressed_at timestamptz not null,
    primary key (canonical_customer_id, activation_suppression_reason)
);

create table if not exists privacy.privacy_audit_log (
    privacy_audit_log_id bigserial primary key,
    tenant_id text,
    canonical_customer_id text,
    privacy_action text not null,
    action_status text not null,
    actor_type text not null,
    action_payload jsonb,
    created_at timestamptz not null default now()
);

create table if not exists privacy.retention_policy (
    dataset_name text primary key,
    retention_days int not null,
    retention_reason text not null,
    owner text not null,
    reviewed_at timestamptz not null default now()
);

insert into privacy.retention_policy (dataset_name, retention_days, retention_reason, owner)
values
    ('raw.raw_cdc_events', 180, 'hot replay and audit window before archive', 'Data Platform'),
    ('raw.rejected_events', 365, 'DLQ repair and source contract audit', 'Data Platform'),
    ('activation.*', 90, 'operational sync traceability', 'Lifecycle Analytics')
on conflict (dataset_name) do update set
    retention_days = excluded.retention_days,
    retention_reason = excluded.retention_reason,
    owner = excluded.owner,
    reviewed_at = now();
