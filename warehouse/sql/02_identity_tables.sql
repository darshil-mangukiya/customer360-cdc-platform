create table if not exists identity.dim_customer_canonical (
    canonical_customer_id text primary key,
    tenant_id text not null default 'tenant_unknown',
    business_unit text,
    primary_email text,
    primary_phone text,
    external_account_id text,
    first_name text,
    last_name text,
    customer_status text,
    first_seen_at timestamptz not null,
    last_seen_at timestamptz not null,
    source_record_count int not null,
    survivorship_rule text not null,
    canonical_customer_version int not null default 1,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create unique index if not exists ux_dim_customer_canonical_external_account
on identity.dim_customer_canonical (tenant_id, external_account_id)
where external_account_id is not null;

create index if not exists ix_dim_customer_canonical_email
on identity.dim_customer_canonical (tenant_id, primary_email)
where primary_email is not null;

create index if not exists ix_dim_customer_canonical_tenant
on identity.dim_customer_canonical (tenant_id, canonical_customer_id);

create table if not exists identity.customer_identity_map (
    canonical_customer_id text not null references identity.dim_customer_canonical (canonical_customer_id),
    tenant_id text not null default 'tenant_unknown',
    source_system text not null,
    source_table text not null,
    source_record_id text not null,
    match_rule text not null,
    match_confidence numeric(5, 4) not null,
    canonical_customer_version int not null default 1,
    identifier_fingerprint text not null,
    first_seen_at timestamptz not null,
    last_seen_at timestamptz not null,
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    primary key (tenant_id, source_system, source_record_id)
);

create index if not exists ix_customer_identity_map_canonical
on identity.customer_identity_map (canonical_customer_id);

create table if not exists identity.identity_resolution_audit (
    identity_resolution_audit_id bigserial primary key,
    canonical_customer_id text not null,
    source_record_id text not null,
    source_system text not null,
    matched_identifiers text not null,
    decision_reason text not null,
    confidence_score numeric(5, 4) not null,
    resolved_at timestamptz not null
);

create table if not exists identity.identity_graph_node (
    node_id text primary key,
    tenant_id text,
    node_type text not null,
    identifier_type text,
    identifier_value_hash text,
    source_system text,
    source_record_id text,
    first_seen_at timestamptz not null,
    last_seen_at timestamptz not null
);

create table if not exists identity.identity_graph_edge (
    edge_id text primary key,
    tenant_id text,
    left_node_id text not null,
    right_node_id text not null,
    match_rule text not null,
    match_confidence numeric(5, 4) not null,
    evidence_count int not null,
    first_seen_at timestamptz not null,
    last_seen_at timestamptz not null
);

create table if not exists identity.identity_match_rule (
    match_rule text primary key,
    rule_strength text not null,
    match_confidence numeric(5, 4) not null,
    identifier_types text not null,
    description text not null
);

create table if not exists identity.identity_resolution_run (
    identity_resolution_run_id text primary key,
    resolved_at timestamptz not null,
    canonical_customer_count int not null,
    source_record_count int not null,
    graph_node_count int not null,
    graph_edge_count int not null,
    merge_event_count int not null,
    run_status text not null
);

create table if not exists identity.identity_merge_event (
    merge_event_id text primary key,
    tenant_id text,
    canonical_customer_id text not null,
    canonical_customer_version int not null,
    merged_source_records text not null,
    strongest_match_rule text not null,
    match_confidence numeric(5, 4) not null,
    occurred_at timestamptz not null,
    merge_reason text not null
);

create table if not exists identity.identity_link_explanation (
    explanation_id text primary key,
    tenant_id text,
    canonical_customer_id text not null,
    source_system text not null,
    source_record_id text not null,
    linked_identifier_types text not null,
    match_rule text not null,
    match_confidence numeric(5, 4) not null,
    explanation_text text not null,
    generated_at timestamptz not null
);

create table if not exists identity.customer_identity_map_history (
    history_id text primary key,
    tenant_id text,
    canonical_customer_id text not null,
    source_system text not null,
    source_record_id text not null,
    canonical_customer_version int not null,
    valid_from timestamptz not null,
    valid_to timestamptz,
    is_current boolean not null,
    change_reason text not null
);

-- Identity stewardship: review queue for ambiguous or
-- conflicting identity-resolution evidence that the deterministic resolver
-- deliberately did not auto-merge or flagged after merging. Populated by
-- identity_resolution.stewardship.detect_review_candidates.
create table if not exists identity.identity_review_queue (
    review_case_id text primary key,
    tenant_id text not null default 'tenant_unknown',
    canonical_customer_id text not null,
    candidate_customer_id text not null,
    source_system text not null,
    source_customer_id text not null,
    conflict_type text not null,
    match_rule text not null,
    confidence_score numeric(5, 4) not null,
    evidence_summary text not null,
    current_status text not null default 'OPEN'
        check (current_status in ('OPEN', 'IN_REVIEW', 'APPROVED', 'REJECTED', 'RESOLVED')),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    resolved_at timestamptz,
    reviewer text,
    decision text
        check (decision is null or decision in
            ('APPROVE_MERGE', 'REJECT_MERGE', 'NEEDS_REVIEW', 'APPROVE_UNMERGE', 'IGNORE_FALSE_POSITIVE')),
    decision_reason text,
    survivorship_rule text,
    source_event_id text
);

create index if not exists ix_identity_review_queue_status
on identity.identity_review_queue (tenant_id, current_status);

create index if not exists ix_identity_review_queue_canonical
on identity.identity_review_queue (canonical_customer_id);

-- Documented, testable survivorship rule table (spec section 14). Mirrors
-- identity_resolution.stewardship.survivorship_rules(); loaded for query/audit
-- convenience, the Python function is the source of truth used by tests.
create table if not exists identity.identity_survivorship_rule (
    field text primary key,
    authoritative_source text not null,
    fallback_source text not null,
    tie_breaking_rule text not null,
    null_behavior text not null,
    conflict_behavior text not null,
    privacy_behavior text not null
);

-- Reversible merge/unmerge audit trail (spec sections 17). Every steward-approved
-- merge or unmerge is recorded here; canonical_customer_id reassignments in
-- identity.customer_identity_map should always have a corresponding audit row.
create table if not exists identity.identity_merge_audit (
    merge_audit_id text primary key,
    tenant_id text,
    review_case_id text references identity.identity_review_queue (review_case_id),
    source_canonical_customer_id text not null,
    target_canonical_customer_id text not null,
    reviewer text not null,
    reason text not null,
    occurred_at timestamptz not null default now()
);

create table if not exists identity.identity_unmerge_audit (
    unmerge_audit_id text primary key,
    tenant_id text,
    review_case_id text references identity.identity_review_queue (review_case_id),
    original_canonical_customer_id text not null,
    new_canonical_customer_id text not null,
    source_system text not null,
    source_record_id text not null,
    reviewer text not null,
    reason text not null,
    occurred_at timestamptz not null default now()
);

alter table identity.identity_unmerge_audit
add column if not exists review_case_id text
references identity.identity_review_queue (review_case_id);

create unique index if not exists ux_identity_merge_audit_review_case
on identity.identity_merge_audit (review_case_id)
where review_case_id is not null;

create unique index if not exists ux_identity_unmerge_audit_review_case
on identity.identity_unmerge_audit (review_case_id)
where review_case_id is not null;
