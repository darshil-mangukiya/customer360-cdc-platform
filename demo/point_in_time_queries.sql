-- Point-in-time Customer 360 reconstruction examples.
-- Replace the values in params with a real canonical_customer_id and analysis timestamp from your run.

with params as (
    select
        'cc_example'::text as canonical_customer_id,
        '2025-02-15T00:00:00Z'::timestamptz as as_of_timestamp
)
select
    p.canonical_customer_id,
    p.as_of_timestamp,
    h.lifecycle_stage,
    h.stage_started_at,
    h.stage_ended_at
from params p
join mart.mart_customer_lifecycle_history h
    on h.canonical_customer_id = p.canonical_customer_id
    and h.stage_started_at <= p.as_of_timestamp
    and coalesce(h.stage_ended_at, '9999-12-31'::timestamptz) > p.as_of_timestamp;

-- Subscription state as of a timestamp using SCD-style validity windows.
with params as (
    select
        'cc_example'::text as canonical_customer_id,
        '2025-02-15T00:00:00Z'::timestamptz as as_of_timestamp
)
select
    s.canonical_customer_id,
    s.subscription_id,
    s.plan_name,
    s.subscription_status,
    s.mrr,
    s.valid_from,
    s.valid_to
from params p
join mart.fct_subscription_history s
    on s.canonical_customer_id = p.canonical_customer_id
    and s.valid_from <= p.as_of_timestamp
    and coalesce(s.valid_to, '9999-12-31'::timestamptz) > p.as_of_timestamp;

-- Raw CDC replay of what the platform knew about a customer's source records by date.
with params as (
    select
        'cc_example'::text as canonical_customer_id,
        '2025-02-15T00:00:00Z'::timestamptz as as_of_timestamp
),
source_records as (
    select
        m.source_system,
        m.source_table,
        m.source_record_id
    from identity.customer_identity_map m
    join params p on p.canonical_customer_id = m.canonical_customer_id
)
select
    e.source_system,
    e.source_table,
    e.record_primary_key,
    e.operation_type,
    e.event_timestamp,
    e.payload_after,
    e.payload_before
from raw.raw_cdc_events e
join source_records r
    on r.source_system = e.source_system
    and r.source_record_id = e.record_primary_key
join params p on e.event_timestamp <= p.as_of_timestamp
order by e.event_timestamp, e.event_id;

-- Identity lineage behind a Customer 360 row.
select
    c.canonical_customer_id,
    c.primary_email,
    c.primary_phone,
    c.survivorship_rule,
    m.source_system,
    m.source_table,
    m.source_record_id,
    m.match_rule,
    m.match_confidence,
    m.first_seen_at,
    m.last_seen_at
from identity.dim_customer_canonical c
join identity.customer_identity_map m using (canonical_customer_id)
where c.canonical_customer_id = 'cc_example'
order by m.source_system, m.source_table, m.source_record_id;
