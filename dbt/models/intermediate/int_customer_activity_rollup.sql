{{ config(materialized='view') }}

with engagement as (
    select
        e.tenant_id,
        map.canonical_customer_id,
        count(*) as engagement_event_rows,
        sum(coalesce(event_count, 0)) as engagement_events,
        sum(coalesce(session_minutes, 0)) as session_minutes,
        max(activity_at) as last_engagement_at
    from {{ ref('stg_engagement_events') }} e
    left join {{ source('identity', 'customer_identity_map') }} map
        on map.tenant_id = e.tenant_id
        and map.source_system = e.source_system
        and map.source_record_id = e.source_engagement_event_id
    where not e.is_delete
    group by 1, 2
),
support as (
    select
        s.tenant_id,
        map.canonical_customer_id,
        count(*) as support_cases,
        {{ conditional_count("s.status = 'open'") }} as open_support_cases,
        {{ conditional_count('s.csat_score <= 2') }} as low_csat_cases,
        max(s.created_at) as last_support_at
    from {{ ref('stg_support_interactions') }} s
    left join {{ source('identity', 'customer_identity_map') }} map
        on map.tenant_id = s.tenant_id
        and map.source_system = s.source_system
        and map.source_record_id = s.source_support_interaction_id
    where not s.is_delete
    group by 1, 2
)

select
    coalesce(e.tenant_id, s.tenant_id) as tenant_id,
    coalesce(e.canonical_customer_id, s.canonical_customer_id) as canonical_customer_id,
    coalesce(e.engagement_event_rows, 0) as engagement_event_rows,
    coalesce(e.engagement_events, 0) as engagement_events,
    coalesce(e.session_minutes, 0) as session_minutes,
    coalesce(s.support_cases, 0) as support_cases,
    coalesce(s.open_support_cases, 0) as open_support_cases,
    coalesce(s.low_csat_cases, 0) as low_csat_cases,
    greatest(e.last_engagement_at, s.last_support_at) as last_activity_at
from engagement e
full outer join support s using (tenant_id, canonical_customer_id)
