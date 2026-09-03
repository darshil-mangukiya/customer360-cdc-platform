{{ config(materialized='view') }}

select
    event_id,
    source_system,
    record_primary_key as source_engagement_event_id,
    operation_type,
    event_timestamp,
    batch_id,
    {{ json_field('payload', 'engagement_event_id') }} as engagement_event_id,
    {{ json_field('payload', 'tenant_id') }} as tenant_id,
    {{ json_field('payload', 'business_unit') }} as business_unit,
    {{ json_field('payload', 'device_id') }} as device_id,
    {{ json_field('payload', 'customer_id') }} as customer_id,
    nullif(lower({{ json_field('payload', 'email') }}), '') as email,
    {{ json_field('payload', 'event_name') }} as event_name,
    nullif({{ json_field('payload', 'event_count') }}, '')::int as event_count,
    nullif({{ json_field('payload', 'session_minutes') }}, '')::int as session_minutes,
    nullif({{ json_field('payload', 'event_timestamp') }}, '')::{{ tstz_type() }} as activity_at,
    is_delete
from {{ ref('stg_cdc_events') }}
where source_table = 'engagement_events'
