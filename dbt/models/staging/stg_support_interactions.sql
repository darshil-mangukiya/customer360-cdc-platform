{{ config(materialized='view') }}

select
    event_id,
    source_system,
    record_primary_key as source_support_interaction_id,
    operation_type,
    event_timestamp,
    batch_id,
    {{ json_field('payload', 'support_interaction_id') }} as support_interaction_id,
    {{ json_field('payload', 'tenant_id') }} as tenant_id,
    {{ json_field('payload', 'business_unit') }} as business_unit,
    {{ json_field('payload', 'support_customer_ref') }} as support_customer_ref,
    nullif(lower({{ json_field('payload', 'email') }}), '') as email,
    {{ json_field('payload', 'phone') }} as phone,
    {{ json_field('payload', 'reason') }} as reason,
    {{ json_field('payload', 'priority') }} as priority,
    {{ json_field('payload', 'status') }} as status,
    nullif({{ json_field('payload', 'csat_score') }}, '')::int as csat_score,
    nullif({{ json_field('payload', 'created_at') }}, '')::{{ tstz_type() }} as created_at,
    nullif({{ json_field('payload', 'updated_at') }}, '')::{{ tstz_type() }} as source_updated_at,
    is_delete
from {{ ref('stg_cdc_events') }}
where source_table = 'support_interactions'
