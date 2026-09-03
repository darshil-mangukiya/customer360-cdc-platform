{{ config(materialized='view') }}

select
    event_id,
    source_system,
    record_primary_key as source_subscription_id,
    operation_type,
    event_timestamp,
    event_sequence_number,
    source_lsn,
    batch_id,
    {{ json_field('payload', 'subscription_id') }} as subscription_id,
    {{ json_field('payload', 'tenant_id') }} as tenant_id,
    {{ json_field('payload', 'business_unit') }} as business_unit,
    {{ json_field('payload', 'customer_id') }} as customer_id,
    {{ json_field('payload', 'external_account_id') }} as external_account_id,
    nullif(lower({{ json_field('payload', 'email') }}), '') as email,
    {{ json_field('payload', 'plan_name') }} as plan_name,
    {{ json_field('payload', 'subscription_status') }} as subscription_status,
    {{ json_field('payload', 'billing_period') }} as billing_period,
    nullif({{ json_field('payload', 'mrr') }}, '')::{{ decimal_type() }} as mrr,
    nullif({{ json_field('payload', 'start_date') }}, '')::{{ tstz_type() }} as start_date,
    nullif({{ json_field('payload', 'cancel_at') }}, '')::{{ tstz_type() }} as cancel_at,
    nullif({{ json_field('payload', 'updated_at') }}, '')::{{ tstz_type() }} as source_updated_at,
    is_delete
from {{ ref('stg_cdc_events') }}
where source_table = 'subscriptions'
