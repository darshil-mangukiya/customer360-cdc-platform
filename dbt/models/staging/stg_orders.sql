{{ config(materialized='view') }}

select
    event_id,
    source_system,
    record_primary_key as source_order_id,
    operation_type,
    event_timestamp,
    event_sequence_number,
    source_lsn,
    batch_id,
    {{ json_field('payload', 'order_id') }} as order_id,
    {{ json_field('payload', 'tenant_id') }} as tenant_id,
    {{ json_field('payload', 'business_unit') }} as business_unit,
    {{ json_field('payload', 'order_customer_ref') }} as order_customer_ref,
    nullif(lower({{ json_field('payload', 'email') }}), '') as email,
    {{ json_field('payload', 'subscription_id') }} as subscription_id,
    {{ json_field('payload', 'order_status') }} as order_status,
    nullif({{ json_field('payload', 'gross_amount') }}, '')::{{ decimal_type() }} as gross_amount,
    {{ json_field('payload', 'currency') }} as currency,
    nullif({{ json_field('payload', 'ordered_at') }}, '')::{{ tstz_type() }} as ordered_at,
    nullif({{ json_field('payload', 'updated_at') }}, '')::{{ tstz_type() }} as source_updated_at,
    is_delete
from {{ ref('stg_cdc_events') }}
where source_table = 'orders'
