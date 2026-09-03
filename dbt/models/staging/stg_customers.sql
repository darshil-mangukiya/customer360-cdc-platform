{{ config(materialized='view') }}

select
    event_id,
    source_system,
    record_primary_key as source_customer_id,
    operation_type,
    event_timestamp,
    event_sequence_number,
    source_lsn,
    batch_id,
    {{ json_field('payload', 'customer_id') }} as customer_id,
    nullif(lower({{ json_field('payload', 'email') }}), '') as email,
    nullif({{ json_field('payload', 'phone') }}, '') as phone,
    {{ json_field('payload', 'external_account_id') }} as external_account_id,
    {{ json_field('payload', 'first_name') }} as first_name,
    {{ json_field('payload', 'last_name') }} as last_name,
    {{ json_field('payload', 'tenant_id') }} as tenant_id,
    {{ json_field('payload', 'business_unit') }} as business_unit,
    {{ json_field('payload', 'customer_status') }} as customer_status,
    nullif({{ json_field('payload', 'updated_at') }}, '')::{{ tstz_type() }} as source_updated_at,
    is_delete
from {{ ref('stg_cdc_events') }}
where source_table = 'customers'
