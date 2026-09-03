{{ config(materialized='table') }}

select
    c.tenant_id,
    c.business_unit,
    count(*) as canonical_customer_count,
    sum(c.source_record_count) as mapped_source_record_count,
    round(avg(c.source_record_count)::{{ decimal_type() }}, 2) as avg_source_records_per_customer,
    {{ conditional_count('c.primary_email is null') }} as customers_missing_email,
    {{ conditional_count('c.primary_phone is null') }} as customers_missing_phone,
    {{ conditional_count('c.source_record_count >= 5') }} as heavily_linked_customers,
    current_timestamp as calculated_at
from {{ source('identity', 'dim_customer_canonical') }} c
group by 1, 2
