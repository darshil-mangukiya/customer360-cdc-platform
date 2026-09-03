{{ config(materialized='incremental', unique_key=['tenant_id', 'order_id']) }}

select
    o.tenant_id,
    o.order_id,
    map.canonical_customer_id,
    o.ordered_at,
    o.order_status,
    o.gross_amount,
    o.currency,
    o.event_id as source_event_id,
    o.event_timestamp as source_event_timestamp,
    {{ current_tstz() }} as loaded_at
from {{ ref('int_latest_orders') }} o
left join {{ source('identity', 'customer_identity_map') }} map
    on map.tenant_id = o.tenant_id
    and map.source_system = o.source_system
    and map.source_record_id = o.source_order_id
where o.order_id is not null
{% if is_incremental() %}
  and o.event_timestamp >= (
      select coalesce({{ days_before('max(source_event_timestamp)', 3) }}, '1900-01-01'::{{ tstz_type() }})
      from {{ this }}
  )
{% endif %}
