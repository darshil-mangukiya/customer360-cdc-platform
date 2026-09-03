{{ config(materialized='table') }}

with sequenced as (
    select
        s.*,
        map.canonical_customer_id,
        lead(s.event_timestamp) over (
            partition by s.tenant_id, s.source_subscription_id
            order by s.event_timestamp, s.event_id
        ) as next_event_timestamp
    from {{ ref('stg_subscriptions') }} s
    left join {{ source('identity', 'customer_identity_map') }} map
        on map.tenant_id = s.tenant_id
        and map.source_system = s.source_system
        and map.source_record_id = s.source_subscription_id
)

select
    {{ surrogate_key(["tenant_id", "source_subscription_id", "event_timestamp", "subscription_status"]) }} as subscription_history_sk,
    tenant_id,
    canonical_customer_id,
    subscription_id,
    plan_name,
    subscription_status,
    mrr,
    event_timestamp as valid_from,
    next_event_timestamp as valid_to,
    next_event_timestamp is null and not is_delete as is_current,
    event_timestamp as effective_timestamp,
    event_id as source_event_id,
    case when is_delete then 'source_delete' else operation_type end as change_reason,
    is_delete
from sequenced
where subscription_id is not null
