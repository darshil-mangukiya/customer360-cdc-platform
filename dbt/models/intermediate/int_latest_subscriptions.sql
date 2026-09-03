{{ config(materialized='view') }}

with ranked as (
    select
        *,
        row_number() over (
            partition by tenant_id, source_system, source_subscription_id
            order by event_sequence_number desc, event_timestamp desc, event_id desc
        ) as rn
    from {{ ref('stg_subscriptions') }}
)

select *
from ranked
where rn = 1 and not is_delete
