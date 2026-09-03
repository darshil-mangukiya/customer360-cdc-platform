{{ config(materialized='table') }}

with events as (
    select
        tenant_id,
        canonical_customer_id,
        case
            when subscription_status = 'canceled' then 'churned'
            when subscription_status = 'past_due' then 'at_risk'
            when subscription_status = 'active' then 'active_customer'
            when subscription_status = 'trialing' then 'trial_or_lead'
            else 'unknown'
        end as lifecycle_stage,
        valid_from,
        valid_to,
        source_event_id
    from {{ ref('fct_subscription_history') }}
),
sequenced as (
    select
        *,
        lead(valid_from) over (
            partition by tenant_id, canonical_customer_id
            order by valid_from
        ) as next_stage_at
    from events
)

select
    {{ surrogate_key(["tenant_id", "canonical_customer_id", "lifecycle_stage", "valid_from"]) }} as lifecycle_history_sk,
    tenant_id,
    canonical_customer_id,
    lifecycle_stage,
    valid_from as stage_started_at,
    coalesce(valid_to, next_stage_at) as stage_ended_at,
    coalesce(valid_to, next_stage_at) is null as is_current,
    source_event_id
from sequenced
where canonical_customer_id is not null
