{{ config(materialized='table') }}

with current_sub_ranked as (
    select
        tenant_id,
        canonical_customer_id,
        plan_name,
        subscription_status,
        valid_from,
        row_number() over (
            partition by tenant_id, canonical_customer_id
            order by valid_from desc
        ) as current_rank
    from {{ ref('fct_subscription_history') }}
    where is_current
),
current_sub as (
    select tenant_id, canonical_customer_id, plan_name, subscription_status, valid_from
    from current_sub_ranked
    where current_rank = 1
),
orders as (
    select
        tenant_id,
        canonical_customer_id,
        count(*) as total_orders,
        sum(case when order_status = 'paid' then gross_amount else 0 end) as total_revenue,
        max(ordered_at) as last_order_at
    from {{ ref('fct_order_history') }}
    group by 1, 2
),
activity as (
    select * from {{ ref('int_customer_activity_rollup') }}
)

select
    c.canonical_customer_id,
    c.tenant_id,
    c.business_unit,
    c.primary_email,
    c.primary_phone,
    case
        when current_sub.subscription_status = 'canceled' then 'churned'
        when current_sub.subscription_status = 'past_due' then 'at_risk'
        when current_sub.subscription_status = 'active' then 'active_customer'
        when current_sub.subscription_status = 'trialing' then 'trial_or_lead'
        else 'unknown'
    end as lifecycle_stage,
    current_sub.plan_name as current_plan_name,
    current_sub.subscription_status as current_subscription_status,
    coalesce(orders.total_orders, 0)::int as total_orders,
    coalesce(orders.total_revenue, 0) as total_revenue,
    coalesce(activity.engagement_events, 0)::int as engagement_events_30d,
    coalesce(activity.support_cases, 0)::int as support_cases_90d,
    coalesce(activity.open_support_cases, 0)::int as open_support_cases,
    health.health_score,
    health.churn_risk_score,
    health.churn_risk_band,
    greatest(orders.last_order_at, activity.last_activity_at, current_sub.valid_from) as last_activity_at,
    {{ current_tstz() }} as updated_at
from {{ source('identity', 'dim_customer_canonical') }} c
left join current_sub using (tenant_id, canonical_customer_id)
left join orders using (tenant_id, canonical_customer_id)
left join activity using (tenant_id, canonical_customer_id)
left join {{ ref('mart_customer_health') }} health using (tenant_id, canonical_customer_id)
