{{ config(materialized='table') }}

with orders as (
    select
        tenant_id,
        canonical_customer_id,
        {{ conditional_count("order_status = 'paid'") }} as paid_orders,
        {{ conditional_count("order_status = 'refunded'") }} as refunded_orders,
        sum(case when order_status = 'paid' then gross_amount else 0 end) as gross_revenue
    from {{ ref('fct_order_history') }}
    group by 1, 2
),
activity as (
    select * from {{ ref('int_customer_activity_rollup') }}
),
features as (
    select
        c.tenant_id,
        c.canonical_customer_id,
        coalesce(o.paid_orders, 0) as paid_orders,
        coalesce(o.refunded_orders, 0) as refunded_orders,
        coalesce(o.gross_revenue, 0) as gross_revenue,
        coalesce(a.engagement_events, 0) as engagement_events,
        coalesce(a.open_support_cases, 0) as open_support_cases,
        coalesce(a.low_csat_cases, 0) as low_csat_cases
    from {{ source('identity', 'dim_customer_canonical') }} c
    left join orders o using (tenant_id, canonical_customer_id)
    left join activity a using (tenant_id, canonical_customer_id)
),
scored as (
    select
        *,
        greatest(
            0,
            least(
                100,
                55
                + least(engagement_events, 30)
                + least((gross_revenue / 50)::int, 20)
                - open_support_cases * 15
                - low_csat_cases * 20
                - refunded_orders * 10
            )
        )::int as health_score
    from features
)

select
    tenant_id,
    canonical_customer_id,
    health_score,
    (100 - health_score)::int as churn_risk_score,
    case
        when 100 - health_score >= 70 then 'high'
        when 100 - health_score >= 40 then 'medium'
        else 'low'
    end as churn_risk_band,
    {{ json_text_object([
        ('paid_orders', 'paid_orders'),
        ('refunded_orders', 'refunded_orders'),
        ('gross_revenue', 'gross_revenue'),
        ('engagement_events', 'engagement_events'),
        ('open_support_cases', 'open_support_cases'),
        ('low_csat_cases', 'low_csat_cases')
    ]) }} as feature_payload,
    {{ current_tstz() }} as calculated_at
from scored
