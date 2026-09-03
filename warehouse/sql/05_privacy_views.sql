create or replace view mart.mart_customer_360_masked as
select
    canonical_customer_id,
    tenant_id,
    business_unit,
    case
        when primary_email is null then null
        else left(split_part(primary_email, '@', 1), 1) || '***@' || split_part(primary_email, '@', 2)
    end as masked_email,
    case
        when primary_phone is null then null
        else left(primary_phone, 3) || '***' || right(primary_phone, 4)
    end as masked_phone,
    lifecycle_stage,
    current_plan_name,
    current_subscription_status,
    total_orders,
    total_revenue,
    engagement_events_30d,
    support_cases_90d,
    open_support_cases,
    health_score,
    churn_risk_score,
    churn_risk_band,
    last_activity_at,
    updated_at
from mart.mart_customer_360_current;

