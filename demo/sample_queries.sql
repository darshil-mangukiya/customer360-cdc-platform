-- Customer 360 current serving view
select
    canonical_customer_id,
    tenant_id,
    business_unit,
    lifecycle_stage,
    current_subscription_status,
    total_revenue,
    health_score,
    churn_risk_band
from mart.mart_customer_360_current
order by churn_risk_score desc nulls last;

-- Point-in-time subscription state
select
    canonical_customer_id,
    subscription_id,
    plan_name,
    subscription_status,
    valid_from,
    valid_to
from mart.fct_subscription_history
where valid_from <= timestamp '2025-02-15'
  and coalesce(valid_to, timestamp '9999-12-31') > timestamp '2025-02-15';

-- Identity lineage for one customer
select
    canonical_customer_id,
    source_system,
    source_table,
    source_record_id,
    match_rule,
    match_confidence
from identity.customer_identity_map
order by canonical_customer_id, source_system;

-- Activation-ready churn export
select
    canonical_customer_id,
    tenant_id,
    churn_risk_score,
    churn_risk_band,
    last_refresh_time,
    source_lineage_refs
from activation.export_churn_risk
order by churn_risk_score desc;

-- Pipeline quality summary
select
    check_name,
    severity,
    status,
    failure_count,
    checked_at
from observability.quality_summary
order by checked_at desc, severity;

-- Pipeline health monitoring mart
select
    entity_name,
    total_events_seen,
    total_events_rejected,
    rejected_event_rate,
    freshness_status,
    pipeline_health_status
from mart.mart_pipeline_health
order by pipeline_health_status desc, rejected_event_rate desc;

-- Privacy-safe analyst mart
select
    canonical_customer_id,
    masked_email,
    masked_phone,
    lifecycle_stage,
    churn_risk_band
from mart.mart_customer_360_masked;
