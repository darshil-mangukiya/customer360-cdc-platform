with eligible_health as (
    select count(*) as row_count
    from {{ ref('mart_customer_health') }} health
    inner join {{ ref('mart_privacy_activation_eligibility') }} privacy
        on health.tenant_id = privacy.tenant_id
        and health.canonical_customer_id = privacy.canonical_customer_id
        and privacy.activation_eligible
),
exports as (
    select 'export_churn_risk' as export_name, count(*) as row_count from {{ ref('export_churn_risk') }}
    union all
    select 'export_customer_health_score', count(*) from {{ ref('export_customer_health_score') }}
    union all
    select 'export_lifecycle_stage', count(*) from {{ ref('export_lifecycle_stage') }}
    union all
    select 'export_support_priority', count(*) from {{ ref('export_support_priority') }}
    union all
    select 'export_campaign_target', count(*) from {{ ref('export_campaign_target') }}
    union all
    select 'export_customer_segment', count(*) from {{ ref('export_customer_segment') }}
)

select e.*
from exports e
cross join eligible_health h
where e.row_count <> h.row_count
