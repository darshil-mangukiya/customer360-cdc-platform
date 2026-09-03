with exported as (
    select tenant_id, canonical_customer_id from {{ ref('export_customer_segment') }}
    union all
    select tenant_id, canonical_customer_id from {{ ref('export_churn_risk') }}
    union all
    select tenant_id, canonical_customer_id from {{ ref('export_lifecycle_stage') }}
    union all
    select tenant_id, canonical_customer_id from {{ ref('export_customer_health_score') }}
    union all
    select tenant_id, canonical_customer_id from {{ ref('export_support_priority') }}
    union all
    select tenant_id, canonical_customer_id from {{ ref('export_campaign_target') }}
)

select exported.*
from exported
left join {{ ref('mart_privacy_activation_eligibility') }} privacy
    on exported.tenant_id = privacy.tenant_id
    and exported.canonical_customer_id = privacy.canonical_customer_id
where coalesce(privacy.activation_eligible, false) = false
