{{ config(materialized='table') }}

select
    c.canonical_customer_id,
    c.tenant_id,
    {{ pii_hash('c.primary_email') }} as email_sha256,
    {{ pii_hash('c.primary_phone') }} as phone_sha256,
    {{ current_tstz() }} as export_timestamp,
    case
        when c.churn_risk_band = 'high' and c.open_support_cases > 0 then 'p1_retention'
        when c.support_cases_90d > 0 and c.health_score < 60 then 'p2_csat_recovery'
        else 'standard'
    end as support_priority,
    c.churn_risk_band,
    c.canonical_customer_id || ':mart_customer_360_current' as source_lineage_refs
from {{ ref('mart_customer_360_current') }} c
inner join {{ ref('mart_privacy_activation_eligibility') }} privacy
    on c.tenant_id = privacy.tenant_id
    and c.canonical_customer_id = privacy.canonical_customer_id
    and privacy.activation_eligible
