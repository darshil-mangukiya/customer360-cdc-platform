{{ config(materialized='table') }}

with segments as (
    select * from {{ ref('export_customer_segment') }}
),
base as (
    select
        c.canonical_customer_id,
        c.tenant_id,
        c.business_unit,
        c.primary_email,
        c.primary_phone,
        c.churn_risk_band,
        s.customer_segment
    from {{ ref('mart_customer_360_current') }} c
    left join segments s using (tenant_id, canonical_customer_id)
    inner join {{ ref('mart_privacy_activation_eligibility') }} privacy
        on c.tenant_id = privacy.tenant_id
        and c.canonical_customer_id = privacy.canonical_customer_id
        and privacy.activation_eligible
)

select
    canonical_customer_id,
    tenant_id,
    business_unit,
    {{ pii_hash('primary_email') }} as email_sha256,
    {{ pii_hash('primary_phone') }} as phone_sha256,
    {{ current_tstz() }} as export_timestamp,
    case
        when churn_risk_band = 'high' then 'save_offer_or_customer_success_outreach'
        when customer_segment = 'activation_needed' then 'onboarding_activation'
        when customer_segment = 'high_value' then 'expansion_nurture'
        else 'product_education'
    end as campaign_target,
    customer_segment,
    churn_risk_band
from base
