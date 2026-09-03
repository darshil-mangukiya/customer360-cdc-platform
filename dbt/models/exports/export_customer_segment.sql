{{ config(materialized='table') }}

select
    c.canonical_customer_id,
    c.tenant_id,
    c.business_unit,
    {{ pii_hash('c.primary_email') }} as email_sha256,
    {{ pii_hash('c.primary_phone') }} as phone_sha256,
    {{ current_tstz() }} as export_timestamp,
    case
        when c.total_revenue >= 250 then 'high_value'
        when c.total_orders = 0 then 'activation_needed'
        when c.business_unit = 'enterprise' then 'enterprise_growth'
        else 'self_serve_core'
    end as customer_segment,
    c.canonical_customer_id || ':mart_customer_360_current' as source_lineage_refs
from {{ ref('mart_customer_360_current') }} c
inner join {{ ref('mart_privacy_activation_eligibility') }} privacy
    on c.tenant_id = privacy.tenant_id
    and c.canonical_customer_id = privacy.canonical_customer_id
    and privacy.activation_eligible
