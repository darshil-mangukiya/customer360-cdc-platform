{{ config(materialized='table') }}

select
    c.canonical_customer_id,
    c.tenant_id,
    {{ pii_hash('c.primary_email') }} as email_sha256,
    {{ pii_hash('c.primary_phone') }} as phone_sha256,
    {{ current_tstz() }} as export_timestamp,
    c.lifecycle_stage,
    {{ current_tstz() }} as last_refresh_time
from {{ ref('mart_customer_360_current') }} c
inner join {{ ref('mart_privacy_activation_eligibility') }} privacy
    on c.tenant_id = privacy.tenant_id
    and c.canonical_customer_id = privacy.canonical_customer_id
    and privacy.activation_eligible
where c.lifecycle_stage is not null
