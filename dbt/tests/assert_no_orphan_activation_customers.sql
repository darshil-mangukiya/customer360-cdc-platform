select e.canonical_customer_id
from {{ ref('export_churn_risk') }} e
left join {{ source('identity', 'dim_customer_canonical') }} c
    on c.tenant_id = e.tenant_id
    and c.canonical_customer_id = e.canonical_customer_id
where c.canonical_customer_id is null
