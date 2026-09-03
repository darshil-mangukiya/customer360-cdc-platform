select tenant_id, canonical_customer_id, subscription_id
from {{ ref('fct_subscription_history') }}
where is_current
group by tenant_id, canonical_customer_id, subscription_id
having count(*) > 1
