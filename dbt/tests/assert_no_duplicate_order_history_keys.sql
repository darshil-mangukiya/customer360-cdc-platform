select tenant_id, order_id
from {{ ref('fct_order_history') }}
group by tenant_id, order_id
having count(*) > 1
