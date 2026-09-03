select *
from {{ ref('fct_subscription_history') }}
where is_delete and is_current
