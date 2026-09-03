select *
from {{ ref('fct_subscription_history') }}
where valid_to is not null
  and valid_from >= valid_to
