with ordered as (
    select
        tenant_id,
        subscription_id,
        valid_from,
        valid_to,
        lead(valid_from) over (
            partition by tenant_id, subscription_id
            order by valid_from, subscription_history_sk
        ) as next_valid_from
    from {{ ref('fct_subscription_history') }}
)

select *
from ordered
where valid_to is not null
  and next_valid_from is not null
  and valid_to > next_valid_from
