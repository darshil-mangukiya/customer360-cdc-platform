{% snapshot snap_subscription_state %}

{{
    config(
      target_schema='mart',
      unique_key='tenant_source_subscription_key',
      strategy='timestamp',
      updated_at='source_updated_at',
      invalidate_hard_deletes=True
    )
}}

select
    {{ surrogate_key(["tenant_id", "source_system", "source_subscription_id"]) }} as tenant_source_subscription_key,
    tenant_id,
    source_system,
    source_subscription_id,
    subscription_id,
    customer_id,
    external_account_id,
    email,
    plan_name,
    subscription_status,
    billing_period,
    mrr,
    start_date,
    cancel_at,
    source_updated_at
from {{ ref('int_latest_subscriptions') }}

{% endsnapshot %}
