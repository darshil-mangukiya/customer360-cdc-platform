{% snapshot snap_customer_state %}

{{
    config(
      target_schema='mart',
      unique_key='tenant_source_customer_key',
      strategy='timestamp',
      updated_at='source_updated_at',
      invalidate_hard_deletes=True
    )
}}

select
    {{ surrogate_key(["tenant_id", "source_system", "source_customer_id"]) }} as tenant_source_customer_key,
    source_customer_id,
    customer_id,
    email,
    phone,
    external_account_id,
    first_name,
    last_name,
    tenant_id,
    business_unit,
    customer_status,
    source_updated_at
from {{ ref('int_latest_customers') }}

{% endsnapshot %}
