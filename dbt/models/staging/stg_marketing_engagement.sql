{{ config(materialized='view') }}

select
    event_id,
    source_system,
    record_primary_key as source_marketing_touch_id,
    operation_type,
    event_timestamp,
    event_sequence_number,
    source_lsn,
    batch_id,
    {{ json_field('payload', 'marketing_touch_id') }} as marketing_touch_id,
    {{ json_field('payload', 'tenant_id') }} as tenant_id,
    {{ json_field('payload', 'business_unit') }} as business_unit,
    nullif(lower({{ json_field('payload', 'email') }}), '') as email,
    {{ json_field('payload', 'external_account_id') }} as external_account_id,
    {{ json_field('payload', 'channel') }} as channel,
    {{ json_field('payload', 'campaign_id') }} as campaign_id,
    {{ json_field('payload', 'engagement_status') }} as engagement_status,
    {{ json_field('payload', 'marketing_consent_status') }} as marketing_consent_status,
    coalesce(nullif({{ json_field('payload', 'email_opt_in') }}, '')::boolean, false) as email_opt_in,
    coalesce(nullif({{ json_field('payload', 'sms_opt_in') }}, '')::boolean, false) as sms_opt_in,
    coalesce(nullif({{ json_field('payload', 'push_opt_in') }}, '')::boolean, false) as push_opt_in,
    {{ json_field('payload', 'unsubscribe_status') }} as unsubscribe_status,
    coalesce(nullif({{ json_field('payload', 'do_not_contact_flag') }}, '')::boolean, false) as do_not_contact_flag,
    nullif({{ json_field('payload', 'lead_score') }}, '')::int as lead_score,
    nullif({{ json_field('payload', 'occurred_at') }}, '')::{{ tstz_type() }} as occurred_at,
    nullif({{ json_field('payload', 'updated_at') }}, '')::{{ tstz_type() }} as source_updated_at,
    is_delete
from {{ ref('stg_cdc_events') }}
where source_table = 'marketing_engagement'
