{{ config(materialized='table') }}

with exports as (
    select 'export_customer_segment' as export_name, max(export_timestamp) as last_exported_at, count(*) as row_count
    from {{ ref('export_customer_segment') }}
    union all
    select 'export_churn_risk', max(export_timestamp), count(*)
    from {{ ref('export_churn_risk') }}
    union all
    select 'export_lifecycle_stage', max(export_timestamp), count(*)
    from {{ ref('export_lifecycle_stage') }}
    union all
    select 'export_customer_health_score', max(export_timestamp), count(*)
    from {{ ref('export_customer_health_score') }}
    union all
    select 'export_support_priority', max(export_timestamp), count(*)
    from {{ ref('export_support_priority') }}
    union all
    select 'export_campaign_target', max(export_timestamp), count(*)
    from {{ ref('export_campaign_target') }}
)

select
    export_name,
    last_exported_at,
    row_count,
    {{ minutes_since('last_exported_at') }} as export_lag_minutes,
    case
        when last_exported_at is null then 'missing'
        when ({{ minutes_since('last_exported_at') }}) > 180 then 'stale'
        else 'fresh'
    end as activation_freshness_status,
    current_timestamp as calculated_at
from exports

