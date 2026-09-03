{{ config(materialized='table') }}

with customer_base as (
    select * from {{ ref('mart_customer_360_current') }}
),
raw_counts as (
    select
        count(*) as raw_event_count
    from {{ source('raw', 'raw_cdc_events') }}
),
rejected as (
    select
        count(*) as rejected_event_count
    from {{ source('raw', 'rejected_events') }}
),
activation as (
    select
        {{ conditional_count("activation_freshness_status = 'fresh'") }} as fresh_activation_exports,
        {{ conditional_count("activation_freshness_status <> 'fresh'") }} as non_fresh_activation_exports
    from {{ ref('mart_activation_freshness') }}
),
sync_health as (
    select
        {{ conditional_count("sync_health_status = 'healthy'") }} as healthy_sync_destinations,
        {{ conditional_count("sync_health_status <> 'healthy'") }} as unhealthy_sync_destinations
    from {{ ref('mart_reverse_etl_sync_health') }}
)

select 'active_customers' as metric_name, count(*)::{{ decimal_type() }} as metric_value, 'customer' as metric_grain, current_timestamp as measured_at, 'Customers currently in active lifecycle state' as metric_description
from customer_base
where lifecycle_stage = 'active_customer'
union all
select 'high_churn_risk_rate', avg(case when churn_risk_band = 'high' then 1 else 0 end)::{{ decimal_type() }}, 'customer', current_timestamp, 'Share of customers in high churn risk band'
from customer_base
union all
select 'avg_customer_health_score', avg(health_score)::{{ decimal_type() }}, 'customer', current_timestamp, 'Average customer health score'
from customer_base
union all
select 'rejected_event_rate', rejected_event_count::{{ decimal_type() }} / nullif(raw_event_count + rejected_event_count, 0), 'event', current_timestamp, 'Rejected CDC events divided by all observed CDC events'
from raw_counts cross join rejected
union all
select 'fresh_activation_export_count', fresh_activation_exports::{{ decimal_type() }}, 'export', current_timestamp, 'Number of activation exports currently fresh'
from activation
union all
select 'unhealthy_sync_destination_count', unhealthy_sync_destinations::{{ decimal_type() }}, 'destination', current_timestamp, 'Reverse ETL destinations not currently healthy'
from sync_health
