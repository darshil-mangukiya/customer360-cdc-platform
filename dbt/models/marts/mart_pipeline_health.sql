{{ config(materialized='table') }}

with ingestion_ranked as (
    select
        *,
        row_number() over (partition by source_table order by load_end_time desc, batch_id desc) as rn
    from {{ source('audit', 'ingestion_log') }}
),
ingestion as (
    select
        source_table as entity_name,
        max(load_end_time) as last_successful_load_at,
        sum(event_count) as total_events_seen,
        sum(landed_count) as total_events_landed,
        sum(rejected_count) as total_events_rejected,
        max(case when rn = 1 then load_status end) as latest_load_status
    from ingestion_ranked
    group by 1
),
freshness_ranked as (
    select
        entity_name,
        lag_minutes,
        status as freshness_status,
        observed_at,
        row_number() over (partition by entity_name order by observed_at desc) as rn
    from {{ source('observability', 'freshness_status') }}
),
freshness as (
    select entity_name, lag_minutes, freshness_status, observed_at
    from freshness_ranked
    where rn = 1
)

select
    ingestion.entity_name,
    ingestion.last_successful_load_at,
    ingestion.total_events_seen,
    ingestion.total_events_landed,
    ingestion.total_events_rejected,
    round(ingestion.total_events_rejected::{{ decimal_type() }} / nullif(ingestion.total_events_seen, 0), 4) as rejected_event_rate,
    freshness.lag_minutes,
    freshness.freshness_status,
    case
        when coalesce(freshness.lag_minutes, 999999) > 360 then 'critical'
        when coalesce(ingestion.total_events_rejected, 0)::{{ decimal_type() }} / nullif(ingestion.total_events_seen, 0) > 0.05 then 'warning'
        else 'healthy'
    end as pipeline_health_status,
    current_timestamp as calculated_at
from ingestion
left join freshness using (entity_name)
