-- Live-verified 2026-08-22; the Task is returned to SUSPENDED after behavior tests.
-- noqa: disable=all -- Current SQLFluff Snowflake parsing does not support this triggered-task MERGE syntax.
-- One append/change Stream plus one triggered Task normalizes Kafka-landed CDC.
use role sysadmin;
use database c360;

create table if not exists staging.normalized_cdc_events (
    tenant_id string not null,
    source_system string not null,
    source_table string not null,
    record_primary_key string not null,
    event_id string not null,
    operation_type string not null,
    payload variant,
    source_commit_timestamp timestamp_tz,
    source_lsn string,
    kafka_topic string,
    kafka_partition number,
    kafka_offset number,
    event_hash string,
    is_deleted boolean not null default false,
    normalized_at timestamp_tz not null default current_timestamp(),
    primary key (tenant_id, source_system, source_table, record_primary_key)
);

create stream if not exists raw.raw_cdc_events_stream
on table raw.raw_cdc_events
show_initial_rows = false;

create task if not exists operations.normalize_raw_cdc_task
    warehouse = c360_wh
    when system$stream_has_data('c360.raw.raw_cdc_events_stream')
as
merge into staging.normalized_cdc_events as target
using (
    select * exclude (dedupe_rank)
    from (
        select
            tenant_id, source_system, source_table, record_primary_key, event_id,
            operation_type,
            iff(operation_type = 'delete', payload_before, payload_after) as payload,
            coalesce(source_commit_timestamp, event_timestamp) as source_commit_timestamp,
            source_lsn, kafka_topic, kafka_partition, kafka_offset, event_hash,
            operation_type = 'delete' as is_deleted,
            row_number() over (
                partition by tenant_id, source_system, source_table, record_primary_key
                order by coalesce(source_commit_timestamp, event_timestamp) desc,
                    kafka_offset desc, event_id desc
            ) as dedupe_rank
        from raw.raw_cdc_events_stream
        where metadata$action = 'INSERT'
    )
    where dedupe_rank = 1
) as source
on target.tenant_id = source.tenant_id
and target.source_system = source.source_system
and target.source_table = source.source_table
and target.record_primary_key = source.record_primary_key
when matched and source.source_commit_timestamp >= target.source_commit_timestamp then update set
    event_id = source.event_id,
    operation_type = source.operation_type,
    payload = source.payload,
    source_commit_timestamp = source.source_commit_timestamp,
    source_lsn = source.source_lsn,
    kafka_topic = source.kafka_topic,
    kafka_partition = source.kafka_partition,
    kafka_offset = source.kafka_offset,
    event_hash = source.event_hash,
    is_deleted = source.is_deleted,
    normalized_at = current_timestamp()
when not matched then insert (
    tenant_id, source_system, source_table, record_primary_key, event_id,
    operation_type, payload, source_commit_timestamp, source_lsn, kafka_topic,
    kafka_partition, kafka_offset, event_hash, is_deleted
) values (
    source.tenant_id, source.source_system, source.source_table,
    source.record_primary_key, source.event_id, source.operation_type,
    source.payload, source.source_commit_timestamp, source.source_lsn,
    source.kafka_topic, source.kafka_partition, source.kafka_offset,
    source.event_hash, source.is_deleted
);

-- Deliberately resume only after bootstrap validation and fixture load:
-- alter task operations.normalize_raw_cdc_task resume;
