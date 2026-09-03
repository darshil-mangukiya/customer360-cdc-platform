do $$
begin
    if not exists (select 1 from pg_roles where rolname = 'raw_loader') then
        create role raw_loader;
    end if;
    if not exists (select 1 from pg_roles where rolname = 'analytics_reader') then
        create role analytics_reader;
    end if;
    if not exists (select 1 from pg_roles where rolname = 'activation_sync') then
        create role activation_sync;
    end if;
    if not exists (select 1 from pg_roles where rolname = 'data_platform_admin') then
        create role data_platform_admin;
    end if;
end $$;

grant usage on schema raw, audit to raw_loader;
grant insert, select on raw.raw_cdc_events, raw.rejected_events to raw_loader;
grant insert, select on audit.ingestion_log, audit.watermark_checkpoint, audit.replay_run_log, audit.dlq_reprocess_log to raw_loader;
grant usage, select on all sequences in schema raw to raw_loader;
grant usage, select on all sequences in schema audit to raw_loader;

grant usage on schema mart, observability, activation to analytics_reader;
-- dbt models do not exist during first-time container initialization. Grant all
-- relations that exist now and set defaults for models created later, rather than
-- aborting bootstrap by naming not-yet-created mart tables.
grant select on all tables in schema mart to analytics_reader;
alter default privileges in schema mart grant select on tables to analytics_reader;
grant select on observability.quality_summary, observability.validation_failures, observability.pipeline_run_log, observability.freshness_status to analytics_reader;

grant usage on schema activation to activation_sync;
grant select on all tables in schema activation to activation_sync;
grant insert, update on activation.reverse_etl_sync_run_log, activation.reverse_etl_sync_failed_row to activation_sync;
grant usage, select on all sequences in schema activation to activation_sync;

grant all privileges on schema raw, audit, identity, staging, intermediate, mart, activation, observability to data_platform_admin;
grant all privileges on all tables in schema raw, audit, identity, staging, intermediate, mart, activation, observability to data_platform_admin;
grant all privileges on all sequences in schema raw, audit, identity, staging, intermediate, mart, activation, observability to data_platform_admin;
