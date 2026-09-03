-- Snowflake bootstrap: 03_database_schemas.sql
-- Mirrors the Postgres schema layering (warehouse/sql/00_schemas.sql) using
-- Snowflake-conventional upper-case schema names. Layer -> Postgres schema mapping:
--
--   RAW           -> raw            (landed CDC events, rejected events)
--   IDENTITY      -> identity       (identity graph, canonical customer, merge audit)
--   STAGING       -> staging        (dbt staging views)
--   INTERMEDIATE  -> intermediate   (dbt intermediate views)
--   ANALYTICS     -> mart           (dbt marts: Customer 360, health, lifecycle history)
--   ACTIVATION    -> activation     (reverse ETL exports, sync logs, reconciliation)
--   GOVERNANCE    -> privacy        (consent, deletion requests, suppression, identity
--                                    stewardship review queue/survivorship rules)
--   OBSERVABILITY -> observability  (quality summaries, freshness, pipeline run log)
--   AUDIT         -> audit          (checkpoints, dedup/ordering logs, tenant config)
--
-- RAW is TRANSIENT: it is a replay-safe landing zone, not a system of record — Time
-- Travel/Fail-safe costs aren't worth paying for it. Everything downstream is
-- permanent (dbt's default) since marts/activation outputs are the actual deliverable.
--
-- Note on dbt-built schemas: dbt's default generate_schema_name behavior (unchanged
-- here, and identical on the Postgres target) writes model output to
-- "<profile schema>_<custom schema>", e.g. with SNOWFLAKE_SCHEMA=ANALYTICS the
-- staging/intermediate/marts/exports layers land in ANALYTICS_staging,
-- ANALYTICS_intermediate, ANALYTICS_mart, ANALYTICS_activation — dbt creates those
-- automatically on first run. The schemas below are for the layers Python code
-- (ingestion, identity resolution, privacy, observability) writes to directly, and
-- for the ANALYTICS default schema dbt targets before adding its per-layer suffix.

use role sysadmin;

create database if not exists c360
comment = 'Customer 360 CDC platform - Snowflake warehouse target';

use database c360;

create transient schema if not exists raw;
create schema if not exists identity;
create schema if not exists staging;
create schema if not exists intermediate;
create schema if not exists marts;
create schema if not exists history;
create schema if not exists analytics;
create schema if not exists activation;
create schema if not exists governance;
create schema if not exists observability;
create schema if not exists audit;
create schema if not exists operations;
