# Warehouse Design

The warehouse is layered into raw, staging, intermediate, mart, activation, audit, identity, and observability schemas.

## Raw

`raw.raw_cdc_events` stores immutable normalized CDC envelopes. The raw table is indexed by source table/timestamp, batch, and source record key to support incremental dbt models and point-in-time reconstruction.

## Staging

dbt staging models parse JSONB payloads into typed columns:

- `stg_customers`
- `stg_subscriptions`
- `stg_orders`
- `stg_engagement_events`
- `stg_support_interactions`
- `stg_marketing_engagement`

Staging models preserve operation flags and event timestamps.

## Intermediate

Intermediate models handle reusable transformation patterns:

- latest-record logic
- deduplicated current source views
- activity rollups
- identity-enriched joins through `identity.customer_identity_map`
- tenant-aware partitioning and joins for source records that may share identifiers across tenants

## Marts

Marts provide trusted business entities:

- `fct_order_history`
- `fct_subscription_history`
- `mart_customer_360_current`
- `mart_customer_lifecycle_history`
- `mart_customer_health`

Key fact and history marts carry `tenant_id` so order, subscription, lifecycle, health, and Customer 360 outputs can be filtered and tested at the tenant grain.

## SCD Type 2 and Snapshots

Subscription history is modeled with `valid_from`, `valid_to`, `is_current`,
`effective_timestamp`, `source_event_id`, `change_reason`, and delete state. dbt
snapshots use tenant/source-scoped keys so identical source IDs cannot collide across
tenants. Singular tests enforce current-row, overlap, validity, and delete invariants.

## Point-in-Time Analysis

For point-in-time customer state, query the relevant history model with:

```sql
where valid_from <= :as_of_timestamp
  and coalesce(valid_to, '9999-12-31') > :as_of_timestamp
```

This makes churn, lifecycle, and plan-change analysis reproducible instead of dependent on current-state overwrites.

## Indexing and Retention Notes

Recommended production indexes:

- raw CDC: `(tenant_id, source_system, record_primary_key, event_timestamp desc)`
- raw CDC: `(tenant_id, batch_id, source_system, source_table)`
- identity map: `(tenant_id, source_system, source_record_id)` and `(canonical_customer_id)`
- marts: tenant/customer keys and effective-date windows

Local dbt parsing is runnable without Postgres. Warehouse-backed `dbt test` runs against the Docker/Postgres profile after the `c360` role/database setup is initialized.

### Snowflake as a second warehouse target

The same model tree executes on the configured `snowflake` dbt target
(`dbt/profiles.yml`) while PostgreSQL remains the default. JSON extraction, timestamp
types, explicit fixed-point numerics, hashing, conditional counts, JSON object
serialization, freshness math, and interval arithmetic use adapter-aware paths. On
2026-08-22, dbt Core 1.11.11 and Snowflake adapter 1.11.6 passed live connectivity,
compile, 28/28 models, 2/2 snapshots, and 56/56 tests. Seven compared datasets then
matched PostgreSQL with zero key, row, or value differences.

Retention strategy:

- retain raw CDC long-term in object storage
- keep warehouse raw partitions for 12-24 months
- compact older raw events into monthly Parquet/iceberg-style history
- retain SCD and activation audit tables according to customer data policy
