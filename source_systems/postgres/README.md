# Operational Postgres CDC Source

This folder defines the operational PostgreSQL source used by the executed local
Debezium path. The offline smoke pipeline remains deterministic with JSONL CDC; the
Docker path separately emits genuine WAL changes through logical replication.

## Tables

- `public.customers`
- `public.subscriptions`
- `public.orders`
- `public.engagement_events`
- `public.support_interactions`
- `public.marketing_engagement`
- `public.source_customers_cdc_demo` (CRUD verification table)

The init SQL enables `replica identity full` and creates `customer360_publication` for Debezium.

## Local Flow

```bash
docker compose up -d source_postgres zookeeper kafka kafka-connect
make source-seed
make debezium-register
```

The Debezium connector routes these tables into `cdc.*` topics. Local verification
captured create/update/delete records with real Kafka offsets and PostgreSQL LSNs,
then resumed a change made while Kafka Connect was stopped. See
`docs/proof/real_kafka_debezium_execution.md`.
