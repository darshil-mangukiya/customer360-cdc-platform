# Real Kafka and Debezium Execution Evidence

Executed locally on 2026-08-21 with `.example.test` customer records.
Services: PostgreSQL 16 source, Debezium PostgreSQL connector 2.7.3.Final, Kafka
Connect, and Kafka 7.6.1. Connector and task both reported `RUNNING`.

## CRUD evidence

| Operation | Debezium op | Topic | Partition | Offset | PostgreSQL LSN | Before | After |
|---|---:|---|---:|---:|---:|---|---|
| INSERT | `c` | `cdc.source_customers_cdc_demo` | 0 | 0 | 26943784 | null | source row |
| UPDATE | `u` | `cdc.source_customers_cdc_demo` | 0 | 1 | 26944320 | lead/original email | active/updated email |
| DELETE | `d` | `cdc.source_customers_cdc_demo` | 0 | 2 | 26944728 | active row | null |

Each event also contained Debezium connector version, database/schema/table,
transaction ID, source timestamp, connector timestamp, and full before/after values.
The repository mapper converted `c/u/d` to `insert/update/delete` and persisted Kafka
topic, partition, offset, source LSN, and transaction ID in `raw.raw_cdc_events`.

## Restart and recovery

Kafka Connect was stopped after offset 2. A new source row was committed
while it was down. After Connect restarted, both connector and task returned to
`RUNNING`; the same consumer group received the new record at offset 3 and LSN
26947512. Warehouse verification showed one row for each offset 0, 1, 2, and 3—no
incorrect replay duplicates.

## P7 chain

A separate insert into `public.customers` produced `cdc.customers`
partition 0, offset 0, LSN 26949896. The corrected streaming consumer normalized and
landed it, persisted canonical identity `cc_270ba382052c`, and dbt built one matching
row in `mart_mart.mart_customer_360_current` for tenant `tenant_us`.

The runtime metadata above came from the PostgreSQL, Debezium, and Kafka services
listed at the start of this document.
