# Snowflake interview guide

PostgreSQL is P7's primary local operational/reference execution plane. Snowflake is a separately validated analytical plane; P7 does not claim continuous production replication between them.

The 2026-08-22 recorded run executed the then-current shared graph: 28 models, 2 snapshots, and 56 tests. It also validated a Stream for Snowflake-local change tracking, a Triggered Task for dependent SQL execution, and a Dynamic Table for managed refresh. These are not substitutes for Debezium/Kafka or Airflow.

Governance evidence includes a seven-role hierarchy, row access by tenant, email/phone masking, four classification tags, an X-Small auto-suspending warehouse, a resource monitor, and tagged query history. Seven PostgreSQL/Snowflake datasets had zero missing, extra, or mismatched values.

P7 2.0 added a 29th dbt privacy model and six tests. It parses for Snowflake, but no credentials were used and it has not been live-executed there. Safe wording is “recorded Snowflake live validation” and “recorded zero-mismatch parity validation,” never “current production Snowflake workload.”
