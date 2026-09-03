# PostgreSQL → Snowflake Migration Lab

Status: **completed** on 2026-08-22. Seven PostgreSQL-to-Snowflake comparisons passed
with no missing, extra, or mismatched values.

The migration is a parallel-run cutover, not a big-bang rewrite:

1. Bootstrap Snowflake infrastructure and roles independently of dbt-owned objects.
2. Load the same seed-42 fixture into PostgreSQL and Snowflake RAW.
3. Run the shared dbt tree on both targets.
4. Export sorted JSON snapshots at the documented business grain.
5. Compare row counts, keys, and canonicalized values with `migration.parity`.
6. Require PASS for Customer 360, history, identity quality, and activation before cutover.
7. Freeze activation writes, switch readers, reconcile again, then enable activation.

Rollback keeps PostgreSQL authoritative, disables Snowflake activation, restores the
previous reader connection, and replays CDC from preserved Kafka offsets. No source
write is dual-authoritative.

Source-to-target mappings are in `source_to_target_mapping.csv`. Snowflake `VARIANT`
replaces PostgreSQL `JSONB`, `TIMESTAMP_TZ` replaces timezone-aware timestamps, and
dbt adapter-dispatched macros handle hashing, date arithmetic, and JSON extraction.

Run the offline source-side check:

```bash
make migration-parity
```

Supply `--target <snowflake-export.json>` after exporting a Snowflake dbt run. Without
a target, the command reports `NOT_RUN` for cross-warehouse comparison.
