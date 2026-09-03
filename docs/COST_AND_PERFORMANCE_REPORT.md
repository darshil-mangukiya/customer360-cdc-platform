# Snowflake Cost and Performance Report

## Status

**EXECUTED** on 2026-08-22 using the project `C360_WH` X-Small warehouse and the
controlled seed-42 workload. Cached results were disabled for the measured statements.

| Query | Query ID | Total elapsed | Execution | Compilation | Bytes scanned | Rows produced |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Customer 360 by tenant | `01c69063-0307-9793-0000-00103a1af615` | 90 ms | 18 ms | 72 ms | 3,072 | 4 |
| Point-in-time subscription SCD | `01c69062-0307-9797-0010-3a1a0001a566` | 161 ms | 35 ms | 126 ms | 4,096 | 1 |
| Activation eligibility | `01c69062-0307-97a1-0010-3a1a00010282` | 227 ms | 59 ms | 168 ms | 4,608 | 1 |
| CDC normalization summary | `01c69062-0307-9797-0010-3a1a0001a56a` | 122 ms | 17 ms | 105 ms | 29,696 | 6 |

The single Dynamic Table refreshed incrementally to 133 non-deleted rows. Its live
metadata reported `TARGET_LAG = 15 minutes`, warehouse `C360_WH`, and 192 seconds of
observed freshness after a manual verification refresh.

## Cost controls and measured usage

- `C360_WH`: X-Small, 60-second auto-suspend, auto-resume enabled.
- Final metadata check: warehouse state `SUSPENDED`.
- `C360_MONTHLY_GUARDRAIL`: 25-credit monthly resource monitor attached, with the
  prepared notification/suspension thresholds.
- Live warehouse metering returned 0.403779166 total credits for the 09:00–10:00
  account hour: 0.377534722 compute and 0.026244444 cloud-services credits.

That hourly figure covers bootstrap, fixture loading, repeated debugging builds,
governance tests, Stream/Task and Dynamic Table verification—not just the four queries
above. It is reported as observed run-hour usage rather than allocated per query or
converted to a dollar estimate.
