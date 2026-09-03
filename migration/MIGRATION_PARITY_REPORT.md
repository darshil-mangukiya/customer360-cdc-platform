# Migration Parity Report

Evidence date: 2026-08-22. PostgreSQL and Snowflake were rebuilt from the same seed-42
seed-42 artifacts. Fixed-point values were normalized semantically across PostgreSQL
`NUMERIC` and Snowflake `NUMBER` before comparison.

| Dataset | Status | PostgreSQL rows | Snowflake rows | Missing | Extra | Mismatched | Snowflake query ID |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `canonical_customer` | **PASS** | 12 | 12 | 0 | 0 | 0 | `01c69058-0307-9797-0010-3a1a0001a516` |
| `customer_360` | **PASS** | 12 | 12 | 0 | 0 | 0 | `01c69058-0307-979f-0010-3a1a0001b416` |
| `subscription_history` | **PASS** | 19 | 19 | 0 | 0 | 0 | `01c69058-0307-97a5-0010-3a1a000192ce` |
| `lifecycle_history` | **PASS** | 19 | 19 | 0 | 0 | 0 | `01c69058-0307-979e-0010-3a1a000112f6` |
| `order_history` | **PASS** | 36 | 36 | 0 | 0 | 0 | `01c69058-0307-978f-0010-3a1a0001421e` |
| `activation_export` | **PASS** | 12 | 12 | 0 | 0 | 0 | `01c69058-0307-9793-0000-00103a1af5a5` |
| `suppression_output` | **PASS** | 2 | 2 | 0 | 0 | 0 | `01c69055-0307-97a0-0010-3a1a0001620e` |

Activation accounting for the comparable fixture is 12 export candidates, 2
suppressed customers, and 10 eligible customers. The PostgreSQL warehouse contained
pre-existing integration-test canonical rows; comparisons were correctly scoped to
the 12 canonical IDs in the shared fixture rather than presenting unrelated rows as a
Snowflake difference.

## Bugs resolved during reconciliation

- Bare `::numeric` became Snowflake `NUMBER(38,0)` and rounded half-unit monetary
  values. The shared models now use explicit `NUMBER/NUMERIC(38,6)` compatibility.
- CSV empty strings were normalized to SQL `NULL` before the final comparison.
- PostgreSQL-only `DISTINCT ON` was replaced by portable window ranking.

No unexplained parity differences remain in the compared fields.
