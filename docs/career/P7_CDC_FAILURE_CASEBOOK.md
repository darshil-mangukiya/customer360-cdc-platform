# P7 CDC failure casebook

All cases use synthetic project data.

1. **Source/contract nullability.** A live customer snapshot contained nullable `created_at`; the contract rejected it before offset commit. Source DDL and contract were aligned, then the same group replayed safely.
2. **Debezium decimal representation.** `mrr` and `gross_amount` arrived as strings by configured design. Fifty-eight records were quarantined. The normalizer now converts only those two declared decimal fields; the clean 152-event replay had zero rejection.
3. **Duplicate replay.** The ordering consumer moved back exactly three offsets. Six processing attempts yielded three distinct persisted event IDs because PostgreSQL landing is idempotent.
4. **Out-of-order risk.** Rapid lead→active→inactive changes received increasing LSNs 27854784, 27858536, 27858952 and offsets 18–20. LSN is now the normalized sequence; final state was inactive.
5. **Breaking contract.** The Airflow gate found seven breaking changes and failed with retry state. The current contract rerun found zero breaking changes and succeeded.
6. **Ambiguous identity.** Weak/shared evidence never auto-merges; it creates a modeled review case. Eight attribute-conflict cases were materialized in the current deterministic run.
7. **Consent and activation failures.** Two do-not-contact customers were suppressed before export; simulators then exercised four retries and one permanent validation failure. Reconciliation variance remained zero.
