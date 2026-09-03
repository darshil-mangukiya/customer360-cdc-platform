# Privacy and activation interview guide

- dbt owns the governed analytical eligibility state. Python independently enforces the final destination export gate and fails closed.
- Eligibility requires an explicit `opted_in` state, at least one active channel, no unsubscribe, no do-not-contact, no deletion request, a live consent source record, and a contact identifier.
- Missing or unknown consent is suppressed. A deletion request immediately removes future activation eligibility; historical CDC/audit retention remains a separate project policy.
- Six activation-ready outputs serve campaign, segment, churn, lifecycle, health, and support use cases.
- Stable record keys, payload hashes, and idempotency keys prevent duplicate simulator effects. Changed payloads become updates; unchanged payloads become skips.
- Retryable 429/5xx-style outcomes use a maximum of three attempts. Contract/privacy/validation failures are permanent and do not loop forever.
- Reconciliation accounts for success, suppression, retryable pending, permanent failure, skip, and duplicate. The current simulator accounting has zero unaccounted candidates.
- All four destinations are simulators. Safe wording: activation pipelines, activation-ready exports, or reverse-ETL-style destination simulation. No real vendor delivery or legal certification is claimed.
