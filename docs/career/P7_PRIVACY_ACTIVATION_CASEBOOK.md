# P7 privacy and activation casebook

| Scenario | Exact outcome |
| --- | --- |
| Eligible customer | explicit opt-in plus active channel and identifier → included in six governed exports |
| Suppressed customer | do-not-contact/opt-out → excluded before payload construction with reason code |
| Deleted customer | deletion request → future activation suppressed; audit/history policy remains distinct |
| Unknown consent | missing state → `missing_consent_state`; blocked fail-closed |
| Retryable simulator failure | bounded retry, same idempotency key, eventual success when the deterministic simulator recovers |
| Permanent simulator failure | one current validation failure stopped after one attempt and remained fully accounted |
| Replay | unchanged payload is skipped; no duplicate destination effect |

Current reconciliation: 48 candidate-dispositions across four simulator categories = 39 success + 8 suppression + 1 permanent failure + 0 unaccounted. This is not a legal-compliance certification or a real SaaS integration.
