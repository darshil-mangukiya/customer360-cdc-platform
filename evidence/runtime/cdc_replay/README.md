# CDC failure and replay evidence

On 2026-09-03 the first bounded consumer attempt stopped before committing offsets because the source allowed a nullable `created_at` while the normalized contract did not. After nullability alignment, the next run quarantined 58 decimal-string records (`mrr` and `gross_amount`) instead of silently coercing arbitrary fields. The mapper was corrected to convert only the two contract-declared PostgreSQL decimal fields.

A clean validation database then consumed all 152 records with zero rejection. The three final ordering events were replayed by moving only consumer group `p7-2-six-domain-aligned` on `cdc.customers` partition 0 back by three offsets. PostgreSQL `ON CONFLICT (event_id) DO NOTHING` kept the persisted result at exactly three distinct events. No broad historical replay was used.

Replay boundary: one named consumer group, one topic, one partition, three records. Ordering boundary: one source key within one Kafka partition, using PostgreSQL LSN as the normalized sequence.
