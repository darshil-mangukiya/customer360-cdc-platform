# Customer 360 interview guide

- Customer 360 is needed because account, billing, commerce, product, support, and consent records describe the same synthetic person differently.
- Identity nodes and edges are tenant-scoped. Identical email, phone, device, account, customer, or subscription references cannot connect across tenants.
- Exact external account/subscription evidence scores 0.99; customer+email 0.95; email+phone 0.92; email 0.86; phone 0.78. Automatic merge starts at 0.75.
- Device/source-reference evidence at 0.62 is review-band evidence. It opens a modeled stewardship case rather than changing authoritative identity.
- Union-Find builds transitive deterministic components. False merges are limited by strong-signal thresholds and tenant scope; false splits are reduced by multiple strong identifiers.
- Survivorship is deterministic: source priority, verification/completeness where available, recency, then stable tie-break. Null values do not displace non-null values.
- Field provenance records the source domain, record, observation time, and rule. Public evidence masks direct values.
- Current Customer 360 and event-derived subscription/lifecycle history are separate products. The bounded dbt run retained 19 subscription and 19 lifecycle history rows with zero overlaps and exactly one current version per active subscription.
- Eight current review cases are labeled modeled; no real steward or enterprise MDM adoption is claimed.
