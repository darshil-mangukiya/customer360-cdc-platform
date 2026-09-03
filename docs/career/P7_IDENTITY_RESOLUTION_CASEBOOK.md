# P7 identity-resolution casebook

| Synthetic case | Evidence and decision | Golden-record result | Limitation |
| --- | --- | --- | --- |
| Straightforward merge | normalized email 0.86 | one tenant-scoped component | deterministic exact normalization |
| Multi-source merge | account/subscription/customer identifiers agree | fragmented records merge transitively | no probabilistic matching |
| Ambiguous match | shared review-band device/source reference | separate components plus review case | modeled reviewer only |
| False-merge prevention | two profiles share only weak evidence | no automatic merge | threshold 0.75 |
| Cross-tenant prevention | same email in tenant A and B | two different golden IDs | automated test; zero cross-tenant merge |
| False-split prevention | strong identifiers bridge multiple domains | one component | depends on supplied identifiers |
| Survivorship conflict | account and support emails differ | higher-priority/recent non-null wins; provenance retained | value masked in public evidence |
| Consent/deletion effect | identity remains auditable after revocation | activation suppressed without rewriting identity history | portfolio privacy-control model |

The formal expected-outcome matrix is `identity/validation/IDENTITY_RESOLUTION_MATRIX.csv`.
