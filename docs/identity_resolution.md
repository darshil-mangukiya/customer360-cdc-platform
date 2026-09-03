# Customer Identity Resolution

The identity layer creates canonical customers from source records across account, billing, commerce, product engagement, support, and marketing systems.

## Matching Strategy

The resolver builds an identity graph. Source records are connected to normalized identifier tokens:

- `external_account_id`
- `subscription_id`
- `customer_id`
- normalized email
- normalized phone
- `device_id`
- `order_customer_ref`
- `support_customer_ref`

Connected components become canonical customers. The canonical customer ID is deterministic from the strongest identifier in the component.

## Survivorship

Canonical attributes are selected with a source priority and latest non-null strategy:

1. account system
2. billing system
3. commerce system
4. support system
5. marketing system
6. product engagement system

This gives account and billing records authority for stable attributes, while still allowing other systems to contribute linkage evidence.

## Outputs

- `identity.dim_customer_canonical`
- `identity.customer_identity_map`
- `identity.identity_resolution_audit`
- `identity.identity_review_queue`, `identity.identity_survivorship_rule`, `identity.identity_merge_audit`, `identity.identity_unmerge_audit` (stewardship, see below)
- local CSV equivalents under `identity_resolution/output/`

## Identity stewardship (review queue, merge/unmerge)

Record unions require threshold-qualified evidence. Matching confidence is split into
an auto-merge band and a review band
(`identity_resolution.config.AUTO_MERGE_MIN` / `REVIEW_MIN`); evidence below the
auto-merge threshold — a shared `device_id`, an unrecognized order/support customer
reference, or a strong merge whose linked records disagree on email/phone — is routed
to `identity.identity_review_queue` instead of being silently merged or discarded.
Reviewers apply decisions (`APPROVE_MERGE`, `REJECT_MERGE`, `NEEDS_REVIEW`,
`APPROVE_UNMERGE`, `IGNORE_FALSE_POSITIVE`) through an explicit status state machine,
and merges/unmerges are reversible with a full audit trail. Full detail:
`docs/master_data_management.md`; implementation: `identity_resolution/stewardship.py`.

## Matching scope

This implementation uses deterministic matching for explainability and repeatable
local tests. A conservative confidence gate defers ambiguous evidence to the review
queue; a trained probabilistic matcher is outside this component.
