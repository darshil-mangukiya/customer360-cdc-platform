# Master Data Management and Identity Stewardship

Formalizes what this project actually implements of "master data management" for
customer identity — golden record, canonical ID, survivorship, stewardship,
merge/unmerge — and is explicit about what a real enterprise MDM suite adds on top.

## Golden customer record

The **canonical customer** (`identity.dim_customer_canonical`, keyed by
`canonical_customer_id`) is the golden record: one row per real-world customer,
survivor-selected from every linked source record. It is produced by
`identity_resolution.resolver.resolve_identity` and is not itself editable by
end users — it is recomputed deterministically from source CDC events plus the
current survivorship rules every time identity resolution runs.

## Canonical customer ID

`canonical_customer_id` (format `cc_<12-hex>`) is a deterministic SHA-1 hash of the
tenant plus the strongest identifier token in the record's component (see
`resolver._canonical_id`). Determinism matters here: re-running resolution on the
same input always produces the same canonical ID for the same person, which is what
makes the review queue, reconciliation, and lineage artifacts reproducible across
runs rather than randomly re-keyed each time.

## Source-to-canonical mapping

`identity.customer_identity_map` is the crosswalk: one row per
`(tenant_id, source_system, source_record_id)` pointing at the
`canonical_customer_id` it resolved to, with `match_rule` and `match_confidence`
recorded per row — so the mapping itself explains why it exists, not just what it is.

## Survivorship

Formal, testable survivorship rules live in
`identity_resolution.stewardship.survivorship_rules()` (also mirrored, for querying,
in `identity.identity_survivorship_rule`). Each rule documents: authoritative source,
fallback source, tie-breaking rule, null behavior, conflict behavior, and privacy
behavior. Two points worth calling out:

- **Marketing consent never overrides account identity**, and identity merges never
  grant activation eligibility a suppressed record didn't already have — consent
  state is governed independently by `privacy.activation_policy`, not by the identity
  graph.
- A **field disagreement doesn't get silently discarded**. When two merged records
  disagree on email or phone, the surviving value wins the canonical record *and* the
  losing value opens a `conflicting_email`/`conflicting_phone` review case — the
  conflict stays visible to a human, it isn't just resolved and forgotten.

## Identity stewardship (review queue)

Not every match is strong enough to auto-merge, and the resolver no longer pretends
otherwise. `identity_resolution.config.AUTO_MERGE_MIN` / `REVIEW_MIN` split resolver
evidence into three bands:

| Confidence | Behavior |
|---|---|
| `>= AUTO_MERGE_MIN` (0.75) | Auto-merges through the canonical resolver |
| `REVIEW_MIN` (0.55) to `AUTO_MERGE_MIN` | **Not merged.** Routed to `identity.identity_review_queue` as a candidate for a human decision |
| `< REVIEW_MIN` | Too weak to act on; records stay separate, no review case |

Weak-link candidates (e.g. two customers sharing only a `device_id`) and
already-merged survivorship conflicts (e.g. two disagreeing emails behind one strong
`external_account_id`) both land in the same review queue with a `conflict_type`,
confidence score, and evidence summary explaining the specific evidence.

Review cases follow an explicit status machine — `OPEN -> IN_REVIEW ->
APPROVED/REJECTED -> RESOLVED` — enforced in code
(`identity_resolution.config.VALID_STATUS_TRANSITIONS`,
`stewardship.apply_decision`) and over the API
(`POST /identity/review/{review_case_id}/decision`). An illegal transition (e.g.
deciding on an already-`RESOLVED` case) is rejected, not silently accepted.

## Merge / unmerge (reversibility)

`identity_resolution.stewardship.merge_customers` and `.unmerge_customer` give a
steward-approved way to correct the graph after the fact:

- **Merge** reassigns every mapping row from a source canonical ID to a target one,
  refuses cross-tenant merges outright, and writes an `identity.identity_merge_audit`
  row.
- **Unmerge** splits exactly one `(source_system, source_record_id)` mapping out of a
  canonical customer into a brand-new canonical ID, writes an
  `identity.identity_unmerge_audit` row, and — this is the property the test suite
  checks — never touches any other mapping row, for that canonical customer or any
  other.

## Explainability

Every important identity decision is reconstructable from repository data alone:

- **Why did two records become one canonical customer?** —
  `identity.identity_link_explanation` (`match_rule`, `match_confidence`,
  human-readable `explanation_text`) plus `identity.identity_merge_event`.
- **Why is this pair *not* merged?** — the corresponding
  `identity.identity_review_queue` row's `evidence_summary` names the shared
  identifier, its confidence, and the auto-merge threshold it fell short of.
- **Why did this field survive over that one?** —
  `identity_resolution.stewardship.survivorship_rules()`.
- **Who approved a merge/unmerge, and why?** — `identity_merge_audit` /
  `identity_unmerge_audit`, both carrying `reviewer` and `reason`.

## Explainable fuzzy candidates and field-level Golden Customer

`identity_resolution.fuzzy` adds tenant-scoped blocking and explicit name, address,
email, phone, verification, source-trust, and recency signals. A verified exact
identifier can produce `AUTO_MATCH`; fuzzy-only evidence is restricted to `REVIEW`
or `NO_MATCH`. Labeled evaluation reports false-positive and false-negative merge
recommendations rather than presenting an unmeasured accuracy claim.

`identity_resolution.golden_record` is the explicit field-level Golden Customer
builder. Each winning field retains source record, source system, observation time,
and rule provenance. Audited manual overrides outrank automated survivorship, while
cross-tenant inputs and overrides fail closed. This package complements the existing
canonical resolver; it does not silently replace stored canonical mappings.

`identity_resolution.ai_steward` exposes a strict, human-review-only provider
interface. The executable provider is deterministic/offline for CI. It allowlists
masked evidence and cannot merge, unmerge, change consent, or activate a customer.
Snowflake Cortex is **ACCOUNT-LIMITED**. A guarded call using masked fixture
evidence was attempted on 2026-08-22 after granting `SNOWFLAKE.CORTEX_USER`; the trial
returned error 399258 stating that `COMPLETE` is unavailable for trial accounts. No
recommendation or merge was produced.

## Runtime scope

The identity graph is customer-domain only. Stewardship decisions use the API or CSV
artifacts, and the fuzzy scorer is deterministic with conservative thresholds. Case
assignment, escalation routing, and tenant-configurable survivorship policies belong
to the operating environment around this component.

## Related docs

- `docs/identity_resolution.md` — the deterministic matching layer itself.
- `snowflake/README.md` / `docs/warehouse_modeling.md` — where these tables live in
  the warehouse.
