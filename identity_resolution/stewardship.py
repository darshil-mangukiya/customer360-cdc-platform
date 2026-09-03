"""Identity stewardship: review queue, survivorship rules, and merge/unmerge audit.

The deterministic identity graph in `identity_resolution.resolver` auto-merges on
strong evidence (confidence >= `config.AUTO_MERGE_MIN`). This module adds a second,
non-destructive analysis pass over the resolver's own
output that:

1. Flags **ambiguous candidate merges** — pairs of canonical customers in the same
   tenant that share only a weak identifier (device_id, or an unrecognized
   order/support customer reference) whose confidence falls in the review band
   (`REVIEW_MIN` <= confidence < `AUTO_MERGE_MIN`). These are never auto-merged; they
   are written to a review queue for a human decision.
2. Flags **survivorship conflicts** — canonical customers that already merged on a
   strong identifier (e.g. `external_account_id`) but whose linked source records
   disagree on email or phone. The merge already happened (the strong identifier
   earns that), but the disagreement is still stewardship-worthy.
3. Provides an explicit, testable status/decision state machine
   (`apply_decision`) and reversible merge/unmerge operations with an audit trail.

Every function here is pure/deterministic given the same resolver output, so review
queue contents are reproducible across runs — the same input CDC batch always
produces the same review cases.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from identity_resolution import config
from identity_resolution.resolver import (
    CanonicalCustomer,
    IdentityMapRow,
    WeakLinkCandidate,
    _latest_records,
    _tenant_scope,
    load_events,
    normalize_email,
    normalize_phone,
    resolve_identity,
    resolve_identity_with_review_candidates,
)


def _now() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _case_id(*parts: str) -> str:
    seed = "|".join(parts)
    return f"revcase_{hashlib.sha1(seed.encode('utf-8')).hexdigest()[:16]}"


@dataclass
class ReviewCase:
    review_case_id: str
    tenant_id: str | None
    canonical_customer_id: str
    candidate_customer_id: str
    source_system: str
    source_customer_id: str
    conflict_type: str
    match_rule: str
    confidence_score: float
    evidence_summary: str
    current_status: str
    created_at: str
    updated_at: str
    resolved_at: str | None
    reviewer: str | None
    decision: str | None
    decision_reason: str | None
    survivorship_rule: str | None
    source_event_id: str | None


@dataclass(frozen=True)
class IdentityMergeAudit:
    merge_audit_id: str
    tenant_id: str | None
    review_case_id: str | None
    source_canonical_customer_id: str
    target_canonical_customer_id: str
    reviewer: str
    reason: str
    occurred_at: str


@dataclass(frozen=True)
class IdentityUnmergeAudit:
    unmerge_audit_id: str
    tenant_id: str | None
    original_canonical_customer_id: str
    new_canonical_customer_id: str
    source_system: str
    source_record_id: str
    reviewer: str
    reason: str
    occurred_at: str


@dataclass(frozen=True)
class SurvivorshipRule:
    field: str
    authoritative_source: str
    fallback_source: str
    tie_breaking_rule: str
    null_behavior: str
    conflict_behavior: str
    privacy_behavior: str


def survivorship_rules() -> list[SurvivorshipRule]:
    """The formal, testable survivorship rule table (spec section 14).

    Mirrors what `resolver._best_value` actually does (source priority, then most
    recent non-null wins) and documents the fields it does *not* cover today
    (privacy/consent fields are governed separately in `privacy.activation_policy`
    and must never be overridden by an identity merge).
    """
    return [
        SurvivorshipRule(
            field="email",
            authoritative_source="highest SOURCE_PRIORITY source with a non-null value",
            fallback_source="most recently updated non-null value across sources",
            tie_breaking_rule="most recent event_timestamp wins",
            null_behavior="null values are skipped, never overwrite a known value",
            conflict_behavior="surviving value wins the canonical record; the losing "
            "value(s) generate a conflicting_email review case, they are not discarded",
            privacy_behavior="hashed via pii_hash before appearing in any activation export",
        ),
        SurvivorshipRule(
            field="phone",
            authoritative_source="highest SOURCE_PRIORITY source with a non-null value",
            fallback_source="most recently updated non-null value across sources",
            tie_breaking_rule="most recent event_timestamp wins",
            null_behavior="null values are skipped, never overwrite a known value",
            conflict_behavior="surviving value wins the canonical record; the losing "
            "value(s) generate a conflicting_phone review case, they are not discarded",
            privacy_behavior="hashed via pii_hash before appearing in any activation export",
        ),
        SurvivorshipRule(
            field="customer_status",
            authoritative_source="account_app / billing_platform (account-system authority)",
            fallback_source="most recently updated non-null value across sources",
            tie_breaking_rule="highest SOURCE_PRIORITY, then most recent event_timestamp",
            null_behavior="null values are skipped",
            conflict_behavior="account-system value always wins over marketing/support "
            "signals; disagreement does not open a review case (expected: marketing "
            "consent status is independent of account lifecycle status)",
            privacy_behavior="not PII, no masking required",
        ),
        SurvivorshipRule(
            field="external_account_id",
            authoritative_source="account_app (system of record for account identity)",
            fallback_source="any source that supplies a non-null value",
            tie_breaking_rule="first non-null value observed wins (stable identifier, "
            "should not change once assigned)",
            null_behavior="a record without it can still merge on email/phone/customer_id",
            conflict_behavior="two different non-null values on what would otherwise be "
            "one cluster blocks the auto-merge and opens an identifier_reuse review case",
            privacy_behavior="treated as a stable identifier, not masked",
        ),
        SurvivorshipRule(
            field="marketing_consent / do_not_contact / deletion_requested",
            authoritative_source="privacy.activation_policy (consent event stream), "
            "not the identity graph",
            fallback_source="none — identity merges never infer consent",
            tie_breaking_rule="most recent consent event wins, independent of which "
            "source record survives identity merge",
            null_behavior="treated as no consent (fail closed)",
            conflict_behavior="identity merges never override or clear an existing "
            "suppression; suppression state always survives a merge",
            privacy_behavior="marketing consent does not override account identity, and "
            "identity merges never grant activation eligibility a suppressed record "
            "did not already have",
        ),
    ]


def _weak_link_candidates(weak_links: list[WeakLinkCandidate]) -> list[ReviewCase]:
    """Convert resolver-detected weak links (deliberately not auto-merged) into
    review cases. The gating decision itself already happened in
    `resolver._component_records` — this only formats the evidence for a steward."""
    now = _now()
    cases: dict[str, ReviewCase] = {}
    for link in weak_links:
        conflict_type = "low_confidence_match" if link.identifier_type == "device_id" else "ambiguous_shared_identifier"
        case_id = _case_id("weak_link", link.token, link.left_canonical_customer_id, link.right_canonical_customer_id)
        if case_id in cases:
            continue
        cases[case_id] = ReviewCase(
            review_case_id=case_id,
            tenant_id=link.tenant_id,
            canonical_customer_id=link.left_canonical_customer_id,
            candidate_customer_id=link.right_canonical_customer_id,
            source_system=link.right_source_system,
            source_customer_id=link.right_source_record_id,
            conflict_type=conflict_type,
            match_rule=link.match_rule,
            confidence_score=link.match_confidence,
            evidence_summary=(
                f"{link.left_source_system}:{link.left_source_record_id} and "
                f"{link.right_source_system}:{link.right_source_record_id} share "
                f"{link.identifier_type} evidence ({link.match_rule}, "
                f"confidence={link.match_confidence:.2f}) but resolved to different "
                f"canonical customers ({link.left_canonical_customer_id} vs "
                f"{link.right_canonical_customer_id}) because the evidence is below the "
                f"auto-merge threshold ({config.AUTO_MERGE_MIN})."
            ),
            current_status="OPEN",
            created_at=now,
            updated_at=now,
            resolved_at=None,
            reviewer=None,
            decision=None,
            decision_reason=None,
            survivorship_rule=None,
            source_event_id=None,
        )
    return sorted(cases.values(), key=lambda c: c.review_case_id)


def _survivorship_conflicts(
    events, canonical: list[CanonicalCustomer], mappings: list[IdentityMapRow]
) -> list[ReviewCase]:
    """Already-merged canonical customers whose linked records disagree on email/phone."""
    latest = _latest_records(events)
    now = _now()
    records_by_customer: dict[str, list[Any]] = {}
    for record in latest.values():
        for row in mappings:
            if (
                _tenant_scope(row.tenant_id) == _tenant_scope(record.tenant_id)
                and row.source_system == record.source_system
                and row.source_record_id == record.source_record_id
            ):
                records_by_customer.setdefault(row.canonical_customer_id, []).append(record)
                break

    cases: list[ReviewCase] = []
    for customer in canonical:
        records = records_by_customer.get(customer.canonical_customer_id, [])
        if len(records) < 2:
            continue
        emails = sorted({normalize_email(r.payload.get("email")) for r in records if normalize_email(r.payload.get("email"))})
        phones = sorted({normalize_phone(r.payload.get("phone")) for r in records if normalize_phone(r.payload.get("phone"))})
        for field_name, values, rule, confidence in (
            ("conflicting_email", emails, "exact_normalized_email", 0.86),
            ("conflicting_phone", phones, "exact_normalized_phone", 0.78),
        ):
            if len(values) < 2:
                continue
            case_id = _case_id("conflict", field_name, customer.canonical_customer_id)
            evidence_record = records[0]
            cases.append(
                ReviewCase(
                    review_case_id=case_id,
                    tenant_id=customer.tenant_id,
                    canonical_customer_id=customer.canonical_customer_id,
                    candidate_customer_id=customer.canonical_customer_id,
                    source_system=evidence_record.source_system,
                    source_customer_id=evidence_record.source_record_id,
                    conflict_type=field_name,
                    match_rule=rule,
                    confidence_score=confidence,
                    evidence_summary=(
                        f"canonical customer {customer.canonical_customer_id} merged "
                        f"{len(records)} source records that disagree on "
                        f"{field_name.replace('conflicting_', '')}: {', '.join(values)}. "
                        f"Merge itself is not undone (a stronger identifier justified it); "
                        f"survivorship_rule below decided which value is authoritative."
                    ),
                    current_status="OPEN",
                    created_at=now,
                    updated_at=now,
                    resolved_at=None,
                    reviewer=None,
                    decision=None,
                    decision_reason=None,
                    survivorship_rule="source_priority_then_latest_non_null",
                    source_event_id=None,
                )
            )
    return sorted(cases, key=lambda c: c.review_case_id)


def detect_review_candidates(
    events, canonical: list[CanonicalCustomer], mappings: list[IdentityMapRow]
) -> list[ReviewCase]:
    """Full review-queue detection pass (spec section 11-12). Never mutates `mappings`.

    Recomputes resolution once more (deterministic, so this stays consistent with the
    `canonical`/`mappings` the caller already has) purely to recover the weak-link
    candidates the resolver's auto-merge gate held back — those aren't derivable from
    `canonical`/`mappings` alone since, by construction, they were never merged.
    """
    _canonical2, _mappings2, _audit2, weak_links = resolve_identity_with_review_candidates(events)
    return _weak_link_candidates(weak_links) + _survivorship_conflicts(events, canonical, mappings)


def apply_decision(
    case: ReviewCase, *, decision: str, reviewer: str, reason: str
) -> ReviewCase:
    """Apply a reviewer decision, enforcing the status state machine (spec section 13).

    Raises ValueError on an unrecognized decision or an illegal status transition —
    a case cannot silently skip states (e.g. OPEN straight to RESOLVED without a
    decision, or a decision applied to an already-RESOLVED case).
    """
    if decision not in config.REVIEW_DECISIONS:
        raise ValueError(f"unknown decision: {decision!r}")
    next_status = config.DECISION_TO_STATUS[decision]
    allowed = config.VALID_STATUS_TRANSITIONS.get(case.current_status, set())
    if next_status not in allowed:
        raise ValueError(
            f"illegal transition: cannot apply decision {decision!r} "
            f"({case.current_status} -> {next_status}) from status {case.current_status!r}; "
            f"allowed next statuses are {sorted(allowed) or 'none (terminal state)'}"
        )
    now = _now()
    return ReviewCase(
        **{
            **asdict(case),
            "current_status": next_status,
            "updated_at": now,
            "resolved_at": now if next_status in {"RESOLVED"} else case.resolved_at,
            "reviewer": reviewer,
            "decision": decision,
            "decision_reason": reason,
        }
    )


def merge_customers(
    mappings: list[IdentityMapRow],
    *,
    source_canonical_customer_id: str,
    target_canonical_customer_id: str,
    reviewer: str,
    reason: str,
    review_case_id: str | None = None,
) -> tuple[list[IdentityMapRow], IdentityMergeAudit]:
    """Steward-approved merge: reassign all mappings from source -> target canonical ID.

    Enforces tenant isolation: refuses to merge canonical customers from different
    tenants, even if a reviewer accidentally requests it.
    """
    source_rows = [m for m in mappings if m.canonical_customer_id == source_canonical_customer_id]
    target_rows = [m for m in mappings if m.canonical_customer_id == target_canonical_customer_id]
    if not source_rows or not target_rows:
        raise ValueError("both source and target canonical_customer_id must have existing mappings")
    source_tenants = {_tenant_scope(m.tenant_id) for m in source_rows}
    target_tenants = {_tenant_scope(m.tenant_id) for m in target_rows}
    if source_tenants != target_tenants:
        raise ValueError(
            f"refusing cross-tenant merge: source tenants={sorted(source_tenants)} "
            f"target tenants={sorted(target_tenants)}"
        )

    updated = [
        m if m.canonical_customer_id != source_canonical_customer_id
        else IdentityMapRow(**{**asdict(m), "canonical_customer_id": target_canonical_customer_id})
        for m in mappings
    ]
    audit = IdentityMergeAudit(
        merge_audit_id=_case_id("merge", source_canonical_customer_id, target_canonical_customer_id, _now()),
        tenant_id=next(iter(source_tenants)),
        review_case_id=review_case_id,
        source_canonical_customer_id=source_canonical_customer_id,
        target_canonical_customer_id=target_canonical_customer_id,
        reviewer=reviewer,
        reason=reason,
        occurred_at=_now(),
    )
    return updated, audit


def unmerge_customer(
    mappings: list[IdentityMapRow],
    *,
    canonical_customer_id: str,
    source_system: str,
    source_record_id: str,
    reviewer: str,
    reason: str,
) -> tuple[list[IdentityMapRow], str, IdentityUnmergeAudit]:
    """Split one mapped source record out of a canonical customer into a new one.

    Only the targeted (tenant, source_system, source_record_id) mapping row is
    touched — every other mapping row, for this canonical customer or any other,
    is returned unchanged. This is what the stewardship test suite checks for:
    unrelated canonical customers must not be affected by an unmerge.
    """
    target_rows = [
        m
        for m in mappings
        if m.canonical_customer_id == canonical_customer_id
        and m.source_system == source_system
        and m.source_record_id == source_record_id
    ]
    if not target_rows:
        raise ValueError("no matching mapping row found for the given canonical_customer_id/source_system/source_record_id")
    if len(target_rows) > 1:
        raise ValueError("ambiguous unmerge target: more than one matching mapping row")
    target = target_rows[0]

    new_canonical_id = "cc_" + hashlib.sha1(
        f"unmerge:{canonical_customer_id}:{source_system}:{source_record_id}:{_now()}".encode("utf-8")
    ).hexdigest()[:12]

    updated = [
        m if not (
            m.canonical_customer_id == canonical_customer_id
            and m.source_system == source_system
            and m.source_record_id == source_record_id
        )
        else IdentityMapRow(**{**asdict(m), "canonical_customer_id": new_canonical_id})
        for m in mappings
    ]
    audit = IdentityUnmergeAudit(
        unmerge_audit_id=_case_id("unmerge", canonical_customer_id, source_system, source_record_id, _now()),
        tenant_id=target.tenant_id,
        original_canonical_customer_id=canonical_customer_id,
        new_canonical_customer_id=new_canonical_id,
        source_system=source_system,
        source_record_id=source_record_id,
        reviewer=reviewer,
        reason=reason,
        occurred_at=_now(),
    )
    return updated, new_canonical_id, audit


def write_stewardship_outputs(
    *,
    review_cases: list[ReviewCase],
    output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "identity_review_queue.csv": [asdict(row) for row in review_cases],
        "identity_survivorship_rule.csv": [asdict(row) for row in survivorship_rules()],
    }
    for filename, rows in outputs.items():
        with (output_dir / filename).open("w", newline="", encoding="utf-8") as fh:
            if not rows:
                continue
            writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Detect identity stewardship review cases from resolved identities.")
    parser.add_argument("--input", default="ingestion/output/raw_cdc_events.jsonl")
    parser.add_argument("--output-dir", default="identity_resolution/output")
    args = parser.parse_args()
    events = load_events(Path(args.input))
    canonical, mappings, _audit = resolve_identity(events)
    review_cases = detect_review_candidates(events, canonical, mappings)
    write_stewardship_outputs(review_cases=review_cases, output_dir=Path(args.output_dir))
    open_cases = sum(1 for c in review_cases if c.current_status == "OPEN")
    print(f"review_cases={len(review_cases)} open={open_cases}")


if __name__ == "__main__":
    main()
