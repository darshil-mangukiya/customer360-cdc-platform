import pytest

from identity_resolution import config
from identity_resolution.resolver import IdentityMapRow, resolve_identity
from identity_resolution.stewardship import (
    apply_decision,
    detect_review_candidates,
    merge_customers,
    survivorship_rules,
    unmerge_customer,
)
from ingestion.loader import normalize_records


def _event(
    *,
    event_id: str,
    source_system: str,
    source_table: str,
    record_id: str,
    tenant_id: str,
    payload: dict,
) -> dict:
    return {
        "event_id": event_id,
        "source_system": source_system,
        "source_table": source_table,
        "operation_type": "insert",
        "event_timestamp": "2026-01-01T00:00:00Z",
        "record_primary_key": record_id,
        "payload_before": None,
        "payload_after": {**payload, "tenant_id": tenant_id},
        "batch_id": "batch_stewardship",
        "schema_version": 1,
        "tenant_id": tenant_id,
    }


def test_survivorship_rules_cover_pii_and_privacy_fields():
    rules = survivorship_rules()
    fields = {r.field for r in rules}
    assert "email" in fields
    assert "phone" in fields
    assert any("consent" in f for f in fields)
    for rule in rules:
        assert rule.tie_breaking_rule
        assert rule.null_behavior
        assert rule.conflict_behavior
        assert rule.privacy_behavior


def test_weak_identifier_link_goes_to_review_not_auto_merge():
    # Two customers only share a device_id (weak signal, confidence 0.62 < AUTO_MERGE_MIN).
    # They must resolve to two different canonical customers, and the shared device_id
    # must appear as an OPEN review case rather than silently merging them.
    events = normalize_records(
        [
            _event(
                event_id="evt_a",
                source_system="product_analytics",
                source_table="engagement_events",
                record_id="eng_a",
                tenant_id="tenant_us",
                payload={
                    "engagement_event_id": "eng_a",
                    "business_unit": "self_serve",
                    "device_id": "device_shared_123",
                    "customer_id": "cust_alpha",
                    "email": "alpha@example.com",
                    "event_name": "login",
                    "event_count": 1,
                    "session_minutes": 5,
                    "event_timestamp": "2026-01-01T00:00:00Z",
                    "updated_at": "2026-01-01T00:00:00Z",
                },
            ),
            _event(
                event_id="evt_b",
                source_system="product_analytics",
                source_table="engagement_events",
                record_id="eng_b",
                tenant_id="tenant_us",
                payload={
                    "engagement_event_id": "eng_b",
                    "business_unit": "self_serve",
                    "device_id": "device_shared_123",
                    "customer_id": "cust_beta",
                    "email": "beta@example.com",
                    "event_name": "login",
                    "event_count": 1,
                    "session_minutes": 5,
                    "event_timestamp": "2026-01-01T00:05:00Z",
                    "updated_at": "2026-01-01T00:05:00Z",
                },
            ),
        ]
    )[0]

    canonical, mappings, _audit = resolve_identity(events)
    assert len({row.canonical_customer_id for row in mappings}) == 2, (
        "records sharing only a weak device_id signal must not be auto-merged"
    )

    cases = detect_review_candidates(events, canonical, mappings)
    weak_cases = [c for c in cases if c.conflict_type == "low_confidence_match"]
    assert weak_cases, "expected a low_confidence_match review case for the shared device_id"
    case = weak_cases[0]
    assert case.current_status == "OPEN"
    assert case.confidence_score == pytest.approx(0.62)
    assert case.canonical_customer_id != case.candidate_customer_id


def test_conflicting_email_within_merged_customer_opens_review_case():
    # Same external_account_id (strong identifier, auto-merges) but two different emails.
    events = normalize_records(
        [
            _event(
                event_id="evt_c1",
                source_system="account_app",
                source_table="customers",
                record_id="cust_c1",
                tenant_id="tenant_us",
                payload={
                    "customer_id": "cust_c1",
                    "external_account_id": "acct_ext_shared_1",
                    "business_unit": "self_serve",
                    "email": "old-email@example.com",
                    "customer_status": "active",
                    "updated_at": "2026-01-01T00:00:00Z",
                },
            ),
            _event(
                event_id="evt_c2",
                source_system="billing_platform",
                source_table="customers",
                record_id="cust_c2",
                tenant_id="tenant_us",
                payload={
                    "customer_id": "cust_c2",
                    "external_account_id": "acct_ext_shared_1",
                    "business_unit": "self_serve",
                    "email": "new-email@example.com",
                    "customer_status": "active",
                    "updated_at": "2026-01-01T00:05:00Z",
                },
            ),
        ]
    )[0]

    canonical, mappings, _audit = resolve_identity(events)
    assert len({row.canonical_customer_id for row in mappings}) == 1, "strong shared identifier should auto-merge"

    cases = detect_review_candidates(events, canonical, mappings)
    conflict_cases = [c for c in cases if c.conflict_type == "conflicting_email"]
    assert len(conflict_cases) == 1
    assert conflict_cases[0].current_status == "OPEN"
    assert conflict_cases[0].survivorship_rule == "source_priority_then_latest_non_null"


def test_status_transitions_valid_and_invalid():
    from identity_resolution.stewardship import ReviewCase

    case = ReviewCase(
        review_case_id="revcase_test",
        tenant_id="tenant_us",
        canonical_customer_id="cc_a",
        candidate_customer_id="cc_b",
        source_system="account_app",
        source_customer_id="cust_x",
        conflict_type="low_confidence_match",
        match_rule="device_id_behavioral_supporting_match",
        confidence_score=0.62,
        evidence_summary="test case",
        current_status="OPEN",
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
        resolved_at=None,
        reviewer=None,
        decision=None,
        decision_reason=None,
        survivorship_rule=None,
        source_event_id=None,
    )

    in_review = apply_decision(case, decision="NEEDS_REVIEW", reviewer="steward_1", reason="looking into it")
    assert in_review.current_status == "IN_REVIEW"

    approved = apply_decision(in_review, decision="APPROVE_MERGE", reviewer="steward_1", reason="confirmed same person")
    assert approved.current_status == "APPROVED"

    resolved = apply_decision(approved, decision="IGNORE_FALSE_POSITIVE", reviewer="steward_1", reason="closing out")
    # APPROVED -> RESOLVED is the only allowed decision landing in RESOLVED from APPROVED,
    # but IGNORE_FALSE_POSITIVE always maps to RESOLVED regardless of source decision text.
    assert resolved.current_status == "RESOLVED"

    with pytest.raises(ValueError):
        # RESOLVED is terminal; no further decisions are legal.
        apply_decision(resolved, decision="APPROVE_MERGE", reviewer="steward_1", reason="too late")

    with pytest.raises(ValueError):
        apply_decision(case, decision="not_a_real_decision", reviewer="steward_1", reason="bogus")


def test_merge_customers_refuses_cross_tenant_merge():
    mappings = [
        IdentityMapRow(
            canonical_customer_id="cc_us",
            tenant_id="tenant_us",
            source_system="account_app",
            source_table="customers",
            source_record_id="cust_us_1",
            match_rule="deterministic_email",
            match_confidence=0.86,
            canonical_customer_version=1,
            identifier_fingerprint="fp1",
            first_seen_at="2026-01-01T00:00:00Z",
            last_seen_at="2026-01-01T00:00:00Z",
            is_active=True,
        ),
        IdentityMapRow(
            canonical_customer_id="cc_eu",
            tenant_id="tenant_eu",
            source_system="account_app",
            source_table="customers",
            source_record_id="cust_eu_1",
            match_rule="deterministic_email",
            match_confidence=0.86,
            canonical_customer_version=1,
            identifier_fingerprint="fp2",
            first_seen_at="2026-01-01T00:00:00Z",
            last_seen_at="2026-01-01T00:00:00Z",
            is_active=True,
        ),
    ]
    with pytest.raises(ValueError, match="cross-tenant"):
        merge_customers(
            mappings,
            source_canonical_customer_id="cc_eu",
            target_canonical_customer_id="cc_us",
            reviewer="steward_1",
            reason="mistaken merge attempt",
        )


def test_unmerge_does_not_affect_unrelated_canonical_customers():
    mappings = [
        IdentityMapRow(
            canonical_customer_id="cc_shared",
            tenant_id="tenant_us",
            source_system="account_app",
            source_table="customers",
            source_record_id="cust_1",
            match_rule="deterministic_customer_id_email",
            match_confidence=0.95,
            canonical_customer_version=1,
            identifier_fingerprint="fp1",
            first_seen_at="2026-01-01T00:00:00Z",
            last_seen_at="2026-01-01T00:00:00Z",
            is_active=True,
        ),
        IdentityMapRow(
            canonical_customer_id="cc_shared",
            tenant_id="tenant_us",
            source_system="billing_platform",
            source_table="customers",
            source_record_id="cust_2",
            match_rule="deterministic_customer_id_email",
            match_confidence=0.95,
            canonical_customer_version=1,
            identifier_fingerprint="fp1",
            first_seen_at="2026-01-01T00:00:00Z",
            last_seen_at="2026-01-01T00:00:00Z",
            is_active=True,
        ),
        IdentityMapRow(
            canonical_customer_id="cc_unrelated",
            tenant_id="tenant_us",
            source_system="account_app",
            source_table="customers",
            source_record_id="cust_3",
            match_rule="deterministic_email",
            match_confidence=0.86,
            canonical_customer_version=1,
            identifier_fingerprint="fp3",
            first_seen_at="2026-01-01T00:00:00Z",
            last_seen_at="2026-01-01T00:00:00Z",
            is_active=True,
        ),
    ]

    updated, new_canonical_id, audit = unmerge_customer(
        mappings,
        canonical_customer_id="cc_shared",
        source_system="billing_platform",
        source_record_id="cust_2",
        reviewer="steward_1",
        reason="different person, false positive merge",
    )

    by_record = {(m.source_system, m.source_record_id): m for m in updated}
    assert by_record[("account_app", "cust_1")].canonical_customer_id == "cc_shared"
    assert by_record[("billing_platform", "cust_2")].canonical_customer_id == new_canonical_id
    assert new_canonical_id != "cc_shared"
    # The unrelated canonical customer must be completely untouched.
    assert by_record[("account_app", "cust_3")].canonical_customer_id == "cc_unrelated"
    assert audit.original_canonical_customer_id == "cc_shared"
    assert audit.new_canonical_customer_id == new_canonical_id
    assert audit.tenant_id == "tenant_us"


def test_unmerge_raises_on_unknown_target():
    mappings = [
        IdentityMapRow(
            canonical_customer_id="cc_shared",
            tenant_id="tenant_us",
            source_system="account_app",
            source_table="customers",
            source_record_id="cust_1",
            match_rule="deterministic_email",
            match_confidence=0.86,
            canonical_customer_version=1,
            identifier_fingerprint="fp1",
            first_seen_at="2026-01-01T00:00:00Z",
            last_seen_at="2026-01-01T00:00:00Z",
            is_active=True,
        ),
    ]
    with pytest.raises(ValueError):
        unmerge_customer(
            mappings,
            canonical_customer_id="cc_shared",
            source_system="does_not_exist",
            source_record_id="nope",
            reviewer="steward_1",
            reason="bad target",
        )


def test_thresholds_are_ordered():
    assert 0.0 < config.REVIEW_MIN < config.AUTO_MERGE_MIN < 1.0
