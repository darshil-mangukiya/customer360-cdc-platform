import pytest

from identity_resolution.fuzzy import IdentityCandidate, evaluate, generate_candidates, score_candidate
from identity_resolution.golden_record import GoldenSource, StewardOverride, build_golden_customer
from identity_resolution.ai_steward import OfflineDeterministicProvider, safe_evidence
from migration.parity import compare_rows


def candidate(record_id, **kwargs):
    defaults = dict(tenant_id="tenant_a", record_id=record_id, source_system="account_app", updated_at="2026-08-01T00:00:00Z")
    defaults.update(kwargs)
    return IdentityCandidate(**defaults)


def test_fuzzy_match_requires_verified_exact_signal_and_explains_score():
    left = candidate("a", email="Case+tag@example.com", name="Darshil Patel", email_verified=True)
    right = candidate("b", source_system="billing_platform", email="case@example.com", name="Darshil P Patel", email_verified=True)
    decision = score_candidate(left, right)
    assert decision.decision == "AUTO_MATCH"
    assert sum(signal.contribution for signal in decision.signals) == pytest.approx(decision.score, abs=0.001)
    assert any(signal.name == "email_exact" and signal.value == 1 for signal in decision.signals)


def test_fuzzy_only_match_goes_to_review_and_never_crosses_tenants():
    left = candidate("a", name="Maria Gonzalez", address="10 Main Street")
    right = candidate("b", name="Maria Gonzales", address="10 Main St")
    assert score_candidate(left, right).decision in {"REVIEW", "NO_MATCH"}
    with pytest.raises(ValueError, match="cross-tenant"):
        score_candidate(left, candidate("c", tenant_id="tenant_b"))


def test_candidate_generation_is_blocked_deduplicated_and_tenant_safe():
    rows = [candidate("a", email="one@example.com"), candidate("b", email="only@example.com"), candidate("c", tenant_id="tenant_b", email="one@example.com")]
    pairs = generate_candidates(rows)
    assert [(left.record_id, right.record_id) for left, right in pairs] == [("a", "b")]


def test_labeled_identity_evaluation_reports_false_merge_risk():
    decisions = [score_candidate(candidate("a", email="x@y.com", email_verified=True), candidate("b", email="x@y.com", email_verified=True))]
    metrics = evaluate(decisions, {("a", "b"): "TRUE_MATCH"})
    assert metrics == {"tp": 1, "fp": 0, "tn": 0, "fn": 0, "ambiguous": 0, "precision": 1.0, "recall": 1.0, "evaluated": 1}


def test_golden_record_uses_field_level_rules_and_audited_override():
    sources = [
        GoldenSource("tenant_a", "cc_1", "marketing_automation", "m1", "2026-08-20T00:00:00Z", {"email": "new@example.com", "name": "D. Patel"}, frozenset({"email"})),
        GoldenSource("tenant_a", "cc_1", "account_app", "a1", "2026-01-01T00:00:00Z", {"email": "trusted@example.com", "name": "Darshil Patel"}),
    ]
    override = StewardOverride("tenant_a", "cc_1", "name", "Darshil P.", "steward@example.test", "verified document", "2026-08-21T00:00:00Z")
    golden, provenance, audit = build_golden_customer(sources, overrides=[override])
    assert golden["email"] == "trusted@example.com"
    assert provenance["email"].source_system == "account_app"
    assert golden["name"] == "Darshil P."
    assert provenance["name"].is_manual_override
    assert audit[0]["previous_value"] == "Darshil Patel"


def test_golden_record_rejects_cross_tenant_override():
    source = GoldenSource("tenant_a", "cc_1", "account_app", "a1", "2026-01-01T00:00:00Z", {"name": "A"})
    override = StewardOverride("tenant_b", "cc_1", "name", "B", "s", "r", "2026-08-21T00:00:00Z")
    with pytest.raises(ValueError, match="cross-tenant"):
        build_golden_customer([source], overrides=[override])


def test_migration_parity_is_honest_and_value_level():
    source = [{"tenant_id": "a", "id": 1, "metric": 1.2}]
    assert compare_rows("m", source, None, key_fields=("tenant_id", "id")).status == "NOT_RUN"
    assert compare_rows("m", source, list(source), key_fields=("tenant_id", "id")).status == "PASS"
    failed = compare_rows("m", source, [{"tenant_id": "a", "id": 1, "metric": 2.1}], key_fields=("tenant_id", "id"))
    assert failed.status == "FAIL" and failed.mismatched_values == ("a|1",)


def test_ai_steward_is_masked_structured_and_never_authoritative():
    evidence = safe_evidence({"case_id": "case_1", "email": "raw@example.com", "deterministic_score": 0.82, "conflicting_fields": []})
    assert "email" not in evidence
    recommendation = OfflineDeterministicProvider().recommend(evidence)
    assert recommendation.recommendation == "MERGE"
    assert recommendation.requires_human_review is True
