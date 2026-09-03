import copy

from contracts.contract_gate import compare_contracts, summarize_compatibility
from ingestion.contracts import load_contracts


def test_contract_gate_accepts_nullable_additive_field():
    baseline = load_contracts()
    candidate = copy.deepcopy(baseline)
    candidate["customers"]["nullable"].append("loyalty_tier")
    candidate["customers"]["types"]["loyalty_tier"] = "string"
    findings = compare_contracts(baseline, candidate)
    assert any(f.change_type == "added_nullable_field" and f.compatibility == "compatible" for f in findings)
    assert not any(f.compatibility == "breaking" for f in findings)


def test_contract_gate_rejects_required_field_removal_and_type_change():
    baseline = load_contracts()
    candidate = copy.deepcopy(baseline)
    candidate["customers"]["required"].remove("customer_id")
    candidate["engagement_events"]["types"]["event_count"] = "string"
    findings = compare_contracts(baseline, candidate)
    assert any(f.change_type == "removed_required_field" for f in findings)
    assert any(f.change_type == "type_changed" for f in findings)
    assert any(f.compatibility == "breaking" for f in findings)
    summary = summarize_compatibility(findings, old_version="1.0", new_version="2.0")
    customers = next(row for row in summary if row.entity_type == "customers")
    assert customers.status == "breaking"
    assert "removed_required_field:customer_id" in customers.breaking_changes
    assert customers.checked_at.endswith("Z")
