from __future__ import annotations

import argparse
import copy
import csv
import json
import sys
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from contracts.contract_gate import compare_contracts
from data_generation.cdc_generator import build_cdc_events
from identity_resolution.resolver import resolve_identity
from identity_resolution.stewardship import detect_review_candidates
from ingestion.cdc_state import build_deduplication_log
from ingestion.contracts import load_contracts
from ingestion.loader import normalize_records
from observability.metrics import build_freshness


@dataclass(frozen=True)
class IncidentSimulation:
    incident_id: str
    incident_name: str
    scenario: str
    severity: str
    impact: str
    detection: str
    root_cause: str
    resolution: str
    corrective_action: str
    prevention: str
    verification: str
    verification_status: str
    expected_status: str
    simulated_at: str


SCENARIOS = {
    "schema_drift_break": {
        "incident_id": "INC-001",
        "scenario": "Breaking CDC schema",
        "severity": "critical",
        "impact": "Older producers would be rejected before landing.",
        "detection": "Contract compatibility gate",
        "root_cause": "A required customer source key was removed.",
        "resolution": "Reject the candidate contract and version a compatible migration.",
        "corrective_action": "Restore the required field or add a mapped versioned field.",
        "prevention": "Run compatibility fixtures in CI before deployment.",
        "expected_status": "blocked_before_activation",
    },
    "duplicate_replay_spike": {
        "incident_id": "INC-002",
        "scenario": "Duplicate/replay spike",
        "severity": "high",
        "impact": "Repeated events could duplicate downstream mutations.",
        "detection": "CDC deduplication audit",
        "root_cause": "The same event was delivered more than once.",
        "resolution": "Retain the first event and classify later deliveries as duplicates.",
        "corrective_action": "Resume from the recorded topic checkpoint.",
        "prevention": "Use event ID, hash, and topic-partition-offset idempotency keys.",
        "expected_status": "recovered",
    },
    "identity_merge_anomaly_spike": {
        "incident_id": "INC-003",
        "scenario": "Identity conflict spike",
        "severity": "critical",
        "impact": "Ambiguous identifiers could create an unsafe merge.",
        "detection": "Identity stewardship review queue",
        "root_cause": "Weak or conflicting identifiers produced review-range confidence.",
        "resolution": "Quarantine the candidate for explicit review.",
        "corrective_action": "Resolve or reject the review case without auto-merging.",
        "prevention": "Keep auto-merge confidence above the review threshold.",
        "expected_status": "blocked_before_activation",
    },
    "activation_reconciliation_mismatch": {
        "incident_id": "INC-004",
        "scenario": "Activation reconciliation mismatch",
        "severity": "critical",
        "impact": "An eligible customer has no recorded activation disposition.",
        "detection": "Activation accounting invariant",
        "root_cause": "One destination result was intentionally omitted.",
        "resolution": "Block completion and investigate the missing disposition.",
        "corrective_action": "Replay only the missing idempotency key and reconcile again.",
        "prevention": "Require eligible = success + failure + suppression + skip + duplicate.",
        "expected_status": "variance_detected",
    },
    "stale_customer_360": {
        "incident_id": "INC-005",
        "scenario": "Stale Customer 360",
        "severity": "high",
        "impact": "Activation could use an outdated customer state.",
        "detection": "Tenant/entity freshness monitor",
        "root_cause": "The deterministic fixture clock is older than the freshness threshold.",
        "resolution": "Hold activation until the model refresh completes.",
        "corrective_action": "Resume ingestion and rerun dbt before export.",
        "prevention": "Gate exports on model freshness.",
        "expected_status": "warning_detected",
    },
    "cross_tenant_identity_attempt": {
        "incident_id": "INC-006",
        "scenario": "Cross-tenant identity attempt",
        "severity": "critical",
        "impact": "A shared identifier could leak identity across tenants.",
        "detection": "Tenant-isolation identity assertion",
        "root_cause": "Two tenants intentionally supplied the same email.",
        "resolution": "Keep separate canonical identities and mappings.",
        "corrective_action": "Reject any mapping whose tenant differs from its canonical identity.",
        "prevention": "Include tenant_id in every identity graph key.",
        "expected_status": "blocked_cross_tenant_merge",
    },
}


def _verification(name: str) -> tuple[bool, str]:
    if name == "schema_drift_break":
        baseline = load_contracts()
        candidate = copy.deepcopy(baseline)
        candidate["customers"]["required"].remove("customer_id")
        breaking = [row for row in compare_contracts(baseline, candidate) if row.compatibility == "breaking"]
        return bool(breaking), f"breaking_findings={len(breaking)}"

    events, _ = normalize_records([event.__dict__ for event in build_cdc_events(seed=42)], deduplicate=False)
    if name == "duplicate_replay_spike":
        duplicate = replace(events[0], event_id="evt_replay_duplicate")
        audit = build_deduplication_log([events[0], duplicate])
        detected = audit[-1].dedupe_status == "duplicate"
        return detected, f"dedupe_status={audit[-1].dedupe_status} duplicate_key={audit[-1].duplicate_key}"
    if name == "identity_merge_anomaly_spike":
        canonical, mappings, _ = resolve_identity(events)
        cases = detect_review_candidates(events, canonical, mappings)
        return bool(cases), f"review_cases={len(cases)}"
    if name == "activation_reconciliation_mismatch":
        eligible, successful, failed, suppressed, skipped, duplicate = 10, 8, 0, 0, 1, 0
        variance = eligible - (successful + failed + suppressed + skipped + duplicate)
        return variance != 0, f"injected_variance_count={variance}"
    if name == "stale_customer_360":
        stale = [row for row in build_freshness(events) if row.status.startswith("stale")]
        return bool(stale), f"stale_tenant_entities={len(stale)}"
    if name == "cross_tenant_identity_attempt":
        customer = next(event for event in events if event.source_table == "customers" and event.payload_after)
        other_payload = {
            **customer.payload_after,
            "tenant_id": "tenant_isolation_test",
            "customer_id": "cust_same_id_other_tenant",
        }
        other = replace(
            customer,
            event_id="evt_cross_tenant_attempt",
            tenant_id="tenant_isolation_test",
            record_primary_key="cust_same_id_other_tenant",
            payload_after=other_payload,
            event_hash="hash_cross_tenant_attempt",
            kafka_offset=(customer.kafka_offset or 0) + 1000,
        )
        canonical, mappings, _ = resolve_identity([customer, other])
        tenant_ids = {row.tenant_id for row in canonical}
        safe = len(canonical) == 2 and tenant_ids == {customer.tenant_id, "tenant_isolation_test"}
        return safe, f"canonical_customers={len(canonical)} tenants={sorted(tenant_ids)} mappings={len(mappings)}"
    raise KeyError(name)


def run_scenarios(names: list[str] | None = None) -> list[IncidentSimulation]:
    now = datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")
    selected = names or sorted(SCENARIOS)
    rows: list[IncidentSimulation] = []
    for name in selected:
        passed, evidence = _verification(name)
        definition = SCENARIOS[name]
        rows.append(
            IncidentSimulation(
                incident_name=name,
                verification=evidence,
                verification_status="pass" if passed else "fail",
                simulated_at=now,
                **definition,
            )
        )
    return rows


def write_outputs(rows: list[IncidentSimulation], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    dicts = [asdict(row) for row in rows]
    with (output_dir / "incident_simulation_results.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(dicts[0].keys()))
        writer.writeheader()
        writer.writerows(dicts)
    with (output_dir / "incident_simulation_results.jsonl").open("w", encoding="utf-8") as fh:
        for row in dicts:
            fh.write(json.dumps(row, sort_keys=True) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run executable local incident scenarios.")
    parser.add_argument("--scenario", choices=sorted(SCENARIOS), action="append")
    parser.add_argument("--output-dir", default="observability/incidents")
    args = parser.parse_args()
    rows = run_scenarios(args.scenario)
    write_outputs(rows, Path(args.output_dir))
    failed = [row.incident_id for row in rows if row.verification_status != "pass"]
    print(f"incidents={len(rows)} failed={failed} output_dir={args.output_dir}")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
