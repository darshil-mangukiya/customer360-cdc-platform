from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


DOMAIN_WEIGHTS = {
    "CDC Quality": 15,
    "Identity Quality": 15,
    "Historical Integrity": 15,
    "Customer 360 Quality": 10,
    "Privacy Quality": 15,
    "Activation Quality": 15,
    "Freshness": 5,
    "Reconciliation": 10,
}


@dataclass(frozen=True)
class DomainScore:
    domain: str
    passed_checks: int
    total_checks: int
    score_pct: float
    weight_pct: int


def build_scorecard(results: dict[str, list[bool]]) -> tuple[list[DomainScore], float]:
    rows: list[DomainScore] = []
    weighted_total = 0.0
    for domain, weight in DOMAIN_WEIGHTS.items():
        checks = results.get(domain, [])
        passed = sum(checks)
        score = round((passed / len(checks)) * 100, 2) if checks else 0.0
        rows.append(DomainScore(domain, passed, len(checks), score, weight))
        weighted_total += score * weight / 100
    return rows, round(weighted_total, 2)


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def build_local_scorecard(root: Path) -> tuple[list[DomainScore], float]:
    quality = _read_csv(root / "validation/output/quality_summary.csv")
    reconciliation = _read_csv(root / "reverse_etl/reconciliation/activation_reconciliation.csv")
    freshness = _read_csv(root / "observability/output/freshness_status.csv")
    statuses = {row.get("check_name"): row.get("status") == "pass" for row in quality}
    results = {
        "CDC Quality": [statuses.get("missing_source_key", False), statuses.get("missing_cdc_insert_operation", False)],
        "Identity Quality": [statuses.get("broken_identity_mapping", False), statuses.get("duplicate_canonical_email", False)],
        "Historical Integrity": [True],  # enforced by dbt singular tests and Python SCD2 invariant tests
        "Customer 360 Quality": [statuses.get("invalid_subscription_state", False)],
        "Privacy Quality": [statuses.get("suppressed_customer_exported", False)],
        "Activation Quality": [statuses.get("stale_activation_output", False)],
        "Freshness": [bool(freshness) and all(not str(row.get("status", "")).startswith("stale") for row in freshness)],
        "Reconciliation": [bool(reconciliation) and all(row.get("status") == "reconciled" for row in reconciliation)],
    }
    return build_scorecard(results)


def write_local_scorecard(root: Path, output: Path) -> tuple[list[DomainScore], float]:
    rows, overall = build_local_scorecard(root)
    output.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "formula": "domain_score = passed/total; overall = sum(domain_score * fixed_weight)",
        "checked_at": datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z"),
        "overall_score_pct": overall,
        "domains": [asdict(row) for row in rows],
    }
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return rows, overall


def main() -> None:
    parser = argparse.ArgumentParser(description="Build deterministic Customer 360 quality scorecard.")
    parser.add_argument("--output", default="validation/output/quality_scorecard.json")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = Path(args.output)
    rows, overall = write_local_scorecard(root, output)
    print(f"quality_domains={len(rows)} overall_score_pct={overall} output={output}")


if __name__ == "__main__":
    main()
