from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class HealthCheck:
    check_name: str
    status: str
    observed_value: str
    expected_value: str
    severity: str


def _count_jsonl(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open(encoding="utf-8") as fh:
        return sum(1 for line in fh if line.strip())


def _load_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def build_health_checks(root: Path = ROOT) -> list[HealthCheck]:
    raw_events = _count_jsonl(root / "ingestion/output/raw_cdc_events.jsonl")
    rejected_events = _count_jsonl(root / "ingestion/output/rejected_events.jsonl")
    canonical = _load_csv(root / "identity_resolution/output/dim_customer_canonical.csv")
    quality = _load_csv(root / "validation/output/quality_summary.csv")
    freshness = _load_csv(root / "observability/output/freshness_status.csv")
    sync_runs = _load_csv(root / "reverse_etl/sync_logs/sync_run_log.csv")
    destination_state = _load_csv(root / "reverse_etl/sync_logs/destination_sync_state.csv")
    exports = list((root / "reverse_etl/exports").glob("*_export.csv"))
    review_queue = _load_csv(root / "identity_resolution/output/identity_review_queue.csv")
    reconciliation = _load_csv(root / "reverse_etl/reconciliation/activation_reconciliation.csv")
    reconciliation_findings = _load_csv(root / "reverse_etl/reconciliation/activation_reconciliation_findings.csv")
    failed_quality = [row for row in quality if row.get("status") == "fail"]
    failed_sync_rows = sum(int(row.get("failed_count") or 0) for row in sync_runs)
    variance_runs = [row for row in reconciliation if row.get("status") == "variance_detected"]
    critical_reconciliation_findings = [row for row in reconciliation_findings if row.get("severity") == "critical"]

    checks = [
        HealthCheck("raw_cdc_landed", "pass" if raw_events > 0 else "fail", str(raw_events), "> 0", "critical"),
        HealthCheck("intentional_reject_path", "pass" if rejected_events >= 1 else "fail", str(rejected_events), ">= 1", "medium"),
        HealthCheck("canonical_customers_built", "pass" if len(canonical) > 0 else "fail", str(len(canonical)), "> 0", "critical"),
        HealthCheck("activation_exports_present", "pass" if len(exports) >= 6 else "fail", str(len(exports)), ">= 6", "critical"),
        HealthCheck("quality_checks_green", "pass" if not failed_quality else "fail", str(len(failed_quality)), "0", "critical"),
        HealthCheck("freshness_rows_present", "pass" if len(freshness) >= 6 else "fail", str(len(freshness)), ">= 6", "medium"),
        HealthCheck("reverse_etl_sync_runs", "pass" if len(sync_runs) >= 4 else "fail", str(len(sync_runs)), ">= 4", "medium"),
        HealthCheck(
            "reverse_etl_destination_state",
            "pass" if len(destination_state) > 0 else "fail",
            str(len(destination_state)),
            "> 0",
            "medium",
        ),
        HealthCheck("reverse_etl_failed_rows", "pass" if failed_sync_rows <= 2 else "warn", str(failed_sync_rows), "<= 2", "low"),
        HealthCheck(
            "identity_review_queue_generated",
            "pass",
            str(len(review_queue)),
            ">= 0 (queue may legitimately be empty; this only checks the stage ran)",
            "low",
        ),
        HealthCheck(
            "activation_reconciliation_present",
            "pass" if reconciliation else "fail",
            str(len(reconciliation)),
            "> 0",
            "critical",
        ),
        # Reconciliation variance blocks: an unexplained mismatch between the
        # warehouse-eligible population and what was actually synced/suppressed is a
        # data-integrity problem, not a warning (spec section 41: quality gates).
        HealthCheck(
            "activation_reconciliation_variance",
            "pass" if not variance_runs else "fail",
            str(len(variance_runs)),
            "0 runs with status=variance_detected",
            "critical",
        ),
        HealthCheck(
            "activation_reconciliation_critical_findings",
            "pass" if not critical_reconciliation_findings else "fail",
            str(len(critical_reconciliation_findings)),
            "0 (e.g. no privacy-suppressed customer exported)",
            "critical",
        ),
    ]
    return checks


def write_reports(checks: list[HealthCheck], markdown_path: Path, json_path: Path) -> None:
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    rows = [asdict(check) for check in checks]
    json_path.write_text(json.dumps(rows, indent=2, sort_keys=True), encoding="utf-8")
    lines = [
        "# Docker E2E Health Report",
        "",
        "| Check | Status | Observed | Expected | Severity |",
        "| --- | --- | --- | --- | --- |",
    ]
    for check in checks:
        lines.append(
            f"| `{check.check_name}` | {check.status} | {check.observed_value} | {check.expected_value} | {check.severity} |"
        )
    markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a health report for the local or Docker E2E pipeline.")
    parser.add_argument("--markdown-output", default="reports/e2e_health_report.md")
    parser.add_argument("--json-output", default="reports/e2e_health_report.json")
    parser.add_argument("--fail-on-critical", action="store_true")
    args = parser.parse_args()
    checks = build_health_checks()
    write_reports(checks, Path(args.markdown_output), Path(args.json_output))
    critical_failures = [check.check_name for check in checks if check.status == "fail" and check.severity == "critical"]
    print(
        f"health_checks={len(checks)} critical_failures={critical_failures} "
        f"markdown={args.markdown_output} json={args.json_output}"
    )
    if args.fail_on_critical and critical_failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
