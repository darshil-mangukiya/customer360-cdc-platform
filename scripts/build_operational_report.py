from __future__ import annotations

import argparse
import csv
import html
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def read_jsonl_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open(encoding="utf-8") as fh:
        return sum(1 for line in fh if line.strip())


def table(rows: list[dict[str, Any]], columns: list[str], limit: int = 10) -> str:
    if not rows:
        return "<p class='muted'>No rows available.</p>"
    header = "".join(f"<th>{html.escape(col)}</th>" for col in columns)
    body = []
    for row in rows[:limit]:
        body.append(
            "<tr>"
            + "".join(f"<td>{html.escape(str(row.get(col, '')))}</td>" for col in columns)
            + "</tr>"
        )
    return f"<table><thead><tr>{header}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def metric_card(label: str, value: Any) -> str:
    return f"<div class='card'><div class='label'>{html.escape(label)}</div><div class='value'>{html.escape(str(value))}</div></div>"


def build_report() -> str:
    ingestion_log = read_csv(ROOT / "ingestion/output/ingestion_log.csv")
    rejected_count = read_jsonl_count(ROOT / "ingestion/output/rejected_events.jsonl")
    canonical = read_csv(ROOT / "identity_resolution/output/dim_customer_canonical.csv")
    quality = read_csv(ROOT / "validation/output/quality_summary.csv")
    freshness = read_csv(ROOT / "observability/output/freshness_status.csv")
    churn = read_csv(ROOT / "reverse_etl/exports/churn_risk_export.csv")
    sync_runs = read_csv(ROOT / "reverse_etl/sync_logs/sync_run_log.csv")
    sync_failed = read_csv(ROOT / "reverse_etl/sync_logs/sync_failed_rows.csv")
    drift = read_csv(ROOT / "data_generation/schema_drift_output/schema_drift_results.csv")
    benchmark = read_csv(ROOT / "benchmark/output/benchmark_summary.csv")

    landed = sum(int(row.get("landed_count") or 0) for row in ingestion_log)
    failed_quality = sum(1 for row in quality if row.get("status") == "fail")
    high_risk = sum(1 for row in churn if row.get("churn_risk_band") == "high")
    sync_success = sum(int(row.get("success_count") or 0) for row in sync_runs)
    sync_failures = sum(int(row.get("failed_count") or 0) for row in sync_runs)

    css = """
    body { font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; color: #172026; background: #f6f8f9; }
    header { background: #12343b; color: white; padding: 32px 42px; }
    h1 { margin: 0 0 8px; font-size: 30px; }
    h2 { margin-top: 34px; color: #12343b; }
    main { padding: 28px 42px 48px; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 14px; }
    .card { background: white; border: 1px solid #d8e1e5; border-radius: 8px; padding: 16px; }
    .label { color: #60717a; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
    .value { font-size: 28px; font-weight: 700; margin-top: 6px; color: #12343b; }
    table { width: 100%; border-collapse: collapse; background: white; border: 1px solid #d8e1e5; border-radius: 8px; overflow: hidden; }
    th, td { text-align: left; padding: 10px 12px; border-bottom: 1px solid #e8eef1; font-size: 13px; vertical-align: top; }
    th { background: #e8f0f2; color: #243942; }
    .muted { color: #64767f; }
    section { margin-bottom: 28px; }
    """

    cards = "".join(
        [
            metric_card("Landed CDC Events", landed),
            metric_card("Rejected Events", rejected_count),
            metric_card("Canonical Customers", len(canonical)),
            metric_card("High Churn-Risk Customers", high_risk),
            metric_card("Failed Quality Checks", failed_quality),
            metric_card("Destination Sync Successes", sync_success),
            metric_card("Destination Sync Failures", sync_failures),
        ]
    )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Customer 360 Operational Report</title>
  <style>{css}</style>
</head>
<body>
  <header>
    <h1>Customer 360 Operational Report</h1>
    <p>CDC ingestion, identity quality, activation freshness, reverse ETL sync, schema drift, and benchmark summary.</p>
  </header>
  <main>
    <section class="grid">{cards}</section>
    <section><h2>Quality Summary</h2>{table(quality, ["check_name", "severity", "status", "failure_count", "checked_at"])}</section>
    <section><h2>Freshness</h2>{table(freshness, ["entity_name", "max_event_timestamp", "lag_minutes", "status"])}</section>
    <section><h2>Reverse ETL Sync Runs</h2>{table(sync_runs, ["destination_name", "export_file", "attempted_count", "success_count", "failed_count", "retry_count", "rate_limit_events", "sync_status"])}</section>
    <section><h2>Sync Failed Rows</h2>{table(sync_failed, ["destination_name", "canonical_customer_id", "failure_reason", "attempts", "is_retryable"], limit=8)}</section>
    <section><h2>Schema Drift Scenarios</h2>{table(drift, ["scenario_name", "drift_type", "expected_status", "actual_status", "passed_expectation"])}</section>
    <section><h2>Benchmark Summary</h2>{table(benchmark, ["stage_name", "row_count", "elapsed_seconds", "rows_per_second"])}</section>
    <section><h2>Churn Risk Sample</h2>{table(churn, ["canonical_customer_id", "tenant_id", "churn_risk_score", "churn_risk_band", "last_refresh_time"], limit=8)}</section>
  </main>
</body>
</html>"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Build operational HTML report from local pipeline outputs.")
    parser.add_argument("--output", default="reports/operational_report.html")
    args = parser.parse_args()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(build_report(), encoding="utf-8")
    print(f"operational_report={output}")


if __name__ == "__main__":
    main()

