import csv
from pathlib import Path

from data_generation.schema_drift import run_schema_drift_suite
from reverse_etl.destinations.simulator import DestinationConfig, simulate_destination_sync
from scripts.run_benchmark import build_scaled_events, run_benchmark


def test_destination_sync_simulator_logs_success_and_failures(tmp_path: Path):
    export_dir = tmp_path / "exports"
    output_dir = tmp_path / "sync_logs"
    export_dir.mkdir()
    with (export_dir / "campaign_target_export.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "canonical_customer_id",
                "tenant_id",
                "business_unit",
                "email_sha256",
                "phone_sha256",
                "export_timestamp",
                "campaign_target",
                "customer_segment",
                "churn_risk_band",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "canonical_customer_id": "cc_test_001",
                "tenant_id": "tenant_us",
                "business_unit": "self_serve",
                "email_sha256": "a" * 64,
                "phone_sha256": "",
                "export_timestamp": "2026-01-01T00:00:00Z",
                "campaign_target": "product_education",
                "customer_segment": "self_serve_core",
                "churn_risk_band": "low",
            }
        )
        writer.writerow(
            {
                "canonical_customer_id": "cc_test_missing",
                "tenant_id": "tenant_us",
                "business_unit": "self_serve",
                "email_sha256": "",
                "phone_sha256": "",
                "export_timestamp": "2026-01-01T00:00:00Z",
                "campaign_target": "product_education",
                "customer_segment": "self_serve_core",
                "churn_risk_band": "low",
            }
        )

    run_logs, failed_rows = simulate_destination_sync(
        export_dir=export_dir,
        output_dir=output_dir,
        destinations=[DestinationConfig("braze", "campaign_target_export.csv", "users/track", 2, 2)],
    )
    assert len(run_logs) == 1
    assert run_logs[0].attempted_count == 2
    assert run_logs[0].inserted_count >= 1
    assert failed_rows
    assert (output_dir / "sync_run_log.csv").exists()
    assert (output_dir / "destination_sync_state.csv").exists()


def test_schema_drift_suite_covers_breaking_and_compatible_changes():
    results, _ = run_schema_drift_suite()
    assert len(results) >= 5
    assert all(row.passed_expectation for row in results)
    statuses = {row.actual_status for row in results}
    assert statuses == {"accepted", "rejected"}


def test_benchmark_scaled_events_and_metrics_are_created():
    scaled = build_scaled_events(2)
    metrics = run_benchmark(1)
    assert len(scaled) == 304
    assert {metric.stage_name for metric in metrics} >= {
        "generate_scaled_events",
        "normalize_cdc",
        "resolve_identity",
        "build_activation_exports",
    }
