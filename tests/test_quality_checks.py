from data_generation.cdc_generator import build_cdc_events
from identity_resolution.resolver import resolve_identity
from ingestion.loader import normalize_records
from reverse_etl.exporter import build_activation_rows
from validation.quality_checks import QualitySummary, run_quality_checks, write_quality_outputs


def test_quality_checks_return_named_summary_rows():
    landed, _ = normalize_records([event.__dict__ for event in build_cdc_events(seed=23)])
    canonical, mappings, _ = resolve_identity(landed)
    exports = build_activation_rows(landed, canonical, mappings)
    failures, summary = run_quality_checks(landed, canonical, mappings, exports)
    assert {row.check_name for row in summary} >= {
        "duplicate_canonical_email",
        "broken_identity_mapping",
        "missing_source_key",
        "missing_cdc_insert_operation",
        "invalid_subscription_state",
        "stale_activation_output",
    }
    assert isinstance(failures, list)


def test_quality_output_csvs_include_headers_when_empty(tmp_path):
    write_quality_outputs(
        [],
        [
            QualitySummary(
                check_name="empty_failure_fixture",
                severity="low",
                status="pass",
                failure_count=0,
                checked_at="2026-01-01T00:00:00Z",
            )
        ],
        tmp_path,
    )

    failure_header = (tmp_path / "validation_failures.csv").read_text(encoding="utf-8").splitlines()[0]
    summary_header = (tmp_path / "quality_summary.csv").read_text(encoding="utf-8").splitlines()[0]

    assert failure_header == "check_name,severity,entity_key,failure_reason,observed_value,detected_at"
    assert summary_header == "check_name,severity,status,failure_count,checked_at"
