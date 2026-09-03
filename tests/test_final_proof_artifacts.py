import csv
from pathlib import Path

from ingestion.debezium_mapper import debezium_to_normalized_envelope
from scripts import build_airflow_dag_proof, build_cdc_demo_proof, run_api_smoke


def test_cdc_demo_proof_validates_connector_sql_and_envelope():
    connector = build_cdc_demo_proof.validate_connector_config()
    build_cdc_demo_proof.validate_demo_sql()

    include_list = connector["config"]["table.include.list"]
    assert "public.source_customers_cdc_demo" in include_list

    envelopes = [
        debezium_to_normalized_envelope(message, topic=build_cdc_demo_proof.DEMO_TOPIC).as_dict()
        for message in build_cdc_demo_proof.sample_debezium_messages()
    ]
    assert [event["operation_type"] for event in envelopes] == ["insert", "update", "delete"]
    assert envelopes[0]["source_lsn"] == "100001"
    assert envelopes[0]["source_transaction_id"] == "9001"
    assert envelopes[0]["kafka_topic"] == build_cdc_demo_proof.DEMO_TOPIC
    partition, offset = build_cdc_demo_proof.DEMO_KAFKA_METADATA[0]
    with_kafka_metadata = debezium_to_normalized_envelope(
        build_cdc_demo_proof.sample_debezium_messages()[0],
        topic=build_cdc_demo_proof.DEMO_TOPIC,
        kafka_partition=partition,
        kafka_offset=offset,
    ).as_dict()
    assert with_kafka_metadata["kafka_partition"] == partition
    assert with_kafka_metadata["kafka_offset"] == offset

    proof = build_cdc_demo_proof.build_proof_markdown()
    assert "Debezium-compatible local CDC proof" in proof
    assert "source_customers_cdc_demo" in proof
    assert "Sample Debezium-compatible update message" in proof


def test_airflow_proof_parses_dag_groups_and_dependencies():
    parsed = build_airflow_dag_proof.parse_dag_source()

    assert parsed["dag_id"] == "customer_360_cdc_platform"
    assert parsed["dependency_order"] == [
        "start",
        "source_readiness",
        "cdc_validation",
        "identity_and_model_validation",
        "quality",
        "privacy_and_activation",
        "observability",
        "end",
    ]
    assert "end" not in parsed["task_groups"]["observability"]
    assert len(parsed["task_groups"]["cdc_validation"]) == 3
    assert len(parsed["task_groups"]["observability"]) == 4
    assert {"emit_lineage", "quality_scorecard"}.issubset(
        parsed["task_groups"]["observability"]
    )
    proof = build_airflow_dag_proof.build_proof_markdown()
    assert "Python source compile: `passed`" in proof
    assert "This proof does not claim a successful scheduled DAG run." in proof


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def test_api_smoke_route_source_proof_uses_tenant_outputs_and_masks_pii(tmp_path: Path, monkeypatch):
    (tmp_path / "api").mkdir()
    (tmp_path / "api/main.py").write_text(
        '\n'.join(
            [
                '@app.get("/health")',
                '@app.get("/customers/{canonical_customer_id}/profile"',
                '@app.get("/customers/{canonical_customer_id}/identity-lineage"',
                '@app.get("/exports/churn-risk"',
                '@app.get("/observability/pipeline-health"',
            ]
        ),
        encoding="utf-8",
    )
    _write_csv(
        tmp_path / "reverse_etl/exports/churn_risk_export.csv",
        [
            {
                "tenant_id": "tenant_us",
                "canonical_customer_id": "cc_test",
                "last_refresh_time": "2026-06-01T00:00:00Z",
            }
        ],
    )
    _write_csv(
        tmp_path / "identity_resolution/output/dim_customer_canonical.csv",
        [
            {
                "tenant_id": "tenant_us",
                "canonical_customer_id": "cc_test",
                "primary_email": "person@example.com",
                "primary_phone": "+14155550123",
            }
        ],
    )
    _write_csv(
        tmp_path / "identity_resolution/output/identity_link_explanation.csv",
        [{"canonical_customer_id": "cc_test", "match_rule": "exact_normalized_email"}],
    )
    _write_csv(
        tmp_path / "observability/output/pipeline_run_log.csv",
        [{"tenant_id": "tenant_us", "started_at": "2026-06-01T00:00:00Z", "status": "success"}],
    )

    monkeypatch.setattr(run_api_smoke, "ROOT", tmp_path)
    proof = run_api_smoke._route_source_proof()

    assert proof["validation_mode"] == "route_source_and_generated_output_validation"
    assert proof["customer_profile"]["canonical_customer"]["canonical_customer_id"] == "cc_test"
    assert "primary_email" not in proof["customer_profile"]["canonical_customer"]
    assert proof["churn_export"]["rows"][0]["last_refresh_time"] == "<local_smoke_timestamp>"
