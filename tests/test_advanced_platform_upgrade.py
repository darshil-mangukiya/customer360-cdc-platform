import csv
from dataclasses import replace
from pathlib import Path

from fastapi import HTTPException

from api.export_registry import resolve_export_filename
from data_generation.cdc_generator import build_cdc_events
from identity_resolution.resolver import build_identity_graph_artifacts, resolve_identity
from ingestion.cdc_envelope import normalize_event
from ingestion.cdc_state import build_deduplication_log, build_ordering_anomalies, filter_idempotent_events
from ingestion.loader import normalize_records
from ingestion.replay import write_replay_records
from privacy.activation_policy import build_suppressed_customers
from reverse_etl.destinations.simulator import DestinationConfig, simulate_destination_sync
from reverse_etl.exporter import build_activation_rows


def test_cdc_envelope_contains_tenant_and_source_log_metadata():
    raw = next(event for event in build_cdc_events(seed=7) if event.operation_type == "insert").__dict__
    envelope = normalize_event(raw)
    assert envelope.tenant_id.startswith("tenant_")
    assert envelope.event_sequence_number > 0
    assert envelope.source_lsn
    assert envelope.source_commit_timestamp == envelope.event_timestamp
    assert envelope.kafka_topic == envelope.topic_name
    assert envelope.kafka_partition in {0, 1, 2}
    assert envelope.kafka_offset is not None
    assert envelope.event_hash


def test_cdc_deduplication_and_out_of_order_detection():
    events, _ = normalize_records([event.__dict__ for event in build_cdc_events(seed=42)], deduplicate=False)
    duplicate = replace(events[0], event_id="evt_duplicate_same_hash")
    dedupe_log = build_deduplication_log([events[0], duplicate])
    assert [row.dedupe_status for row in dedupe_log] == ["accepted", "duplicate"]
    assert len(filter_idempotent_events([events[0], duplicate])) == 1

    first = replace(events[0], record_primary_key="same_pk", event_sequence_number=10)
    second = replace(
        events[1],
        record_primary_key="same_pk",
        event_sequence_number=5,
        source_system=first.source_system,
        source_table=first.source_table,
    )
    anomalies = build_ordering_anomalies([first, second])
    assert anomalies
    assert anomalies[0].anomaly_type in {"out_of_order_sequence", "late_arriving_commit"}


def test_replay_writer_marks_selected_records_as_replay(tmp_path: Path):
    output = tmp_path / "replay.jsonl"
    raw = [build_cdc_events(seed=3)[0].__dict__]
    write_replay_records(raw, output, replay_run_id="replay_test_001")
    replayed = output.read_text(encoding="utf-8")
    assert '"is_replay": true' in replayed
    assert '"replay_batch_id": "replay_test_001"' in replayed


def test_identity_graph_outputs_explanations_and_merge_history():
    landed, _ = normalize_records([event.__dict__ for event in build_cdc_events(seed=11)])
    canonical, mappings, _ = resolve_identity(landed)
    graph = build_identity_graph_artifacts(landed, canonical, mappings)
    assert graph.nodes
    assert graph.edges
    assert graph.match_rules
    assert graph.merge_events
    assert graph.link_explanations
    assert all("linked to" in row.explanation_text for row in graph.link_explanations[:5])


def test_privacy_policy_suppresses_opted_out_customers_from_activation():
    landed, _ = normalize_records([event.__dict__ for event in build_cdc_events(seed=42)])
    canonical, mappings, _ = resolve_identity(landed)
    _, suppressed = build_suppressed_customers(events=landed, canonical=canonical, mappings=mappings)
    activation_rows = build_activation_rows(landed, canonical, mappings)
    suppressed_ids = {row.canonical_customer_id for row in suppressed}
    exported_ids = {row.canonical_customer_id for row in activation_rows}
    assert suppressed_ids
    assert suppressed_ids.isdisjoint(exported_ids)
    assert {"do_not_contact", "marketing_unsubscribed"} & {row.activation_suppression_reason for row in suppressed}


def test_reverse_etl_simulator_writes_payload_audit_and_destination_status(tmp_path: Path):
    export_dir = tmp_path / "exports"
    output_dir = tmp_path / "sync_logs"
    export_dir.mkdir()
    with (export_dir / "customer_segment_export.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "canonical_customer_id",
                "tenant_id",
                "business_unit",
                "email_sha256",
                "phone_sha256",
                "export_timestamp",
                "customer_segment",
                "source_lineage_refs",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "canonical_customer_id": "cc_test",
                "tenant_id": "tenant_us",
                "business_unit": "self_serve",
                "email_sha256": "a" * 64,
                "phone_sha256": "",
                "export_timestamp": "2026-01-01T00:00:00Z",
                "customer_segment": "self_serve_core",
                "source_lineage_refs": "account_app:cust_1",
            }
        )

    simulate_destination_sync(
        export_dir=export_dir,
        output_dir=output_dir,
        destinations=[DestinationConfig("hubspot", "customer_segment_export.csv", "contacts", 5, 2)],
    )
    assert (output_dir / "payload_audit.csv").exists()
    assert (output_dir / "destination_status.csv").exists()
    payload_rows = list(csv.DictReader((output_dir / "payload_audit.csv").open(encoding="utf-8")))
    assert payload_rows[0]["tenant_id"] == "tenant_us"
    assert payload_rows[0]["payload_hash"]


def test_api_auth_tenant_filter_and_profile_endpoint():
    import api.main as api

    try:
        api.require_api_key(None)
    except HTTPException as exc:
        assert exc.status_code == 401
    else:
        raise AssertionError("missing API key should be rejected")

    principal = api.require_api_key("local-dev-key")
    tenants = api.tenants(principal)
    assert tenants

    response = api.export_churn_risk(
        tenant_id="tenant_us", limit=5, offset=0, principal=principal
    )
    assert response.tenant_id == "tenant_us"
    assert all(row["tenant_id"] == "tenant_us" for row in response.rows)

    if response.rows:
        canonical_customer_id = response.rows[0]["canonical_customer_id"]
        profile = api.customer_profile(canonical_customer_id, principal=principal)
        assert profile["canonical_customer"]["canonical_customer_id"] == canonical_customer_id


def test_export_allowlist_accepts_only_known_exports():
    assert resolve_export_filename("customer_segment_export") == "customer_segment_export.csv"
    assert resolve_export_filename("churn_risk_export.csv") == "churn_risk_export.csv"
    assert resolve_export_filename("../../unapproved") is None
