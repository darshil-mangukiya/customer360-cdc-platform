from dataclasses import replace

from data_generation.cdc_generator import build_cdc_events
from ingestion.cdc_envelope import CDCValidationError, normalize_event
from ingestion.cdc_state import (
    build_deduplication_log,
    build_ordering_anomalies,
    build_watermark_checkpoints,
    filter_idempotent_events,
    materialize_latest_state,
)
from ingestion.loader import normalize_records


def _events():
    raw = [event.__dict__ for event in build_cdc_events(seed=17)]
    landed, rejected = normalize_records(raw, deduplicate=False)
    assert all(row["failure_category"] == "UNKNOWN_OPERATION" for row in rejected)
    return landed


def test_rejection_has_stable_category_and_forensic_fields():
    raw = build_cdc_events(seed=17)[0].__dict__.copy()
    raw["schema_version"] = 99
    landed, rejected = normalize_records([raw])
    assert not landed
    assert rejected[0]["failure_category"] == "UNSUPPORTED_SCHEMA_VERSION"
    assert rejected[0]["failure_stage"] == "contract_validation"
    assert rejected[0]["event_reference"] == raw["event_hash"]
    assert rejected[0]["retry_eligible"] is False


def test_missing_tenant_and_source_key_are_classified():
    raw = build_cdc_events(seed=17)[0].__dict__.copy()
    raw["record_primary_key"] = ""
    try:
        normalize_event(raw)
    except CDCValidationError as exc:
        assert exc.category == "MISSING_SOURCE_KEY"
    else:
        raise AssertionError("missing source key was accepted")

    raw = build_cdc_events(seed=17)[0].__dict__.copy()
    raw["tenant_id"] = None
    payload_key = "payload_after" if raw.get("payload_after") else "payload_before"
    raw[payload_key] = {**raw[payload_key], "tenant_id": None}
    try:
        normalize_event(raw)
    except CDCValidationError as exc:
        assert exc.category == "INVALID_TENANT"
    else:
        raise AssertionError("missing tenant was accepted")


def test_duplicate_id_hash_and_offset_are_idempotent():
    first, second, third = _events()[:3]
    same_id = replace(first, event_hash="different_hash", envelope_hash="different_envelope")
    same_hash = replace(second, event_id="evt_new_hash_duplicate", event_hash=first.event_hash)
    same_offset = replace(
        third,
        event_id="evt_new_offset_duplicate",
        event_hash="unique_hash",
        envelope_hash="unique_envelope",
        kafka_topic=first.kafka_topic,
        kafka_partition=first.kafka_partition,
        kafka_offset=first.kafka_offset,
    )
    rows = build_deduplication_log([first, same_id, same_hash, same_offset])
    assert [row.duplicate_key for row in rows] == [None, "event_id", "event_hash", "kafka_offset"]
    assert len(filter_idempotent_events([first, same_id, same_hash, same_offset])) == 1


def test_late_event_is_reported_but_does_not_replace_latest_state():
    base = _events()[0]
    newest = replace(base, event_id="evt_new", event_hash="hash_new", event_sequence_number=20, kafka_offset=20)
    late = replace(base, event_id="evt_late", event_hash="hash_late", event_sequence_number=10, kafka_offset=10)
    after = replace(base, event_id="evt_after", event_hash="hash_after", event_sequence_number=30, kafka_offset=30)
    anomalies = build_ordering_anomalies([newest, late, after])
    assert [row.event_id for row in anomalies] == ["evt_late"]
    state = materialize_latest_state([newest, late, after])
    assert next(iter(state.values())).event_id == "evt_after"


def test_checkpoint_captures_high_watermark_run_and_replay_metadata():
    event = _events()[0]
    checkpoint = build_watermark_checkpoints(
        [event], run_id="run_001", replay_start="2026-01-01T00:00:00Z"
    )[0]
    assert checkpoint.high_watermark == event.source_commit_timestamp
    assert checkpoint.last_successful_run_id == "run_001"
    assert checkpoint.replay_start == "2026-01-01T00:00:00Z"
