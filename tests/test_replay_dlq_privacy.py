from data_generation.cdc_generator import build_cdc_events
from ingestion.dlq_reprocessor import repair_rejected_event
from ingestion.loader import normalize_records
from ingestion.replay import select_replay_records
from privacy.pii import hash_email, mask_email, mask_phone


def test_replay_filters_by_batch_and_table():
    raw = [event.__dict__ for event in build_cdc_events(seed=42)]
    selected = select_replay_records(
        raw,
        batch_ids={"batch_003"},
        source_tables={"subscriptions"},
    )
    assert selected
    assert {row["batch_id"] for row in selected} == {"batch_003"}
    assert {row["source_table"] for row in selected} == {"subscriptions"}


def test_dlq_repair_converts_demo_bad_event_to_valid_cdc():
    bad = build_cdc_events(seed=42)[-1].__dict__
    repaired, reasons = repair_rejected_event({"raw_event": bad, "event_id": bad["event_id"]})
    landed, rejected = normalize_records([repaired])
    assert reasons
    assert len(landed) == 1
    assert not rejected
    assert repaired["operation_type"] == "insert"
    assert repaired["payload_after"]["tenant_id"] == "tenant_unknown"


def test_privacy_helpers_hash_and_mask_identifiers():
    first = hash_email("Alex+trial@Example.com", salt="test")
    second = hash_email("alex@example.com", salt="test")
    assert first == second
    assert len(first or "") == 64
    assert mask_email("alex@example.com") == "a***x@example.com"
    assert mask_phone("+14155550101") == "+14***0101"

