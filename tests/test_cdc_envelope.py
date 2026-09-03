from data_generation.cdc_generator import build_cdc_events
from ingestion.cdc_envelope import normalize_event


def test_normalize_valid_cdc_event_routes_to_topic():
    raw = next(event for event in build_cdc_events(seed=7) if event.operation_type == "insert").__dict__
    envelope = normalize_event(raw)
    assert envelope.topic_name.startswith("cdc.")
    assert envelope.operation_type == "insert"
    assert envelope.envelope_hash


def test_rejects_invalid_operation_type():
    raw = build_cdc_events(seed=7)[0].__dict__.copy()
    raw["operation_type"] = "upsert"
    try:
        normalize_event(raw)
    except ValueError as exc:
        assert "unsupported operation_type" in str(exc)
    else:
        raise AssertionError("invalid operation should have been rejected")


def test_rejects_payload_contract_violation():
    raw = next(event for event in build_cdc_events(seed=7) if event.source_table == "customers").__dict__
    raw["payload_after"] = raw["payload_after"].copy()
    raw["payload_after"].pop("customer_id")
    try:
        normalize_event(raw)
    except ValueError as exc:
        assert "payload contract violation" in str(exc)
    else:
        raise AssertionError("contract violation should have been rejected")
