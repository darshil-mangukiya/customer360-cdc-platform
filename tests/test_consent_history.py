from dataclasses import replace

from data_generation.cdc_generator import build_cdc_events
from identity_resolution.resolver import resolve_identity
from ingestion.loader import normalize_records
from privacy.activation_policy import build_consent_state_versions


def test_consent_versions_preserve_opt_in_before_opt_out():
    events, _ = normalize_records([event.__dict__ for event in build_cdc_events(seed=42)])
    canonical, mappings, _ = resolve_identity(events)
    assert canonical
    marketing = next(event for event in events if event.source_table == "marketing_engagement")
    opted_in_payload = {
        **(marketing.payload_after or {}),
        "marketing_consent_status": "opted_in",
        "unsubscribe_status": "subscribed",
        "do_not_contact_flag": False,
        "email_opt_in": True,
    }
    opted_out_payload = {
        **opted_in_payload,
        "marketing_consent_status": "opted_out",
        "unsubscribe_status": "unsubscribed",
        "do_not_contact_flag": True,
        "email_opt_in": False,
    }
    first = replace(
        marketing,
        event_id="evt_consent_in",
        event_hash="hash_consent_in",
        event_timestamp="2026-01-01T00:00:00Z",
        payload_after=opted_in_payload,
    )
    second = replace(
        marketing,
        event_id="evt_consent_out",
        event_hash="hash_consent_out",
        event_timestamp="2026-02-01T00:00:00Z",
        payload_after=opted_out_payload,
    )
    versions = build_consent_state_versions([first, second], mappings)
    assert [row.state for row in versions] == ["opted_in", "opted_out"]
    assert versions[0].valid_to == "2026-02-01T00:00:00Z"
    assert versions[1].attributes["do_not_contact_flag"] is True
