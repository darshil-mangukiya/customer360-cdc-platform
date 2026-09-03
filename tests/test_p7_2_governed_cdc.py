from __future__ import annotations

import json
from pathlib import Path

from identity_resolution.resolver import resolve_identity
from ingestion.loader import normalize_records
from privacy.activation_policy import build_suppressed_customers
from streaming.consumer import consume_to_postgres


ROOT = Path(__file__).resolve().parents[1]


def _customer(event_id: str, tenant_id: str, record_id: str, email: str) -> dict:
    return {
        "event_id": event_id,
        "source_system": "account_app",
        "source_table": "customers",
        "operation_type": "insert",
        "event_timestamp": "2026-09-02T12:00:00Z",
        "record_primary_key": record_id,
        "payload_before": None,
        "payload_after": {
            "customer_id": record_id,
            "external_account_id": f"acct_{record_id}",
            "email": email,
            "tenant_id": tenant_id,
            "business_unit": "self_serve",
            "customer_status": "active",
            "updated_at": "2026-09-02T12:00:00Z",
        },
        "batch_id": "p7_2_test",
        "schema_version": 1,
        "tenant_id": tenant_id,
    }


def test_missing_consent_fails_closed() -> None:
    landed, rejected = normalize_records([_customer("evt_a", "tenant_a", "cust_a", "unknown@example.test")])
    assert not rejected
    canonical, mappings, _audit = resolve_identity(landed)
    _consent, suppressed = build_suppressed_customers(events=landed, canonical=canonical, mappings=mappings)
    assert len(suppressed) == 1
    assert suppressed[0].activation_suppression_reason == "missing_consent_state"


def test_identical_email_never_crosses_tenant_boundary() -> None:
    events = [
        _customer("evt_a", "tenant_a", "cust_a", "same@example.test"),
        _customer("evt_b", "tenant_b", "cust_b", "same@example.test"),
    ]
    landed, rejected = normalize_records(events)
    assert not rejected
    canonical, mappings, _audit = resolve_identity(landed)
    assert len(canonical) == 2
    assert len({row.canonical_customer_id for row in canonical}) == 2
    assert {row.tenant_id for row in mappings} == {"tenant_a", "tenant_b"}


def test_source_contract_and_connector_cover_all_six_domains() -> None:
    connector = json.loads((ROOT / "connect/debezium/postgres_customer_sources.json").read_text())
    included = set(connector["config"]["table.include.list"].split(","))
    contracts = json.loads((ROOT / "contracts/cdc_payload_contracts.json").read_text())
    domains = {
        "customers",
        "subscriptions",
        "orders",
        "engagement_events",
        "support_interactions",
        "marketing_engagement",
    }
    assert {f"public.{domain}" for domain in domains} <= included
    assert domains <= set(contracts)


def test_consumer_defaults_match_connector_routed_topics() -> None:
    # The parser defaults are kept inspectable without contacting Kafka.
    source = (ROOT / "streaming/consumer.py").read_text()
    for topic in (
        "cdc.customers",
        "cdc.subscriptions",
        "cdc.orders",
        "cdc.engagement_events",
        "cdc.support_interactions",
        "cdc.marketing_engagement",
    ):
        assert topic in source
    assert callable(consume_to_postgres)
