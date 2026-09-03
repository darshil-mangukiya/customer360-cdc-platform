from data_generation.cdc_generator import build_cdc_events
from identity_resolution.resolver import resolve_identity
from ingestion.loader import normalize_records


def test_identity_resolution_merges_duplicate_source_records():
    raw = [event.__dict__ for event in build_cdc_events(seed=11)]
    landed, rejected = normalize_records(raw)
    canonical, mappings, audit = resolve_identity(landed)
    assert rejected
    assert len(canonical) < len({(e.source_system, e.record_primary_key) for e in landed})
    assert all(row.canonical_customer_id.startswith("cc_") for row in canonical)
    assert len(mappings) == len(audit)


def test_identity_resolution_does_not_merge_same_identifier_across_tenants():
    def customer_event(tenant_id: str, customer_id: str) -> dict:
        return {
            "event_id": f"evt_{tenant_id}",
            "source_system": "account_app",
            "source_table": "customers",
            "operation_type": "insert",
            "event_timestamp": "2026-01-01T00:00:00Z",
            "record_primary_key": customer_id,
            "payload_before": None,
            "payload_after": {
                "customer_id": customer_id,
                "external_account_id": "shared_account_123",
                "email": "shared@example.com",
                "phone": "+15551234567",
                "first_name": "Shared",
                "last_name": "Customer",
                "tenant_id": tenant_id,
                "business_unit": "self_serve",
                "customer_status": "active",
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
            },
            "batch_id": "batch_cross_tenant",
            "schema_version": 1,
            "tenant_id": tenant_id,
        }

    landed, rejected = normalize_records(
        [
            customer_event("tenant_us", "cust_shared_us"),
            customer_event("tenant_eu", "cust_shared_eu"),
        ]
    )
    canonical, mappings, _ = resolve_identity(landed)

    assert not rejected
    assert len(canonical) == 2
    assert {row.tenant_id for row in canonical} == {"tenant_us", "tenant_eu"}
    assert len({row.canonical_customer_id for row in mappings}) == 2
