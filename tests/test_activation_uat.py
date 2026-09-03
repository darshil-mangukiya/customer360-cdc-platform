"""Business-oriented UAT suite for reverse ETL activation (spec section 22).

Each test maps to one modeled business/operational rule, not a generic technical
assertion. This is deliberately a small, curated set — one meaningful case per rule —
not a large matrix of near-duplicate PASS rows. Requirements traceability for these
cases lives in `docs/activation_reconciliation.md` (case IDs: ACT-UAT-00N below).
"""

from __future__ import annotations

from pathlib import Path

from identity_resolution.resolver import resolve_identity
from ingestion.loader import normalize_records
from privacy.activation_policy import build_suppressed_customers
from reverse_etl.destinations.simulator import DESTINATIONS, _deterministic_bucket, simulate_destination_sync
from reverse_etl.exporter import build_activation_rows


def _event(*, event_id, source_system, source_table, record_id, tenant_id, payload, timestamp="2026-01-01T00:00:00Z") -> dict:
    return {
        "event_id": event_id,
        "source_system": source_system,
        "source_table": source_table,
        "operation_type": "insert",
        "event_timestamp": timestamp,
        "record_primary_key": record_id,
        "payload_before": None,
        "payload_after": {**payload, "tenant_id": tenant_id},
        "batch_id": "batch_uat",
        "schema_version": 1,
        "tenant_id": tenant_id,
    }


def _customer_event(customer_id: str, tenant_id: str, *, email: str, external_account_id: str) -> dict:
    return _event(
        event_id=f"evt_cust_{customer_id}",
        source_system="account_app",
        source_table="customers",
        record_id=customer_id,
        tenant_id=tenant_id,
        payload={
            "customer_id": customer_id,
            "external_account_id": external_account_id,
            "business_unit": "self_serve",
            "email": email,
            "customer_status": "active",
            "updated_at": "2026-01-01T00:00:00Z",
        },
    )


def _marketing_event(touch_id: str, tenant_id: str, *, external_account_id: str, email: str, consent_status: str, do_not_contact: bool = False) -> dict:
    return _event(
        event_id=f"evt_mkt_{touch_id}",
        source_system="marketing_automation",
        source_table="marketing_engagement",
        record_id=touch_id,
        tenant_id=tenant_id,
        payload={
            "marketing_touch_id": touch_id,
            "business_unit": "self_serve",
            "external_account_id": external_account_id,
            "email": email,
            "channel": "email",
            "campaign_id": "camp_1",
            "engagement_status": "opened",
            "marketing_consent_status": consent_status,
            "email_opt_in": consent_status == "opted_in",
            "sms_opt_in": False,
            "push_opt_in": False,
            "unsubscribe_status": "unsubscribed" if consent_status == "opted_out" else "subscribed",
            "do_not_contact_flag": do_not_contact,
            "lead_score": 10,
            "occurred_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
        },
    )


def _support_event(case_id: str, tenant_id: str, *, support_customer_ref: str, csat_score: int, priority: str = "high") -> dict:
    return _event(
        event_id=f"evt_support_{case_id}",
        source_system="support_desk",
        source_table="support_interactions",
        record_id=case_id,
        tenant_id=tenant_id,
        payload={
            "support_interaction_id": case_id,
            "business_unit": "self_serve",
            "support_customer_ref": support_customer_ref,
            "reason": "billing_issue",
            "priority": priority,
            "status": "open",
            "csat_score": csat_score,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
        },
    )


def test_act_uat_001_opted_out_customer_never_appears_in_any_activation_export():
    """ACT-UAT-001. Given: customer has marketing_consent_status=opted_out.
    Expected: customer must not appear in any activation export (not just campaigns)."""
    raw = [
        _customer_event("cust_optout", "tenant_us", email="optout@example.com", external_account_id="acct_ext_optout"),
        _marketing_event(
            "mkt_optout",
            "tenant_us",
            external_account_id="acct_ext_optout",
            email="optout@example.com",
            consent_status="opted_out",
        ),
    ]
    landed, _rejected = normalize_records(raw)
    canonical, mappings, _audit = resolve_identity(landed)
    rows = build_activation_rows(landed, canonical, mappings)
    assert not any(row.canonical_customer_id == canonical[0].canonical_customer_id for row in rows)


def test_act_uat_002_deletion_request_makes_customer_activation_ineligible():
    """ACT-UAT-002. Given: an approved deletion request for a customer.
    Expected: customer becomes activation-ineligible (excluded from export build),
    matching the deletion-aware suppression path `reverse_etl.exporter.main` uses."""
    raw = [_customer_event("cust_delete", "tenant_us", email="delete@example.com", external_account_id="acct_ext_delete")]
    landed, _rejected = normalize_records(raw)
    canonical, mappings, _audit = resolve_identity(landed)
    target_id = canonical[0].canonical_customer_id

    all_rows = build_activation_rows(landed, canonical, mappings, exclude_suppressed=False)
    assert any(row.canonical_customer_id == target_id for row in all_rows), "sanity: customer is eligible before deletion"

    _consent, suppressed = build_suppressed_customers(
        events=landed,
        canonical=canonical,
        mappings=mappings,
        deletion_requests=[{"canonical_customer_id": target_id}],
    )
    suppressed_ids = {row.canonical_customer_id for row in suppressed}
    assert target_id in suppressed_ids
    remaining = [row for row in all_rows if row.canonical_customer_id not in suppressed_ids]
    assert not any(row.canonical_customer_id == target_id for row in remaining)


def test_act_uat_003_low_csat_support_case_elevates_support_priority():
    """ACT-UAT-003. Given: customer has a low-CSAT (<=2) open support interaction.
    Expected: customer's support_priority export value is elevated to
    p2_csat_recovery (the implemented support-priority escalation rule), not
    'standard'."""
    raw = [
        _customer_event("cust_csat", "tenant_us", email="csat@example.com", external_account_id="acct_ext_csat"),
        _marketing_event(
            "mkt_csat",
            "tenant_us",
            external_account_id="acct_ext_csat",
            email="csat@example.com",
            consent_status="opted_in",
        ),
        _support_event("case_csat", "tenant_us", support_customer_ref="acct_ext_csat", csat_score=1),
    ]
    landed, _rejected = normalize_records(raw)
    canonical, mappings, _audit = resolve_identity(landed)
    rows = build_activation_rows(landed, canonical, mappings)
    assert rows
    assert rows[0].support_priority == "p2_csat_recovery"


def _clean_customer_id(destination: str, tenant_id: str, prefix: str) -> str:
    for i in range(2000):
        candidate = f"{prefix}{i}"
        bucket = _deterministic_bucket(destination, {"tenant_id": tenant_id, "canonical_customer_id": candidate})
        if bucket % 29 and bucket % 17 and bucket % 23:
            return candidate
    raise AssertionError("could not find a deterministically-clean customer id for this test")


def test_act_uat_004_repeated_sync_run_does_not_duplicate_a_successful_update(tmp_path: Path):
    """ACT-UAT-004. Given: an identical destination payload synced twice in a row.
    Expected: the second run marks the row skipped_unchanged (idempotent), not a
    second 'inserted'/'updated' success — the destination simulator does not create
    a duplicate successful update for unchanged data."""
    config = DESTINATIONS[0]
    customer_id = _clean_customer_id(config.destination_name, "tenant_us", "cc_idem_")
    export_dir = tmp_path / "exports"
    output_dir = tmp_path / "sync_logs"
    export_dir.mkdir()
    row = {
        "canonical_customer_id": customer_id,
        "tenant_id": "tenant_us",
        "business_unit": "self_serve",
        "email_sha256": "a" * 64,
        "phone_sha256": "",
        "export_timestamp": "2026-01-01T00:00:00Z",
        "campaign_target": "product_education",
        "customer_segment": "self_serve_core",
        "churn_risk_band": "low",
    }
    import csv

    with (export_dir / config.export_file).open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(row.keys()))
        writer.writeheader()
        writer.writerow(row)

    simulate_destination_sync(export_dir=export_dir, output_dir=output_dir, destinations=[config])
    first_state = {r["canonical_customer_id"]: r["sync_action"] for r in _read_csv(output_dir / "destination_sync_state.csv")}
    assert first_state[customer_id] == "inserted"

    simulate_destination_sync(export_dir=export_dir, output_dir=output_dir, destinations=[config])
    second_state = {r["canonical_customer_id"]: r["sync_action"] for r in _read_csv(output_dir / "destination_sync_state.csv")}
    assert second_state[customer_id] == "skipped_unchanged"


def _read_csv(path: Path) -> list[dict]:
    import csv

    with path.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def test_act_uat_005_same_identifier_in_two_tenants_never_merges_or_leaks_activation():
    """ACT-UAT-005. Given: the same email/external_account_id appears in Tenant A and
    Tenant B. Expected: no cross-tenant identity merge, and each tenant's activation
    rows are attributed only to that tenant's canonical customer."""
    raw = [
        _customer_event("cust_a", "tenant_a", email="shared@example.com", external_account_id="acct_ext_shared"),
        _customer_event("cust_b", "tenant_b", email="shared@example.com", external_account_id="acct_ext_shared"),
        _marketing_event(
            "mkt_a",
            "tenant_a",
            external_account_id="acct_ext_shared",
            email="shared@example.com",
            consent_status="opted_in",
        ),
        _marketing_event(
            "mkt_b",
            "tenant_b",
            external_account_id="acct_ext_shared",
            email="shared@example.com",
            consent_status="opted_in",
        ),
    ]
    landed, _rejected = normalize_records(raw)
    canonical, mappings, _audit = resolve_identity(landed)
    assert len(canonical) == 2
    assert {c.tenant_id for c in canonical} == {"tenant_a", "tenant_b"}

    rows = build_activation_rows(landed, canonical, mappings)
    rows_by_tenant = {row.tenant_id: row.canonical_customer_id for row in rows}
    assert len(rows_by_tenant) == 2
    assert rows_by_tenant["tenant_a"] != rows_by_tenant["tenant_b"]
