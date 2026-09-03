from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from customer360.history import HistoryVersion, build_scd2_history
from identity_resolution.resolver import CanonicalCustomer, IdentityMapRow
from ingestion.cdc_envelope import NormalizedEnvelope


@dataclass(frozen=True)
class CustomerConsentState:
    tenant_id: str | None
    canonical_customer_id: str
    marketing_consent_status: str
    email_opt_in: bool
    sms_opt_in: bool
    push_opt_in: bool
    unsubscribe_status: str
    do_not_contact_flag: bool
    deletion_requested_flag: bool
    deletion_request_timestamp: str | None
    last_consent_event_at: str | None


@dataclass(frozen=True)
class SuppressedCustomer:
    tenant_id: str | None
    canonical_customer_id: str
    activation_suppression_reason: str
    marketing_consent_status: str
    email_opt_in: bool
    sms_opt_in: bool
    push_opt_in: bool
    do_not_contact_flag: bool
    deletion_requested_flag: bool
    suppressed_at: str


def _now() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _tenant_scope(tenant_id: str | None) -> str:
    return tenant_id or "tenant_unknown"


def _source_to_canonical(mappings: list[IdentityMapRow]) -> dict[tuple[str, str, str], str]:
    return {
        (_tenant_scope(row.tenant_id), row.source_system, row.source_record_id): row.canonical_customer_id
        for row in mappings
    }


def build_consent_history(
    events: list[NormalizedEnvelope],
    mappings: list[IdentityMapRow],
    *,
    deletion_requests: list[dict[str, Any]] | None = None,
) -> dict[str, CustomerConsentState]:
    canonical_by_source = _source_to_canonical(mappings)
    deletion_scopes = {
        (row.get("tenant_id"), row.get("canonical_customer_id"))
        for row in deletion_requests or []
        if row.get("canonical_customer_id")
    }
    state: dict[str, CustomerConsentState] = {}
    for event in sorted(events, key=lambda row: (row.event_timestamp, row.event_id)):
        if event.source_table != "marketing_engagement":
            continue
        canonical_id = canonical_by_source.get((_tenant_scope(event.tenant_id), event.source_system, event.record_primary_key))
        payload = event.payload_after or event.payload_before or {}
        if not canonical_id or not payload:
            continue
        state[canonical_id] = CustomerConsentState(
            tenant_id=event.tenant_id,
            canonical_customer_id=canonical_id,
            marketing_consent_status=str(payload.get("marketing_consent_status") or "unknown"),
            email_opt_in=bool(payload.get("email_opt_in")),
            sms_opt_in=bool(payload.get("sms_opt_in")),
            push_opt_in=bool(payload.get("push_opt_in")),
            unsubscribe_status=str(payload.get("unsubscribe_status") or "unknown"),
            do_not_contact_flag=bool(payload.get("do_not_contact_flag")),
            deletion_requested_flag=(event.tenant_id, canonical_id) in deletion_scopes
            or (None, canonical_id) in deletion_scopes,
            deletion_request_timestamp=None,
            last_consent_event_at=event.event_timestamp,
        )

    for deletion_tenant_id, canonical_id in deletion_scopes:
        previous = state.get(canonical_id)
        state[canonical_id] = CustomerConsentState(
            tenant_id=deletion_tenant_id or (previous.tenant_id if previous else None),
            canonical_customer_id=canonical_id,
            marketing_consent_status=previous.marketing_consent_status if previous else "unknown",
            email_opt_in=previous.email_opt_in if previous else False,
            sms_opt_in=previous.sms_opt_in if previous else False,
            push_opt_in=previous.push_opt_in if previous else False,
            unsubscribe_status=previous.unsubscribe_status if previous else "unknown",
            do_not_contact_flag=previous.do_not_contact_flag if previous else True,
            deletion_requested_flag=True,
            deletion_request_timestamp=None,
            last_consent_event_at=previous.last_consent_event_at if previous else None,
        )
    return state


def build_consent_state_versions(
    events: list[NormalizedEnvelope], mappings: list[IdentityMapRow]
) -> list[HistoryVersion]:
    """Return effective-dated consent versions instead of overwriting prior state."""
    canonical_by_source = _source_to_canonical(mappings)
    changes: list[dict[str, Any]] = []
    for event in events:
        if event.source_table != "marketing_engagement":
            continue
        canonical_id = canonical_by_source.get(
            (_tenant_scope(event.tenant_id), event.source_system, event.record_primary_key)
        )
        payload = event.payload_after or event.payload_before or {}
        if not canonical_id or not payload:
            continue
        changes.append(
            {
                "tenant_id": event.tenant_id,
                "entity_id": canonical_id,
                "effective_timestamp": event.event_timestamp,
                "consent_state": str(payload.get("marketing_consent_status") or "unknown"),
                "source_event_id": event.event_id,
                "change_reason": "source_delete" if event.operation_type == "delete" else "consent_change",
                "is_deleted": event.operation_type == "delete",
                "attributes": {
                    "email_opt_in": bool(payload.get("email_opt_in")),
                    "sms_opt_in": bool(payload.get("sms_opt_in")),
                    "push_opt_in": bool(payload.get("push_opt_in")),
                    "unsubscribe_status": str(payload.get("unsubscribe_status") or "unknown"),
                    "do_not_contact_flag": bool(payload.get("do_not_contact_flag")),
                },
            }
        )
    return build_scd2_history(changes, state_field="consent_state")


def suppression_reason(
    *,
    customer: CanonicalCustomer,
    consent: CustomerConsentState | None,
    requires_destination_identifier: bool = True,
) -> str | None:
    if consent is None:
        return "missing_consent_state"
    if consent and consent.deletion_requested_flag:
        return "privacy_deletion_requested"
    if consent and consent.do_not_contact_flag:
        return "do_not_contact"
    if consent and consent.unsubscribe_status == "unsubscribed":
        return "marketing_unsubscribed"
    if consent and consent.marketing_consent_status == "opted_out":
        return "marketing_consent_opted_out"
    if consent and consent.marketing_consent_status != "opted_in":
        return "unknown_consent_state"
    if consent and not any([consent.email_opt_in, consent.sms_opt_in, consent.push_opt_in]):
        return "no_active_channel_consent"
    if requires_destination_identifier and not (customer.primary_email or customer.primary_phone):
        return "missing_destination_identifier"
    return None


def build_suppressed_customers(
    *,
    events: list[NormalizedEnvelope],
    canonical: list[CanonicalCustomer],
    mappings: list[IdentityMapRow],
    deletion_requests: list[dict[str, Any]] | None = None,
) -> tuple[dict[str, CustomerConsentState], list[SuppressedCustomer]]:
    consent_by_customer = build_consent_history(events, mappings, deletion_requests=deletion_requests)
    suppressed_at = _now()
    suppressed: list[SuppressedCustomer] = []
    for customer in canonical:
        consent = consent_by_customer.get(customer.canonical_customer_id)
        reason = suppression_reason(customer=customer, consent=consent)
        if not reason:
            continue
        suppressed.append(
            SuppressedCustomer(
                tenant_id=customer.tenant_id,
                canonical_customer_id=customer.canonical_customer_id,
                activation_suppression_reason=reason,
                marketing_consent_status=consent.marketing_consent_status if consent else "unknown",
                email_opt_in=consent.email_opt_in if consent else False,
                sms_opt_in=consent.sms_opt_in if consent else False,
                push_opt_in=consent.push_opt_in if consent else False,
                do_not_contact_flag=consent.do_not_contact_flag if consent else False,
                deletion_requested_flag=consent.deletion_requested_flag if consent else False,
                suppressed_at=suppressed_at,
            )
        )
    return consent_by_customer, suppressed


def write_privacy_activation_outputs(
    *,
    consent_by_customer: dict[str, CustomerConsentState],
    suppressed: list[SuppressedCustomer],
    output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "customer_consent_history.csv": [asdict(row) for row in consent_by_customer.values()],
        "export_suppressed_customers.csv": [asdict(row) for row in suppressed],
        "activation_suppression_list.csv": [
            {
                "canonical_customer_id": row.canonical_customer_id,
                "tenant_id": row.tenant_id,
                "activation_suppression_reason": row.activation_suppression_reason,
                "suppressed_at": row.suppressed_at,
            }
            for row in suppressed
        ],
    }
    fallback_fields = {
        "customer_consent_history.csv": [field.name for field in CustomerConsentState.__dataclass_fields__.values()],
        "export_suppressed_customers.csv": [field.name for field in SuppressedCustomer.__dataclass_fields__.values()],
        "activation_suppression_list.csv": [
            "canonical_customer_id",
            "tenant_id",
            "activation_suppression_reason",
            "suppressed_at",
        ],
    }
    for filename, rows in outputs.items():
        with (output_dir / filename).open("w", newline="", encoding="utf-8") as fh:
            fieldnames = list(rows[0].keys()) if rows else fallback_fields[filename]
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
