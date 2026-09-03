"""Field-level Golden Customer survivorship with provenance and manual overrides."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from identity_resolution.fuzzy import SOURCE_TRUST


@dataclass(frozen=True)
class GoldenSource:
    tenant_id: str
    canonical_customer_id: str
    source_system: str
    source_record_id: str
    observed_at: str
    values: dict[str, Any]
    verified_fields: frozenset[str] = frozenset()


@dataclass(frozen=True)
class FieldProvenance:
    field: str
    value: Any
    source_system: str
    source_record_id: str
    observed_at: str
    rule: str
    is_manual_override: bool


@dataclass(frozen=True)
class StewardOverride:
    tenant_id: str
    canonical_customer_id: str
    field: str
    value: Any
    steward: str
    reason: str
    decided_at: str


def build_golden_customer(
    sources: Iterable[GoldenSource], *, overrides: Iterable[StewardOverride] = ()
) -> tuple[dict[str, Any], dict[str, FieldProvenance], list[dict[str, Any]]]:
    rows = list(sources)
    if not rows:
        raise ValueError("at least one source record is required")
    tenant, canonical = rows[0].tenant_id, rows[0].canonical_customer_id
    if any(row.tenant_id != tenant or row.canonical_customer_id != canonical for row in rows):
        raise ValueError("golden customer inputs must share tenant and canonical customer")
    candidates: dict[str, list[tuple[tuple[float, int, str, str], GoldenSource, Any]]] = {}
    for row in rows:
        for field, value in row.values.items():
            if value in (None, ""):
                continue
            rank = (SOURCE_TRUST.get(row.source_system, 0.4), int(field in row.verified_fields), row.observed_at, row.source_record_id)
            candidates.setdefault(field, []).append((rank, row, value))
    golden: dict[str, Any] = {"tenant_id": tenant, "canonical_customer_id": canonical}
    provenance: dict[str, FieldProvenance] = {}
    for field, options in sorted(candidates.items()):
        _rank, source, value = max(options, key=lambda item: item[0])
        golden[field] = value
        provenance[field] = FieldProvenance(
            field, value, source.source_system, source.source_record_id, source.observed_at,
            "source_trust_then_verified_then_recency", False,
        )
    audit: list[dict[str, Any]] = []
    for override in sorted(overrides, key=lambda row: (row.decided_at, row.field)):
        if override.tenant_id != tenant or override.canonical_customer_id != canonical:
            raise ValueError("cross-tenant or cross-customer override is forbidden")
        previous = golden.get(override.field)
        golden[override.field] = override.value
        provenance[override.field] = FieldProvenance(
            override.field, override.value, "manual_steward", override.steward,
            override.decided_at, "audited_manual_override", True,
        )
        audit.append({
            "tenant_id": tenant, "canonical_customer_id": canonical, "field": override.field,
            "previous_value": previous, "new_value": override.value, "steward": override.steward,
            "reason": override.reason, "decided_at": override.decided_at,
        })
    return golden, provenance, audit
