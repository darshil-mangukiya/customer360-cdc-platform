from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Iterable


@dataclass(frozen=True)
class HistoryVersion:
    tenant_id: str
    entity_id: str
    state: str
    valid_from: str
    valid_to: str | None
    is_current: bool
    effective_timestamp: str
    source_event_id: str
    change_reason: str
    is_deleted: bool = False
    attributes: dict[str, Any] | None = None


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def build_scd2_history(changes: Iterable[dict[str, Any]], *, state_field: str) -> list[HistoryVersion]:
    """Build tenant-scoped SCD2 intervals from effective-dated changes."""
    ordered = sorted(
        changes,
        key=lambda row: (
            str(row["tenant_id"]),
            str(row["entity_id"]),
            _instant(str(row["effective_timestamp"])),
            str(row["source_event_id"]),
        ),
    )
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in ordered:
        tenant_id = str(row.get("tenant_id") or "")
        entity_id = str(row.get("entity_id") or "")
        if not tenant_id or not entity_id:
            raise ValueError("tenant_id and entity_id are required for history")
        grouped.setdefault((tenant_id, entity_id), []).append(row)

    versions: list[HistoryVersion] = []
    for (tenant_id, entity_id), rows in sorted(grouped.items()):
        entity_versions: list[HistoryVersion] = []
        for row in rows:
            deleted = bool(row.get("is_deleted"))
            state = "deleted" if deleted else str(row.get(state_field) or "unknown")
            effective = str(row["effective_timestamp"])
            candidate = HistoryVersion(
                tenant_id=tenant_id,
                entity_id=entity_id,
                state=state,
                valid_from=effective,
                valid_to=None,
                is_current=not deleted,
                effective_timestamp=effective,
                source_event_id=str(row["source_event_id"]),
                change_reason=str(row.get("change_reason") or ("delete" if deleted else "source_update")),
                is_deleted=deleted,
                attributes=dict(row.get("attributes") or {}),
            )
            if entity_versions:
                previous = entity_versions[-1]
                if previous.valid_from == effective:
                    entity_versions[-1] = candidate
                    continue
                entity_versions[-1] = replace(previous, valid_to=effective, is_current=False)
            entity_versions.append(candidate)
        versions.extend(entity_versions)
    return versions


def point_in_time(
    history: Iterable[HistoryVersion], *, tenant_id: str, entity_id: str, as_of_timestamp: str
) -> HistoryVersion | None:
    as_of = _instant(as_of_timestamp)
    matches = [
        row
        for row in history
        if row.tenant_id == tenant_id
        and row.entity_id == entity_id
        and _instant(row.valid_from) <= as_of
        and (row.valid_to is None or as_of < _instant(row.valid_to))
    ]
    if not matches:
        return None
    result = max(matches, key=lambda row: (_instant(row.valid_from), row.source_event_id))
    return None if result.is_deleted else result


def validate_scd2_invariants(history: Iterable[HistoryVersion]) -> list[str]:
    errors: list[str] = []
    grouped: dict[tuple[str, str], list[HistoryVersion]] = {}
    for row in history:
        grouped.setdefault((row.tenant_id, row.entity_id), []).append(row)
        if row.valid_to and _instant(row.valid_from) >= _instant(row.valid_to):
            errors.append(f"invalid_window:{row.tenant_id}:{row.entity_id}:{row.source_event_id}")
    for key, rows in grouped.items():
        ordered = sorted(rows, key=lambda row: (_instant(row.valid_from), row.source_event_id))
        current = [row for row in ordered if row.is_current]
        if len(current) > 1:
            errors.append(f"multiple_current:{key[0]}:{key[1]}")
        for previous, following in zip(ordered, ordered[1:]):
            if previous.valid_to is None or _instant(previous.valid_to) > _instant(following.valid_from):
                errors.append(f"overlap:{key[0]}:{key[1]}:{following.source_event_id}")
    return errors
