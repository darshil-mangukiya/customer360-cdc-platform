from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ingestion.contracts import validate_payload_contract


VALID_OPERATIONS = {"insert", "update", "delete"}
SUPPORTED_SCHEMA_VERSIONS = {1}
REQUIRED_FIELDS = {
    "event_id",
    "source_system",
    "source_table",
    "operation_type",
    "event_timestamp",
    "record_primary_key",
    "payload_before",
    "payload_after",
    "batch_id",
    "schema_version",
}

TOPIC_BY_TABLE = {
    "customers": "cdc.customers",
    "source_customers_cdc_demo": "cdc.source_customers_cdc_demo",
    "subscriptions": "cdc.subscriptions",
    "orders": "cdc.orders",
    "engagement_events": "cdc.engagement_events",
    "support_interactions": "cdc.support_interactions",
    "marketing_engagement": "cdc.marketing_engagement",
}


class CDCValidationError(ValueError):
    """A contract failure with a stable quarantine category."""

    def __init__(self, category: str, detail: str, *, retry_eligible: bool = False) -> None:
        super().__init__(detail)
        self.category = category
        self.retry_eligible = retry_eligible


@dataclass(frozen=True)
class NormalizedEnvelope:
    event_id: str
    tenant_id: str
    source_system: str
    source_table: str
    operation_type: str
    event_timestamp: str
    record_primary_key: str
    payload_before: dict[str, Any] | None
    payload_after: dict[str, Any] | None
    batch_id: str
    schema_version: int
    topic_name: str
    envelope_hash: str
    ingested_at: str
    event_sequence_number: int = 0
    source_transaction_id: str | None = None
    source_lsn: str | None = None
    source_commit_timestamp: str | None = None
    ingestion_timestamp: str | None = None
    kafka_topic: str | None = None
    kafka_partition: int | None = None
    kafka_offset: int | None = None
    event_hash: str | None = None
    replay_batch_id: str | None = None
    is_replay: bool = False

    def as_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


def _parse_timestamp(value: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _hash_payload(raw: dict[str, Any]) -> str:
    payload = json.dumps(raw, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _payload_tenant(raw: dict[str, Any]) -> str:
    payload = raw.get("payload_after") or raw.get("payload_before") or {}
    tenant = raw.get("tenant_id") or payload.get("tenant_id")
    return str(tenant or "tenant_unknown")


def normalize_event(raw: dict[str, Any]) -> NormalizedEnvelope:
    missing = REQUIRED_FIELDS.difference(raw)
    if missing:
        raise CDCValidationError("SCHEMA_INVALID", f"missing required CDC envelope fields: {sorted(missing)}")

    operation = str(raw["operation_type"]).lower()
    if operation not in VALID_OPERATIONS:
        raise CDCValidationError("UNKNOWN_OPERATION", f"unsupported operation_type={raw['operation_type']}")

    try:
        schema_version = int(raw["schema_version"])
    except (TypeError, ValueError) as exc:
        raise CDCValidationError("SCHEMA_INVALID", "schema_version must be an integer") from exc
    if schema_version not in SUPPORTED_SCHEMA_VERSIONS:
        raise CDCValidationError(
            "UNSUPPORTED_SCHEMA_VERSION",
            f"unsupported schema_version={schema_version}; supported={sorted(SUPPORTED_SCHEMA_VERSIONS)}",
        )

    if raw.get("record_primary_key") in {None, "", "None"}:
        raise CDCValidationError("MISSING_SOURCE_KEY", "record_primary_key is required")

    tenant_id = _payload_tenant(raw)
    if tenant_id == "tenant_unknown" and not (
        raw.get("tenant_id") or (raw.get("payload_after") or raw.get("payload_before") or {}).get("tenant_id")
    ):
        raise CDCValidationError("INVALID_TENANT", "tenant_id is required in the envelope or payload")

    before = raw.get("payload_before")
    after = raw.get("payload_after")
    if operation == "delete" and before is None:
        raise CDCValidationError("INVALID_PAYLOAD", "delete events require payload_before")
    if operation in {"insert", "update"} and after is None:
        raise CDCValidationError("INVALID_PAYLOAD", f"{operation} events require payload_after")

    source_table = str(raw["source_table"])
    topic = TOPIC_BY_TABLE.get(source_table)
    if topic is None:
        raise CDCValidationError("SCHEMA_INVALID", f"no Kafka topic configured for source_table={source_table}")
    try:
        validate_payload_contract(source_table, after if operation != "delete" else before)
    except ValueError as exc:
        raise CDCValidationError("SCHEMA_INVALID", str(exc)) from exc

    try:
        event_timestamp = _parse_timestamp(str(raw["event_timestamp"]))
    except (TypeError, ValueError) as exc:
        raise CDCValidationError("SCHEMA_INVALID", "event_timestamp must be an ISO-8601 timestamp") from exc

    normalized = {
        "event_id": str(raw["event_id"]),
        "tenant_id": tenant_id,
        "source_system": str(raw["source_system"]),
        "source_table": source_table,
        "operation_type": operation,
        "event_timestamp": event_timestamp,
        "record_primary_key": str(raw["record_primary_key"]),
        "payload_before": before,
        "payload_after": after,
        "batch_id": str(raw["batch_id"]),
        "schema_version": schema_version,
    }
    return NormalizedEnvelope(
        **normalized,
        topic_name=topic,
        envelope_hash=_hash_payload(normalized),
        ingested_at=datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z"),
        event_sequence_number=int(raw.get("event_sequence_number") or 0),
        source_transaction_id=raw.get("source_transaction_id"),
        source_lsn=raw.get("source_lsn"),
        source_commit_timestamp=_parse_timestamp(str(raw["source_commit_timestamp"]))
        if raw.get("source_commit_timestamp")
        else normalized["event_timestamp"],
        ingestion_timestamp=_parse_timestamp(str(raw["ingestion_timestamp"]))
        if raw.get("ingestion_timestamp")
        else None,
        kafka_topic=raw.get("kafka_topic") or topic,
        kafka_partition=int(raw["kafka_partition"]) if raw.get("kafka_partition") is not None else None,
        kafka_offset=int(raw["kafka_offset"]) if raw.get("kafka_offset") is not None else None,
        event_hash=raw.get("event_hash") or _hash_payload(normalized),
        replay_batch_id=raw.get("replay_batch_id"),
        is_replay=bool(raw.get("is_replay")),
    )


def load_jsonl(path: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with open(path, encoding="utf-8") as fh:
        for line_number, line in enumerate(fh, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                records.append(json.loads(stripped))
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSON on line {line_number}: {exc}") from exc
    return records
