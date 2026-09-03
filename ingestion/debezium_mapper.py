from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from ingestion.cdc_envelope import NormalizedEnvelope, normalize_event


DEBEZIUM_OPS = {
    "c": "insert",
    "r": "insert",
    "u": "update",
    "d": "delete",
}

PRIMARY_KEY_BY_TABLE = {
    "customers": "customer_id",
    "source_customers_cdc_demo": "customer_id",
    "subscriptions": "subscription_id",
    "orders": "order_id",
    "engagement_events": "engagement_event_id",
    "support_interactions": "support_interaction_id",
    "marketing_engagement": "marketing_touch_id",
}

SOURCE_SYSTEM_BY_TABLE = {
    "customers": "account_app",
    "source_customers_cdc_demo": "account_app",
    "subscriptions": "billing_platform",
    "orders": "commerce_platform",
    "engagement_events": "product_analytics",
    "support_interactions": "support_desk",
    "marketing_engagement": "marketing_automation",
}

DECIMAL_FIELDS_BY_TABLE = {
    "subscriptions": {"mrr"},
    "orders": {"gross_amount"},
}


def is_debezium_message(value: dict[str, Any]) -> bool:
    payload = value.get("payload", value)
    return isinstance(payload, dict) and "op" in payload and "source" in payload


def _payload(value: dict[str, Any]) -> dict[str, Any]:
    payload = value.get("payload", value)
    if not isinstance(payload, dict):
        raise ValueError("Debezium message payload must be an object")
    return payload


def _timestamp_from_ms(value: Any) -> str:
    if value is None:
        return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")
    return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _record_primary_key(table: str, before: dict[str, Any] | None, after: dict[str, Any] | None) -> str:
    payload = after or before or {}
    pk_field = PRIMARY_KEY_BY_TABLE.get(table)
    if pk_field and payload.get(pk_field):
        return str(payload[pk_field])
    for candidate in ["id", "customer_id", "subscription_id", "order_id"]:
        if payload.get(candidate):
            return str(payload[candidate])
    raise ValueError(f"could not infer primary key for Debezium table={table}")


def _normalize_source_payload(table: str, payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if payload is None:
        return None
    enriched = payload.copy()
    enriched.setdefault("tenant_id", "tenant_unknown")
    enriched.setdefault("business_unit", "unknown")
    # The connector intentionally serializes PostgreSQL DECIMAL values as
    # strings to avoid precision loss in Kafka. Convert only contract-declared
    # numeric fields at the normalized-envelope boundary.
    for field in DECIMAL_FIELDS_BY_TABLE.get(table, set()):
        value = enriched.get(field)
        if isinstance(value, str) and value:
            enriched[field] = float(value)
    return enriched


def _event_id(raw: dict[str, Any], table: str, pk: str) -> str:
    source = raw.get("source", {})
    key = {
        "connector": source.get("connector"),
        "db": source.get("db"),
        "schema": source.get("schema"),
        "table": table,
        "pk": pk,
        "op": raw.get("op"),
        "lsn": source.get("lsn"),
        "txId": source.get("txId"),
        "ts_ms": raw.get("ts_ms") or source.get("ts_ms"),
    }
    digest = hashlib.sha256(json.dumps(key, sort_keys=True).encode("utf-8")).hexdigest()[:24]
    return f"dbz_{digest}"


def debezium_to_normalized_envelope(
    value: dict[str, Any],
    *,
    topic: str | None = None,
    kafka_partition: int | None = None,
    kafka_offset: int | None = None,
) -> NormalizedEnvelope:
    raw = _payload(value)
    op = raw.get("op")
    operation_type = DEBEZIUM_OPS.get(str(op))
    if operation_type is None:
        raise ValueError(f"unsupported Debezium op={op}")

    source = raw.get("source") or {}
    table = source.get("table") or (topic.rsplit(".", 1)[-1] if topic else None)
    if not table:
        raise ValueError("Debezium message missing source.table")

    before = _normalize_source_payload(str(table), raw.get("before"))
    after = _normalize_source_payload(str(table), raw.get("after"))
    pk = _record_primary_key(table, before, after)
    event_timestamp = _timestamp_from_ms(raw.get("ts_ms") or source.get("ts_ms"))
    # A single PostgreSQL connector captures six logical domains. Preserve the
    # logical domain name used by identity survivorship rather than collapsing
    # every record to the connector/database name (customer_ops).
    source_system = SOURCE_SYSTEM_BY_TABLE.get(
        str(table), str(source.get("name") or source.get("db") or "debezium_postgres")
    )
    event = {
        "event_id": _event_id(raw, table, pk),
        "source_system": source_system,
        "source_table": table,
        "operation_type": operation_type,
        "event_timestamp": event_timestamp,
        "record_primary_key": pk,
        "payload_before": before,
        "payload_after": after,
        "batch_id": f"debezium_{datetime.fromisoformat(event_timestamp.replace('Z', '+00:00')).date().isoformat()}",
        "schema_version": 1,
        # PostgreSQL LSN is monotonic within this connector stream and gives the
        # consumer a deterministic source-order signal for rapid same-key writes.
        "event_sequence_number": int(source.get("lsn") or 0),
        "source_transaction_id": str(source.get("txId")) if source.get("txId") is not None else None,
        "source_lsn": str(source.get("lsn")) if source.get("lsn") is not None else None,
        "source_commit_timestamp": _timestamp_from_ms(source.get("ts_ms") or raw.get("ts_ms")),
        "kafka_topic": topic,
        "kafka_partition": kafka_partition,
        "kafka_offset": kafka_offset,
    }
    return normalize_event(event)
