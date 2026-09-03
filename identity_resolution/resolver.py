from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from identity_resolution import config
from ingestion.cdc_envelope import NormalizedEnvelope, load_jsonl, normalize_event
from ingestion.loader import pg_connection


SOURCE_PRIORITY = {
    "account_app": 100,
    "billing_platform": 90,
    "commerce_platform": 70,
    "support_desk": 60,
    "marketing_automation": 50,
    "product_analytics": 40,
}

IDENTIFIER_WEIGHTS = {
    "external_account_id": 100,
    "subscription_id": 90,
    "customer_id": 85,
    "email": 75,
    "phone": 65,
    "device_id": 45,
    "order_customer_ref": 40,
    "support_customer_ref": 40,
}


@dataclass(frozen=True)
class SourceRecord:
    tenant_id: str | None
    source_system: str
    source_table: str
    source_record_id: str
    payload: dict[str, Any]
    event_timestamp: str
    operation_type: str


@dataclass(frozen=True)
class IdentityMapRow:
    canonical_customer_id: str
    tenant_id: str | None
    source_system: str
    source_table: str
    source_record_id: str
    match_rule: str
    match_confidence: float
    canonical_customer_version: int
    identifier_fingerprint: str
    first_seen_at: str
    last_seen_at: str
    is_active: bool


@dataclass(frozen=True)
class CanonicalCustomer:
    canonical_customer_id: str
    tenant_id: str | None
    business_unit: str | None
    primary_email: str | None
    primary_phone: str | None
    external_account_id: str | None
    first_name: str | None
    last_name: str | None
    customer_status: str | None
    first_seen_at: str
    last_seen_at: str
    source_record_count: int
    survivorship_rule: str
    canonical_customer_version: int = 1


@dataclass(frozen=True)
class IdentityAuditRow:
    canonical_customer_id: str
    source_record_id: str
    source_system: str
    matched_identifiers: str
    decision_reason: str
    confidence_score: float
    resolved_at: str


@dataclass(frozen=True)
class IdentityGraphNode:
    node_id: str
    tenant_id: str | None
    node_type: str
    identifier_type: str | None
    identifier_value_hash: str | None
    source_system: str | None
    source_record_id: str | None
    first_seen_at: str
    last_seen_at: str


@dataclass(frozen=True)
class IdentityGraphEdge:
    edge_id: str
    tenant_id: str | None
    left_node_id: str
    right_node_id: str
    match_rule: str
    match_confidence: float
    evidence_count: int
    first_seen_at: str
    last_seen_at: str


@dataclass(frozen=True)
class IdentityMatchRuleRow:
    match_rule: str
    rule_strength: str
    match_confidence: float
    identifier_types: str
    description: str


@dataclass(frozen=True)
class IdentityResolutionRun:
    identity_resolution_run_id: str
    resolved_at: str
    canonical_customer_count: int
    source_record_count: int
    graph_node_count: int
    graph_edge_count: int
    merge_event_count: int
    run_status: str


@dataclass(frozen=True)
class IdentityMergeEvent:
    merge_event_id: str
    tenant_id: str | None
    canonical_customer_id: str
    canonical_customer_version: int
    merged_source_records: str
    strongest_match_rule: str
    match_confidence: float
    occurred_at: str
    merge_reason: str


@dataclass(frozen=True)
class IdentityLinkExplanation:
    explanation_id: str
    tenant_id: str | None
    canonical_customer_id: str
    source_system: str
    source_record_id: str
    linked_identifier_types: str
    match_rule: str
    match_confidence: float
    explanation_text: str
    generated_at: str


@dataclass(frozen=True)
class CustomerIdentityMapHistory:
    history_id: str
    tenant_id: str | None
    canonical_customer_id: str
    source_system: str
    source_record_id: str
    canonical_customer_version: int
    valid_from: str
    valid_to: str | None
    is_current: bool
    change_reason: str


@dataclass(frozen=True)
class IdentityGraphArtifacts:
    nodes: list[IdentityGraphNode]
    edges: list[IdentityGraphEdge]
    match_rules: list[IdentityMatchRuleRow]
    resolution_runs: list[IdentityResolutionRun]
    merge_events: list[IdentityMergeEvent]
    link_explanations: list[IdentityLinkExplanation]
    map_history: list[CustomerIdentityMapHistory]


class UnionFind:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def find(self, item: str) -> str:
        self.parent.setdefault(item, item)
        if self.parent[item] != item:
            self.parent[item] = self.find(self.parent[item])
        return self.parent[item]

    def union(self, left: str, right: str) -> None:
        root_left = self.find(left)
        root_right = self.find(right)
        if root_left != root_right:
            self.parent[root_right] = root_left


def normalize_email(value: Any) -> str | None:
    if not value:
        return None
    email = str(value).strip().lower()
    if "@" not in email:
        return None
    local, domain = email.split("@", 1)
    if "+" in local:
        local = local.split("+", 1)[0]
    return f"{local}@{domain}"


def normalize_phone(value: Any) -> str | None:
    if not value:
        return None
    digits = re.sub(r"\D", "", str(value))
    if len(digits) < 10:
        return None
    if len(digits) == 10:
        digits = f"1{digits}"
    return f"+{digits}"


def _identifier_value(name: str, value: Any) -> str | None:
    if value in {None, ""}:
        return None
    if name == "email":
        normalized = normalize_email(value)
    elif name == "phone":
        normalized = normalize_phone(value)
    else:
        normalized = str(value).strip().lower()
    if not normalized:
        return None
    return f"{name}:{normalized}"


def extract_identifiers(payload: dict[str, Any]) -> dict[str, str]:
    identifiers: dict[str, str] = {}
    for field in IDENTIFIER_WEIGHTS:
        token = _identifier_value(field, payload.get(field))
        if token:
            identifiers[field] = token
    for ref_field in ["order_customer_ref", "support_customer_ref"]:
        ref_value = payload.get(ref_field)
        if not ref_value:
            continue
        normalized_ref = str(ref_value).strip().lower()
        if normalized_ref.startswith("cust_"):
            identifiers[f"{ref_field}_as_customer_id"] = f"customer_id:{normalized_ref}"
        elif normalized_ref.startswith("acct_ext_"):
            identifiers[f"{ref_field}_as_external_account_id"] = f"external_account_id:{normalized_ref}"
    return identifiers


def _tenant_scope(tenant_id: str | None) -> str:
    return tenant_id or "tenant_unknown"


def _source_record_node(record: SourceRecord) -> str:
    return f"record:{_tenant_scope(record.tenant_id)}:{record.source_system}:{record.source_record_id}"


def _canonical_id(tokens: list[str], *, tenant_id: str | None = None) -> str:
    strongest = sorted(tokens, key=lambda t: (-IDENTIFIER_WEIGHTS.get(t.split(":", 1)[0], 1), t))[0]
    scoped_seed = f"{_tenant_scope(tenant_id)}:{strongest}"
    digest = hashlib.sha1(scoped_seed.encode("utf-8")).hexdigest()[:12]
    return f"cc_{digest}"


def _latest_records(events: list[NormalizedEnvelope]) -> dict[tuple[str, str, str], SourceRecord]:
    state: dict[tuple[str, str, str], SourceRecord] = {}
    ordered = sorted(events, key=lambda e: (e.event_timestamp, e.event_id))
    for event in ordered:
        key = (_tenant_scope(event.tenant_id), event.source_system, event.record_primary_key)
        payload = event.payload_after if event.operation_type != "delete" else event.payload_before
        if not payload:
            continue
        state[key] = SourceRecord(
            tenant_id=event.tenant_id,
            source_system=event.source_system,
            source_table=event.source_table,
            source_record_id=event.record_primary_key,
            payload=payload,
            event_timestamp=event.event_timestamp,
            operation_type=event.operation_type,
        )
    return state


def _component_records(
    records: dict[tuple[str, str, str], SourceRecord],
) -> tuple[dict[str, list[SourceRecord]], dict[str, list[str]], list[tuple[str, SourceRecord, SourceRecord]]]:
    """Group source records into canonical-customer components.

    Union-Find only unions two records on an identifier whose match-rule confidence
    is at or above `config.AUTO_MERGE_MIN` (the identity stewardship gate).
    Weaker-but-not-negligible identifiers (confidence in
    `[config.REVIEW_MIN, config.AUTO_MERGE_MIN)`, e.g. a shared device_id) are never
    silently merged; instead every pair of records they would have linked, but that
    ended up in different final components, is returned as a weak-link candidate for
    `identity_resolution.stewardship` to route into the review queue.
    """
    uf = UnionFind()
    node_to_record: dict[str, SourceRecord] = {}
    component_tokens: dict[str, list[str]] = defaultdict(list)
    weak_token_records: dict[str, list[tuple[str, SourceRecord]]] = defaultdict(list)

    for record in records.values():
        record_node = _source_record_node(record)
        node_to_record[record_node] = record
        identifiers = extract_identifiers(record.payload)
        for token in identifiers.values():
            identifier_type = token.split(":", 1)[0]
            _, confidence, _strength = _rule_for_identifier(identifier_type)
            identifier_node = f"id:{_tenant_scope(record.tenant_id)}:{token}"
            if confidence >= config.AUTO_MERGE_MIN:
                uf.union(record_node, identifier_node)
            elif confidence >= config.REVIEW_MIN:
                scoped_token = f"{_tenant_scope(record.tenant_id)}:{token}"
                weak_token_records[scoped_token].append((record_node, record))

    for record_node, record in node_to_record.items():
        root = uf.find(record_node)
        component_tokens[root].extend(extract_identifiers(record.payload).values())

    grouped: dict[str, list[SourceRecord]] = defaultdict(list)
    canonical_tokens: dict[str, list[str]] = {}
    for record_node, record in node_to_record.items():
        root = uf.find(record_node)
        tokens = sorted(set(component_tokens[root]))
        canonical_id = _canonical_id(tokens, tenant_id=record.tenant_id) if tokens else _canonical_id([record_node], tenant_id=record.tenant_id)
        grouped[canonical_id].append(record)
        canonical_tokens[canonical_id] = tokens

    weak_links: list[tuple[str, SourceRecord, SourceRecord]] = []
    for scoped_token, entries in weak_token_records.items():
        token = scoped_token.split(":", 1)[1]
        # Dedupe by the final component root each record_node landed in, so we emit at
        # most one candidate pair per pair of distinct components sharing this token.
        by_root: dict[str, SourceRecord] = {}
        for record_node, record in entries:
            by_root.setdefault(uf.find(record_node), record)
        distinct_records = list(by_root.values())
        if len(distinct_records) < 2:
            continue
        anchor = distinct_records[0]
        for other in distinct_records[1:]:
            weak_links.append((token, anchor, other))

    return grouped, canonical_tokens, weak_links


def _best_value(records: list[SourceRecord], fields: list[str]) -> Any:
    candidates: list[tuple[int, str, Any]] = []
    for record in records:
        if record.operation_type == "delete":
            continue
        for field in fields:
            value = record.payload.get(field)
            if value not in {None, ""}:
                candidates.append((SOURCE_PRIORITY.get(record.source_system, 0), record.event_timestamp, value))
    if not candidates:
        return None
    candidates.sort(key=lambda row: (row[0], row[1]), reverse=True)
    return candidates[0][2]


def _match_rule(tokens: list[str]) -> tuple[str, float]:
    fields = {token.split(":", 1)[0] for token in tokens}
    if {"external_account_id", "subscription_id"} & fields:
        return "deterministic_id_graph_external_or_subscription", 0.99
    if {"customer_id", "email"} <= fields:
        return "deterministic_customer_id_email", 0.95
    if {"email", "phone"} <= fields:
        return "deterministic_email_phone", 0.92
    if "email" in fields:
        return "deterministic_email", 0.86
    if "phone" in fields:
        return "deterministic_phone", 0.78
    return "low_confidence_device_or_source_ref", 0.62


def _identifier_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _node_id(kind: str, value: str) -> str:
    return f"{kind}_{hashlib.sha1(value.encode('utf-8')).hexdigest()[:16]}"


def _rule_for_identifier(identifier_type: str) -> tuple[str, float, str]:
    if identifier_type == "email":
        return "exact_normalized_email", 0.86, "strong"
    if identifier_type == "phone":
        return "exact_normalized_phone", 0.78, "medium"
    if identifier_type == "subscription_id":
        return "exact_subscription_id", 0.99, "strong"
    if identifier_type == "external_account_id":
        return "exact_external_account_id", 0.99, "strong"
    if identifier_type == "customer_id":
        return "exact_source_customer_id", 0.95, "strong"
    if identifier_type == "device_id":
        return "device_id_behavioral_supporting_match", 0.62, "weak"
    return "source_reference_match", 0.60, "weak"


def identity_match_rules() -> list[IdentityMatchRuleRow]:
    return [
        IdentityMatchRuleRow(
            "exact_normalized_email",
            "strong",
            0.86,
            "email",
            "Links records with the same lower-cased plus-address-normalized email inside a tenant.",
        ),
        IdentityMatchRuleRow(
            "exact_normalized_phone",
            "medium",
            0.78,
            "phone",
            "Links records with the same E.164-style normalized phone number inside a tenant.",
        ),
        IdentityMatchRuleRow(
            "exact_subscription_id",
            "strong",
            0.99,
            "subscription_id",
            "Links source records that reference the same billing subscription ID.",
        ),
        IdentityMatchRuleRow(
            "exact_external_account_id",
            "strong",
            0.99,
            "external_account_id",
            "Links account, billing, order, support, and marketing records sharing the same external account ID.",
        ),
        IdentityMatchRuleRow(
            "strong_composite_customer_email",
            "strong",
            0.95,
            "customer_id,email",
            "Uses source customer ID plus normalized email as a composite deterministic match.",
        ),
        IdentityMatchRuleRow(
            "device_id_behavioral_supporting_match",
            "weak",
            0.62,
            "device_id",
            "Uses device ID only as supporting evidence because shared devices can create false positives.",
        ),
    ]


def build_identity_graph_artifacts(
    events: list[NormalizedEnvelope],
    canonical: list[CanonicalCustomer],
    mappings: list[IdentityMapRow],
) -> IdentityGraphArtifacts:
    latest = _latest_records(events)
    canonical_by_source = {
        (_tenant_scope(row.tenant_id), row.source_system, row.source_record_id): row
        for row in mappings
    }
    canonical_by_id = {row.canonical_customer_id: row for row in canonical}
    now = datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")
    nodes_by_id: dict[str, IdentityGraphNode] = {}
    edges_by_id: dict[str, IdentityGraphEdge] = {}
    explanations: list[IdentityLinkExplanation] = []
    history: list[CustomerIdentityMapHistory] = []

    for record in latest.values():
        map_row = canonical_by_source.get((_tenant_scope(record.tenant_id), record.source_system, record.source_record_id))
        if not map_row:
            continue
        canonical_customer = canonical_by_id.get(map_row.canonical_customer_id)
        tenant_id = map_row.tenant_id or record.tenant_id
        record_node_id = _node_id("record", f"{tenant_id}:{record.source_system}:{record.source_record_id}")
        nodes_by_id[record_node_id] = IdentityGraphNode(
            node_id=record_node_id,
            tenant_id=tenant_id,
            node_type="source_record",
            identifier_type=None,
            identifier_value_hash=None,
            source_system=record.source_system,
            source_record_id=record.source_record_id,
            first_seen_at=record.event_timestamp,
            last_seen_at=record.event_timestamp,
        )

        identifiers = extract_identifiers(record.payload)
        identifier_types: list[str] = []
        for identifier_name, token in sorted(identifiers.items()):
            identifier_type = token.split(":", 1)[0]
            identifier_types.append(identifier_type)
            identifier_node_id = _node_id("identifier", f"{tenant_id}:{token}")
            nodes_by_id[identifier_node_id] = IdentityGraphNode(
                node_id=identifier_node_id,
                tenant_id=tenant_id,
                node_type="identifier",
                identifier_type=identifier_type,
                identifier_value_hash=_identifier_hash(token),
                source_system=None,
                source_record_id=None,
                first_seen_at=record.event_timestamp,
                last_seen_at=record.event_timestamp,
            )
            rule, confidence, _ = _rule_for_identifier(identifier_type)
            edge_id = _node_id("edge", f"{record_node_id}:{identifier_node_id}:{rule}")
            edges_by_id[edge_id] = IdentityGraphEdge(
                edge_id=edge_id,
                tenant_id=tenant_id,
                left_node_id=record_node_id,
                right_node_id=identifier_node_id,
                match_rule=rule,
                match_confidence=confidence,
                evidence_count=1,
                first_seen_at=record.event_timestamp,
                last_seen_at=record.event_timestamp,
            )

        explanation_id = _node_id(
            "explanation",
            f"{tenant_id}:{map_row.canonical_customer_id}:{record.source_system}:{record.source_record_id}",
        )
        explanation_text = (
            f"{record.source_system}:{record.source_record_id} linked to "
            f"{map_row.canonical_customer_id} using {map_row.match_rule}; identifiers="
            f"{','.join(sorted(set(identifier_types)))}; confidence={map_row.match_confidence:.2f}."
        )
        explanations.append(
            IdentityLinkExplanation(
                explanation_id=explanation_id,
                tenant_id=tenant_id,
                canonical_customer_id=map_row.canonical_customer_id,
                source_system=record.source_system,
                source_record_id=record.source_record_id,
                linked_identifier_types=",".join(sorted(set(identifier_types))),
                match_rule=map_row.match_rule,
                match_confidence=map_row.match_confidence,
                explanation_text=explanation_text,
                generated_at=now,
            )
        )
        history.append(
            CustomerIdentityMapHistory(
                history_id=_node_id(
                    "maphist",
                    f"{tenant_id}:{map_row.canonical_customer_id}:{record.source_system}:{record.source_record_id}:v{map_row.canonical_customer_version}",
                ),
                tenant_id=tenant_id,
                canonical_customer_id=map_row.canonical_customer_id,
                source_system=record.source_system,
                source_record_id=record.source_record_id,
                canonical_customer_version=map_row.canonical_customer_version,
                valid_from=record.event_timestamp,
                valid_to=None,
                is_current=record.operation_type != "delete",
                change_reason="initial_deterministic_link",
            )
        )

    merge_events: list[IdentityMergeEvent] = []
    for customer in canonical:
        linked_records = [
            row
            for row in mappings
            if row.canonical_customer_id == customer.canonical_customer_id and row.is_active
        ]
        if len(linked_records) < 2:
            continue
        strongest = sorted(linked_records, key=lambda row: (row.match_confidence, row.match_rule), reverse=True)[0]
        merge_events.append(
            IdentityMergeEvent(
                merge_event_id=_node_id("merge", f"{customer.tenant_id}:{customer.canonical_customer_id}:v{customer.canonical_customer_version}"),
                tenant_id=customer.tenant_id,
                canonical_customer_id=customer.canonical_customer_id,
                canonical_customer_version=customer.canonical_customer_version,
                merged_source_records="|".join(
                    sorted(f"{row.source_system}:{row.source_record_id}" for row in linked_records)
                ),
                strongest_match_rule=strongest.match_rule,
                match_confidence=strongest.match_confidence,
                occurred_at=customer.last_seen_at,
                merge_reason="multiple source records linked by deterministic identity graph evidence",
            )
        )

    resolution_run = IdentityResolutionRun(
        identity_resolution_run_id=f"idrun_{uuid.uuid4().hex[:12]}",
        resolved_at=now,
        canonical_customer_count=len(canonical),
        source_record_count=len(mappings),
        graph_node_count=len(nodes_by_id),
        graph_edge_count=len(edges_by_id),
        merge_event_count=len(merge_events),
        run_status="success",
    )
    return IdentityGraphArtifacts(
        nodes=sorted(nodes_by_id.values(), key=lambda row: row.node_id),
        edges=sorted(edges_by_id.values(), key=lambda row: row.edge_id),
        match_rules=identity_match_rules(),
        resolution_runs=[resolution_run],
        merge_events=sorted(merge_events, key=lambda row: row.merge_event_id),
        link_explanations=sorted(explanations, key=lambda row: row.explanation_id),
        map_history=sorted(history, key=lambda row: row.history_id),
    )


@dataclass(frozen=True)
class WeakLinkCandidate:
    """A pair of records sharing only a review-band-confidence identifier.

    They were deliberately *not* auto-merged (see `_component_records`); this is the
    evidence `identity_resolution.stewardship` uses to open a review case instead.
    """

    token: str
    identifier_type: str
    match_rule: str
    match_confidence: float
    tenant_id: str | None
    left_canonical_customer_id: str
    left_source_system: str
    left_source_record_id: str
    right_canonical_customer_id: str
    right_source_system: str
    right_source_record_id: str


def _resolve_identity_core(
    events: list[NormalizedEnvelope],
) -> tuple[list[CanonicalCustomer], list[IdentityMapRow], list[IdentityAuditRow], list[WeakLinkCandidate]]:
    latest = _latest_records(events)
    grouped, tokens_by_customer, weak_links_raw = _component_records(latest)
    resolved_at = datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")

    canonical_rows: list[CanonicalCustomer] = []
    map_rows: list[IdentityMapRow] = []
    audit_rows: list[IdentityAuditRow] = []

    for canonical_id, records in sorted(grouped.items()):
        timestamps = [record.event_timestamp for record in records]
        tokens = tokens_by_customer.get(canonical_id, [])
        rule, confidence = _match_rule(tokens)
        fingerprint = hashlib.sha256("|".join(tokens).encode("utf-8")).hexdigest()
        active_records = [r for r in records if r.operation_type != "delete"]
        canonical_version = 1
        canonical_rows.append(
            CanonicalCustomer(
                canonical_customer_id=canonical_id,
                tenant_id=_best_value(active_records, ["tenant_id"]),
                business_unit=_best_value(active_records, ["business_unit"]),
                primary_email=normalize_email(_best_value(active_records, ["email"])),
                primary_phone=normalize_phone(_best_value(active_records, ["phone"])),
                external_account_id=_best_value(active_records, ["external_account_id"]),
                first_name=_best_value(active_records, ["first_name"]),
                last_name=_best_value(active_records, ["last_name"]),
                customer_status=_best_value(active_records, ["customer_status", "subscription_status"]),
                first_seen_at=min(timestamps),
                last_seen_at=max(timestamps),
                source_record_count=len(records),
                survivorship_rule="source_priority_then_latest_non_null",
                canonical_customer_version=canonical_version,
            )
        )
        for record in records:
            identifiers = extract_identifiers(record.payload)
            map_rows.append(
                IdentityMapRow(
                    canonical_customer_id=canonical_id,
                    tenant_id=_best_value([record], ["tenant_id"]) or record.tenant_id,
                    source_system=record.source_system,
                    source_table=record.source_table,
                    source_record_id=record.source_record_id,
                    match_rule=rule,
                    match_confidence=confidence,
                    canonical_customer_version=canonical_version,
                    identifier_fingerprint=fingerprint,
                    first_seen_at=record.event_timestamp,
                    last_seen_at=record.event_timestamp,
                    is_active=record.operation_type != "delete",
                )
            )
            audit_rows.append(
                IdentityAuditRow(
                    canonical_customer_id=canonical_id,
                    source_record_id=record.source_record_id,
                    source_system=record.source_system,
                    matched_identifiers="|".join(sorted(identifiers.values())),
                    decision_reason=rule,
                    confidence_score=confidence,
                    resolved_at=resolved_at,
                )
            )

    record_to_canonical = {
        _source_record_node(record): canonical_id for canonical_id, records in grouped.items() for record in records
    }
    weak_links: list[WeakLinkCandidate] = []
    for token, left, right in weak_links_raw:
        identifier_type = token.split(":", 1)[0]
        rule, confidence, _strength = _rule_for_identifier(identifier_type)
        left_canonical = record_to_canonical.get(_source_record_node(left))
        right_canonical = record_to_canonical.get(_source_record_node(right))
        if not left_canonical or not right_canonical or left_canonical == right_canonical:
            continue
        weak_links.append(
            WeakLinkCandidate(
                token=token,
                identifier_type=identifier_type,
                match_rule=rule,
                match_confidence=confidence,
                tenant_id=left.tenant_id or right.tenant_id,
                left_canonical_customer_id=left_canonical,
                left_source_system=left.source_system,
                left_source_record_id=left.source_record_id,
                right_canonical_customer_id=right_canonical,
                right_source_system=right.source_system,
                right_source_record_id=right.source_record_id,
            )
        )

    return canonical_rows, map_rows, audit_rows, weak_links


def resolve_identity(events: list[NormalizedEnvelope]) -> tuple[list[CanonicalCustomer], list[IdentityMapRow], list[IdentityAuditRow]]:
    canonical_rows, map_rows, audit_rows, _weak_links = _resolve_identity_core(events)
    return canonical_rows, map_rows, audit_rows


def resolve_identity_with_review_candidates(
    events: list[NormalizedEnvelope],
) -> tuple[list[CanonicalCustomer], list[IdentityMapRow], list[IdentityAuditRow], list[WeakLinkCandidate]]:
    """Same as `resolve_identity`, plus the weak-link candidates the auto-merge gate
    held back (used by `identity_resolution.stewardship.detect_review_candidates`)."""
    return _resolve_identity_core(events)


def write_identity_outputs(
    *,
    canonical: list[CanonicalCustomer],
    mappings: list[IdentityMapRow],
    audit: list[IdentityAuditRow],
    output_dir: Path,
    graph_artifacts: IdentityGraphArtifacts | None = None,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "dim_customer_canonical.csv": [asdict(row) for row in canonical],
        "customer_identity_map.csv": [asdict(row) for row in mappings],
        "identity_resolution_audit.csv": [asdict(row) for row in audit],
    }
    if graph_artifacts:
        outputs.update(
            {
                "identity_graph_node.csv": [asdict(row) for row in graph_artifacts.nodes],
                "identity_graph_edge.csv": [asdict(row) for row in graph_artifacts.edges],
                "identity_match_rule.csv": [asdict(row) for row in graph_artifacts.match_rules],
                "identity_resolution_run.csv": [asdict(row) for row in graph_artifacts.resolution_runs],
                "identity_merge_event.csv": [asdict(row) for row in graph_artifacts.merge_events],
                "identity_link_explanation.csv": [asdict(row) for row in graph_artifacts.link_explanations],
                "customer_identity_map_history.csv": [asdict(row) for row in graph_artifacts.map_history],
            }
        )
    for filename, rows in outputs.items():
        with (output_dir / filename).open("w", newline="", encoding="utf-8") as fh:
            if not rows:
                continue
            writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)


def load_identity_to_postgres(
    *,
    dsn: str,
    canonical: list[CanonicalCustomer],
    mappings: list[IdentityMapRow],
    audit: list[IdentityAuditRow],
) -> None:
    with pg_connection(dsn) as conn:
        with conn.cursor() as cur:
            for row in canonical:
                cur.execute(
                    """
                    insert into identity.dim_customer_canonical (
                        canonical_customer_id, tenant_id, business_unit, primary_email,
                        primary_phone, external_account_id, first_name, last_name,
                        customer_status, first_seen_at, last_seen_at, source_record_count,
                        survivorship_rule, canonical_customer_version, updated_at
                    )
                    values (
                        %(canonical_customer_id)s, %(tenant_id)s, %(business_unit)s,
                        %(primary_email)s, %(primary_phone)s, %(external_account_id)s,
                        %(first_name)s, %(last_name)s, %(customer_status)s,
                        %(first_seen_at)s, %(last_seen_at)s, %(source_record_count)s,
                        %(survivorship_rule)s, %(canonical_customer_version)s, now()
                    )
                    on conflict (canonical_customer_id) do update set
                        tenant_id = excluded.tenant_id,
                        business_unit = excluded.business_unit,
                        primary_email = excluded.primary_email,
                        primary_phone = excluded.primary_phone,
                        external_account_id = excluded.external_account_id,
                        first_name = excluded.first_name,
                        last_name = excluded.last_name,
                        customer_status = excluded.customer_status,
                        first_seen_at = excluded.first_seen_at,
                        last_seen_at = excluded.last_seen_at,
                        source_record_count = excluded.source_record_count,
                        survivorship_rule = excluded.survivorship_rule,
                        canonical_customer_version = excluded.canonical_customer_version,
                        updated_at = now()
                    """,
                    asdict(row),
                )
            for row in mappings:
                cur.execute(
                    """
                    insert into identity.customer_identity_map (
                        canonical_customer_id, tenant_id, source_system, source_table, source_record_id,
                        match_rule, match_confidence, canonical_customer_version,
                        identifier_fingerprint, first_seen_at,
                        last_seen_at, is_active
                    )
                    values (
                        %(canonical_customer_id)s, %(tenant_id)s, %(source_system)s, %(source_table)s,
                        %(source_record_id)s, %(match_rule)s, %(match_confidence)s,
                        %(canonical_customer_version)s, %(identifier_fingerprint)s,
                        %(first_seen_at)s, %(last_seen_at)s,
                        %(is_active)s
                    )
                    on conflict (tenant_id, source_system, source_record_id) do update set
                        canonical_customer_id = excluded.canonical_customer_id,
                        tenant_id = excluded.tenant_id,
                        source_table = excluded.source_table,
                        match_rule = excluded.match_rule,
                        match_confidence = excluded.match_confidence,
                        canonical_customer_version = excluded.canonical_customer_version,
                        identifier_fingerprint = excluded.identifier_fingerprint,
                        first_seen_at = excluded.first_seen_at,
                        last_seen_at = excluded.last_seen_at,
                        is_active = excluded.is_active
                    """,
                    asdict(row),
                )
            for row in audit:
                cur.execute(
                    """
                    insert into identity.identity_resolution_audit (
                        canonical_customer_id, source_record_id, source_system,
                        matched_identifiers, decision_reason, confidence_score, resolved_at
                    )
                    values (
                        %(canonical_customer_id)s, %(source_record_id)s, %(source_system)s,
                        %(matched_identifiers)s, %(decision_reason)s, %(confidence_score)s,
                        %(resolved_at)s
                    )
                    """,
                    asdict(row),
                )
        conn.commit()


def load_events(path: Path) -> list[NormalizedEnvelope]:
    events: list[NormalizedEnvelope] = []
    for raw in load_jsonl(str(path)):
        try:
            if "topic_name" in raw and "tenant_id" in raw:
                events.append(NormalizedEnvelope(**raw))
            else:
                events.append(normalize_event(raw))
        except ValueError:
            continue
    return events


def main() -> None:
    parser = argparse.ArgumentParser(description="Resolve source identities into canonical customers.")
    parser.add_argument("--input", default="ingestion/output/raw_cdc_events.jsonl")
    parser.add_argument("--output-dir", default="identity_resolution/output")
    parser.add_argument("--dsn", help="Optional Postgres DSN for loading identity tables.")
    args = parser.parse_args()
    events = load_events(Path(args.input))
    canonical, mappings, audit = resolve_identity(events)
    graph_artifacts = build_identity_graph_artifacts(events, canonical, mappings)
    write_identity_outputs(
        canonical=canonical,
        mappings=mappings,
        audit=audit,
        output_dir=Path(args.output_dir),
        graph_artifacts=graph_artifacts,
    )
    if args.dsn:
        load_identity_to_postgres(dsn=args.dsn, canonical=canonical, mappings=mappings, audit=audit)
    print(
        f"canonical_customers={len(canonical)} mappings={len(mappings)} "
        f"audit_rows={len(audit)} loaded_to_postgres={bool(args.dsn)}"
    )


if __name__ == "__main__":
    main()
