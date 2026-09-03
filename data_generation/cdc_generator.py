from __future__ import annotations

import argparse
import hashlib
import json
import random
import uuid
from copy import deepcopy
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


SOURCE_TABLES = {
    "account_app": "customers",
    "billing_platform": "subscriptions",
    "commerce_platform": "orders",
    "product_analytics": "engagement_events",
    "support_desk": "support_interactions",
    "marketing_automation": "marketing_engagement",
}


@dataclass(frozen=True)
class CDCEvent:
    event_id: str
    source_system: str
    source_table: str
    operation_type: str
    event_timestamp: str
    record_primary_key: str
    payload_before: dict[str, Any] | None
    payload_after: dict[str, Any] | None
    batch_id: str
    schema_version: int
    tenant_id: str | None = None
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


def _iso(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def _event(
    *,
    source_system: str,
    operation: str,
    ts: datetime,
    pk: str,
    before: dict[str, Any] | None,
    after: dict[str, Any] | None,
    batch_id: str,
    schema_version: int = 1,
) -> CDCEvent:
    payload = after or before or {}
    return CDCEvent(
        event_id=f"evt_{uuid.uuid4().hex}",
        source_system=source_system,
        source_table=SOURCE_TABLES[source_system],
        operation_type=operation,
        event_timestamp=_iso(ts),
        record_primary_key=str(pk),
        payload_before=before,
        payload_after=after,
        batch_id=batch_id,
        schema_version=schema_version,
        tenant_id=payload.get("tenant_id"),
    )


def _mutate(base: dict[str, Any], **changes: Any) -> dict[str, Any]:
    nxt = deepcopy(base)
    nxt.update(changes)
    return nxt


def _stable_hash(value: str) -> int:
    return int(hashlib.sha256(value.encode("utf-8")).hexdigest()[:8], 16)


def _event_hash(event: CDCEvent) -> str:
    payload = asdict(event).copy()
    payload["event_hash"] = None
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _with_log_metadata(events: list[CDCEvent], *, ingestion_start: datetime) -> list[CDCEvent]:
    """Add realistic source-log and Kafka metadata without changing ingestion order."""
    sequence_lookup: dict[str, int] = {}
    for seq, event in enumerate(
        sorted(events, key=lambda row: (row.source_system, row.source_table, row.event_timestamp, row.event_id)),
        start=1,
    ):
        sequence_lookup[event.event_id] = seq

    offsets: dict[tuple[str, int], int] = {}
    enriched: list[CDCEvent] = []
    for ingest_idx, event in enumerate(events, start=1):
        seq = sequence_lookup[event.event_id]
        topic = f"cdc.{event.source_table}"
        partition = _stable_hash(f"{event.tenant_id}:{event.record_primary_key}") % 3
        offset_key = (topic, partition)
        offset = offsets.get(offset_key, 0)
        offsets[offset_key] = offset + 1
        ingestion_ts = ingestion_start + timedelta(seconds=ingest_idx * 7)
        enriched_event = replace(
            event,
            tenant_id=event.tenant_id or "tenant_unknown",
            event_sequence_number=seq,
            source_transaction_id=f"txn_{event.source_system}_{event.batch_id}_{seq:06d}",
            source_lsn=f"{event.source_system}:{seq:012d}",
            source_commit_timestamp=event.event_timestamp,
            ingestion_timestamp=_iso(ingestion_ts),
            kafka_topic=topic,
            kafka_partition=partition,
            kafka_offset=offset,
            is_replay=bool(event.replay_batch_id),
        )
        enriched.append(replace(enriched_event, event_hash=_event_hash(enriched_event)))
    return enriched


def seed_customer_profiles() -> list[dict[str, Any]]:
    """Return intentionally messy cross-system customer identities.

    The records include shared identifiers, changed emails, nulls, conflicting
    phones, and system-specific IDs so the identity resolver has meaningful work.
    """
    return [
        {
            "person_key": "p001",
            "first_name": "Alex",
            "last_name": "Rivera",
            "email": "alex.rivera@example.com",
            "alt_email": "alex+trial@example.com",
            "phone": "+14155550101",
            "external_account_id": "acct_ext_001",
            "device_id": "dev_ios_001",
            "business_unit": "self_serve",
            "tenant_id": "tenant_us",
        },
        {
            "person_key": "p002",
            "first_name": "Morgan",
            "last_name": "Lee",
            "email": "morgan.lee@example.com",
            "alt_email": "m.lee@workmail.test",
            "phone": "+14155550102",
            "external_account_id": "acct_ext_002",
            "device_id": "dev_web_002",
            "business_unit": "enterprise",
            "tenant_id": "tenant_us",
        },
        {
            "person_key": "p003",
            "first_name": "Priya",
            "last_name": "Nair",
            "email": "priya.nair@example.com",
            "alt_email": None,
            "phone": "+15550101003",
            "external_account_id": "acct_ext_003",
            "device_id": "dev_android_003",
            "business_unit": "self_serve",
            "tenant_id": "tenant_emea",
        },
        {
            "person_key": "p004",
            "first_name": "Sam",
            "last_name": "Taylor",
            "email": "sam.taylor@example.com",
            "alt_email": "sam.billing@example.com",
            "phone": "+14155550104",
            "external_account_id": "acct_ext_004",
            "device_id": "dev_web_004",
            "business_unit": "self_serve",
            "tenant_id": "tenant_us",
        },
        {
            "person_key": "p005",
            "first_name": "Jamie",
            "last_name": "Chen",
            "email": "jamie.chen@example.com",
            "alt_email": "jamie.old@example.com",
            "phone": "+14155550105",
            "external_account_id": "acct_ext_005",
            "device_id": "dev_ios_005",
            "business_unit": "enterprise",
            "tenant_id": "tenant_us",
        },
        {
            "person_key": "p006",
            "first_name": "Riley",
            "last_name": "Nguyen",
            "email": "riley.nguyen@example.com",
            "alt_email": None,
            "phone": None,
            "external_account_id": "acct_ext_006",
            "device_id": "dev_android_006",
            "business_unit": "self_serve",
            "tenant_id": "tenant_apac",
        },
        {
            "person_key": "p007",
            "first_name": "Casey",
            "last_name": "Wong",
            "email": "casey.wong@example.com",
            "alt_email": "casey@agency.test",
            "phone": "+15550101007",
            "external_account_id": "acct_ext_007",
            "device_id": "dev_web_007",
            "business_unit": "partner",
            "tenant_id": "tenant_apac",
        },
        {
            "person_key": "p008",
            "first_name": "Taylor",
            "last_name": "Brooks",
            "email": "taylor.brooks@example.com",
            "alt_email": None,
            "phone": "+14155550108",
            "external_account_id": "acct_ext_008",
            "device_id": "dev_ios_008",
            "business_unit": "self_serve",
            "tenant_id": "tenant_us",
        },
        {
            "person_key": "p009",
            "first_name": "Avery",
            "last_name": "Patel",
            "email": "avery.patel@example.com",
            "alt_email": "avery.p@company.test",
            "phone": "+14155550109",
            "external_account_id": "acct_ext_009",
            "device_id": "dev_android_009",
            "business_unit": "enterprise",
            "tenant_id": "tenant_us",
        },
        {
            "person_key": "p010",
            "first_name": "Jordan",
            "last_name": "Smith",
            "email": "jordan.smith@example.com",
            "alt_email": "j.smith@example.com",
            "phone": "+14155550110",
            "external_account_id": "acct_ext_010",
            "device_id": "dev_web_010",
            "business_unit": "self_serve",
            "tenant_id": "tenant_us",
        },
        {
            "person_key": "p011",
            "first_name": "Drew",
            "last_name": "Kim",
            "email": "drew.kim@example.com",
            "alt_email": None,
            "phone": "+14155550111",
            "external_account_id": "acct_ext_011",
            "device_id": "dev_ios_011",
            "business_unit": "self_serve",
            "tenant_id": "tenant_us",
        },
        {
            "person_key": "p012",
            "first_name": "Nina",
            "last_name": "Garcia",
            "email": "nina.garcia@example.com",
            "alt_email": "n.garcia@example.net",
            "phone": "+15550101012",
            "external_account_id": "acct_ext_012",
            "device_id": "dev_web_012",
            "business_unit": "enterprise",
            "tenant_id": "tenant_latam",
        },
    ]


def build_cdc_events(
    *,
    seed: int = 42,
    base_time: datetime | None = None,
    days: int = 60,
) -> list[CDCEvent]:
    random.seed(seed)
    start = base_time or datetime(2025, 1, 1, 9, 0, 0, tzinfo=timezone.utc)
    profiles = seed_customer_profiles()
    events: list[CDCEvent] = []

    batch_ids = [f"batch_{idx:03d}" for idx in range(1, 7)]
    plans = ["trial", "starter", "growth", "pro", "enterprise"]
    status_cycle = ["trialing", "active", "past_due", "canceled"]
    support_reasons = ["billing", "onboarding", "bug", "feature_request", "renewal"]
    channels = ["email", "sms", "in_app", "ads", "webinar"]

    for idx, profile in enumerate(profiles, start=1):
        ts = start + timedelta(days=idx)
        customer_pk = f"cust_{idx:04d}"
        customer_after = {
            "customer_id": customer_pk,
            "external_account_id": profile["external_account_id"],
            "email": profile["email"],
            "phone": profile["phone"],
            "first_name": profile["first_name"],
            "last_name": profile["last_name"],
            "tenant_id": profile["tenant_id"],
            "business_unit": profile["business_unit"],
            "customer_status": "lead" if idx % 4 == 0 else "active",
            "created_at": _iso(ts - timedelta(days=3)),
            "updated_at": _iso(ts),
            "source_updated_at": _iso(ts),
        }
        if idx == 6:
            customer_after["email"] = None
        if idx == 10:
            customer_after["phone"] = "not-a-phone"
        events.append(
            _event(
                source_system="account_app",
                operation="insert",
                ts=ts,
                pk=customer_pk,
                before=None,
                after=customer_after,
                batch_id=batch_ids[idx % len(batch_ids)],
            )
        )

        if idx in {1, 4, 5, 10}:
            updated = _mutate(
                customer_after,
                email=profile["alt_email"] or profile["email"],
                customer_status="active",
                updated_at=_iso(ts + timedelta(days=18)),
                source_updated_at=_iso(ts + timedelta(days=18)),
            )
            events.append(
                _event(
                    source_system="account_app",
                    operation="update",
                    ts=ts + timedelta(days=18),
                    pk=customer_pk,
                    before=customer_after,
                    after=updated,
                    batch_id=batch_ids[(idx + 2) % len(batch_ids)],
                )
            )
            customer_after = updated

        sub_pk = f"sub_{idx:04d}"
        sub_status = status_cycle[idx % 3]
        subscription = {
            "subscription_id": sub_pk,
            "tenant_id": profile["tenant_id"],
            "business_unit": profile["business_unit"],
            "customer_id": customer_pk if idx != 3 else None,
            "external_account_id": profile["external_account_id"],
            "email": profile["alt_email"] if idx in {1, 5, 10} else profile["email"],
            "plan_name": plans[idx % len(plans)],
            "subscription_status": sub_status,
            "billing_period": "monthly" if idx % 3 else "annual",
            "mrr": 49 + idx * 30,
            "start_date": _iso(ts + timedelta(days=1)),
            "trial_end_date": _iso(ts + timedelta(days=15)),
            "cancel_at": None,
            "updated_at": _iso(ts + timedelta(days=1)),
        }
        events.append(
            _event(
                source_system="billing_platform",
                operation="insert",
                ts=ts + timedelta(days=1),
                pk=sub_pk,
                before=None,
                after=subscription,
                batch_id=batch_ids[(idx + 1) % len(batch_ids)],
            )
        )

        if idx in {2, 3, 5, 8, 11}:
            changed = _mutate(
                subscription,
                subscription_status="active",
                plan_name="pro" if subscription["plan_name"] != "pro" else "enterprise",
                mrr=subscription["mrr"] + 80,
                updated_at=_iso(ts + timedelta(days=24)),
            )
            events.append(
                _event(
                    source_system="billing_platform",
                    operation="update",
                    ts=ts + timedelta(days=24),
                    pk=sub_pk,
                    before=subscription,
                    after=changed,
                    batch_id=batch_ids[(idx + 3) % len(batch_ids)],
                )
            )
            subscription = changed

        if idx in {4, 9}:
            canceled = _mutate(
                subscription,
                subscription_status="canceled",
                cancel_at=_iso(ts + timedelta(days=35)),
                updated_at=_iso(ts + timedelta(days=35)),
            )
            events.append(
                _event(
                    source_system="billing_platform",
                    operation="update",
                    ts=ts + timedelta(days=35),
                    pk=sub_pk,
                    before=subscription,
                    after=canceled,
                    batch_id=batch_ids[(idx + 4) % len(batch_ids)],
                )
            )

        if idx == 12:
            events.append(
                _event(
                    source_system="billing_platform",
                    operation="delete",
                    ts=ts + timedelta(days=40),
                    pk=sub_pk,
                    before=subscription,
                    after=None,
                    batch_id=batch_ids[(idx + 5) % len(batch_ids)],
                )
            )

        for order_idx in range(1, 4):
            order_ts = ts + timedelta(days=order_idx * 7)
            order_pk = f"ord_{idx:04d}_{order_idx}"
            order = {
                "order_id": order_pk,
                "tenant_id": profile["tenant_id"],
                "business_unit": profile["business_unit"],
                "order_customer_ref": customer_pk if order_idx != 2 else profile["external_account_id"],
                "email": profile["email"] if order_idx != 3 else profile["alt_email"],
                "subscription_id": sub_pk,
                "order_status": "paid",
                "gross_amount": round(39.0 + idx * 8.5 + order_idx * 10.0, 2),
                "currency": "USD",
                "ordered_at": _iso(order_ts),
                "updated_at": _iso(order_ts),
            }
            events.append(
                _event(
                    source_system="commerce_platform",
                    operation="insert",
                    ts=order_ts,
                    pk=order_pk,
                    before=None,
                    after=order,
                    batch_id=batch_ids[(idx + order_idx) % len(batch_ids)],
                )
            )
            if idx in {3, 7} and order_idx == 2:
                refunded = _mutate(
                    order,
                    order_status="refunded",
                    updated_at=_iso(order_ts + timedelta(days=5)),
                )
                events.append(
                    _event(
                        source_system="commerce_platform",
                        operation="update",
                        ts=order_ts + timedelta(days=5),
                        pk=order_pk,
                        before=order,
                        after=refunded,
                        batch_id=batch_ids[(idx + 4) % len(batch_ids)],
                    )
                )

        for event_idx in range(1, 5):
            engagement_ts = ts + timedelta(days=event_idx * 5, hours=random.randint(1, 8))
            event_name = random.choice(["login", "workspace_created", "feature_used", "invite_sent"])
            engagement = {
                "engagement_event_id": f"eng_{idx:04d}_{event_idx}",
                "tenant_id": profile["tenant_id"],
                "business_unit": profile["business_unit"],
                "device_id": profile["device_id"],
                "customer_id": customer_pk if event_idx != 1 else None,
                "email": profile["email"] if event_idx != 4 else None,
                "event_name": event_name,
                "event_count": random.randint(1, 12),
                "session_minutes": random.randint(2, 75),
                "event_timestamp": _iso(engagement_ts),
                "updated_at": _iso(engagement_ts),
            }
            events.append(
                _event(
                    source_system="product_analytics",
                    operation="insert",
                    ts=engagement_ts,
                    pk=engagement["engagement_event_id"],
                    before=None,
                    after=engagement,
                    batch_id=batch_ids[(idx + event_idx + 2) % len(batch_ids)],
                )
            )

        ticket_ts = ts + timedelta(days=random.randint(8, 34))
        support = {
            "support_interaction_id": f"case_{idx:04d}",
            "tenant_id": profile["tenant_id"],
            "business_unit": profile["business_unit"],
            "support_customer_ref": customer_pk if idx not in {2, 5} else profile["external_account_id"],
            "email": profile["alt_email"] if idx in {2, 5} else profile["email"],
            "phone": profile["phone"],
            "reason": support_reasons[idx % len(support_reasons)],
            "priority": "high" if idx in {4, 9, 11} else "normal",
            "status": "open" if idx in {4, 9} else "closed",
            "csat_score": None if idx in {4, 9} else random.randint(2, 5),
            "created_at": _iso(ticket_ts),
            "updated_at": _iso(ticket_ts),
        }
        events.append(
            _event(
                source_system="support_desk",
                operation="insert",
                ts=ticket_ts,
                pk=support["support_interaction_id"],
                before=None,
                after=support,
                batch_id=batch_ids[(idx + 5) % len(batch_ids)],
            )
        )
        if idx in {4, 9}:
            resolved = _mutate(
                support,
                status="closed",
                csat_score=2,
                updated_at=_iso(ticket_ts + timedelta(days=6)),
            )
            events.append(
                _event(
                    source_system="support_desk",
                    operation="update",
                    ts=ticket_ts + timedelta(days=6),
                    pk=support["support_interaction_id"],
                    before=support,
                    after=resolved,
                    batch_id=batch_ids[(idx + 1) % len(batch_ids)],
                )
            )

        marketing = {
            "marketing_touch_id": f"mkt_{idx:04d}",
            "tenant_id": profile["tenant_id"],
            "business_unit": profile["business_unit"],
            "email": profile["alt_email"] or profile["email"],
            "external_account_id": profile["external_account_id"] if idx % 2 == 0 else None,
            "channel": channels[idx % len(channels)],
            "campaign_id": f"cmp_{idx % 4 + 1}",
            "engagement_status": "clicked" if idx % 3 else "opened",
            "marketing_consent_status": "opted_in",
            "email_opt_in": True,
            "sms_opt_in": idx % 4 != 0,
            "push_opt_in": idx % 5 != 0,
            "unsubscribe_status": "subscribed",
            "do_not_contact_flag": False,
            "lead_score": 20 + idx * 5,
            "occurred_at": _iso(ts + timedelta(days=2)),
            "updated_at": _iso(ts + timedelta(days=2)),
        }
        events.append(
            _event(
                source_system="marketing_automation",
                operation="insert",
                ts=ts + timedelta(days=2),
                pk=marketing["marketing_touch_id"],
                before=None,
                after=marketing,
                batch_id=batch_ids[(idx + 2) % len(batch_ids)],
            )
        )
        if idx in {1, 8}:
            suppressed = _mutate(
                marketing,
                engagement_status="unsubscribed",
                marketing_consent_status="opted_out",
                email_opt_in=False,
                sms_opt_in=False,
                push_opt_in=False,
                unsubscribe_status="unsubscribed",
                do_not_contact_flag=True,
                updated_at=_iso(ts + timedelta(days=30)),
            )
            events.append(
                _event(
                    source_system="marketing_automation",
                    operation="update",
                    ts=ts + timedelta(days=30),
                    pk=marketing["marketing_touch_id"],
                    before=marketing,
                    after=suppressed,
                    batch_id=batch_ids[(idx + 3) % len(batch_ids)],
                )
            )

    # Deliberate late-arriving update: occurred earlier, arrives in final batch.
    late_profile = profiles[0]
    before = {
        "customer_id": "cust_0001",
        "external_account_id": late_profile["external_account_id"],
        "email": late_profile["email"],
        "phone": late_profile["phone"],
        "first_name": late_profile["first_name"],
        "last_name": late_profile["last_name"],
        "tenant_id": late_profile["tenant_id"],
        "business_unit": late_profile["business_unit"],
        "customer_status": "lead",
        "updated_at": _iso(start + timedelta(days=2)),
    }
    after = _mutate(
        before,
        customer_status="active",
        updated_at=_iso(start + timedelta(days=4)),
        source_updated_at=_iso(start + timedelta(days=4)),
    )
    events.append(
        _event(
            source_system="account_app",
            operation="update",
            ts=start + timedelta(days=4),
            pk="cust_0001",
            before=before,
            after=after,
            batch_id="batch_999_late_arriving",
            schema_version=1,
        )
    )

    # Intentionally malformed event for rejected record path.
    bad_event = CDCEvent(
        event_id=f"evt_{uuid.uuid4().hex}",
        source_system="account_app",
        source_table="customers",
        operation_type="upsert",
        event_timestamp=_iso(start + timedelta(days=days)),
        record_primary_key="cust_bad_001",
        payload_before=None,
        payload_after={"customer_id": "cust_bad_001", "email": "bad@example.com"},
        batch_id="batch_bad_records",
        schema_version=1,
        tenant_id="tenant_unknown",
    )
    events.append(bad_event)

    # Shuffle by ingestion order, preserving the late-arriving semantic.
    normal = events[:-2]
    random.shuffle(normal)
    return _with_log_metadata(normal + events[-2:], ingestion_start=start + timedelta(days=days + 1))


def write_jsonl(events: Iterable[CDCEvent], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        for event in events:
            fh.write(json.dumps(asdict(event), sort_keys=True) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate realistic CDC event history.")
    parser.add_argument(
        "--output",
        default="data_generation/output/cdc_events.jsonl",
        help="JSONL path for generated CDC envelopes.",
    )
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    events = build_cdc_events(seed=args.seed)
    write_jsonl(events, Path(args.output))
    print(f"generated_events={len(events)} output={args.output}")


if __name__ == "__main__":
    main()
