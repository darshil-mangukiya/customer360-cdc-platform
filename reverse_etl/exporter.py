from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from identity_resolution.resolver import (
    CanonicalCustomer,
    IdentityMapRow,
    load_events,
    resolve_identity,
)
from ingestion.cdc_envelope import NormalizedEnvelope
from ingestion.loader import pg_connection
from privacy.activation_policy import build_suppressed_customers, write_privacy_activation_outputs
from privacy.deletion_workflow import load_deletion_requests
from privacy.pii import hash_email, hash_phone


@dataclass(frozen=True)
class ActivationExportRow:
    canonical_customer_id: str
    tenant_id: str | None
    business_unit: str | None
    email_sha256: str | None
    phone_sha256: str | None
    export_timestamp: str
    lifecycle_stage: str
    customer_segment: str
    health_score: int
    churn_risk_score: int
    churn_risk_band: str
    support_priority: str
    campaign_target: str
    last_refresh_time: str
    source_lineage_refs: str


def _payload(event: NormalizedEnvelope) -> dict[str, Any] | None:
    return event.payload_after if event.operation_type != "delete" else event.payload_before


def _tenant_scope(tenant_id: str | None) -> str:
    return tenant_id or "tenant_unknown"


def _index_mappings(mappings: list[IdentityMapRow]) -> dict[tuple[str, str, str], str]:
    return {
        (_tenant_scope(row.tenant_id), row.source_system, row.source_record_id): row.canonical_customer_id
        for row in mappings
    }


def _collect_features(events: list[NormalizedEnvelope], mappings: list[IdentityMapRow]) -> dict[str, dict[str, Any]]:
    canonical_by_source = _index_mappings(mappings)
    features: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "paid_orders": 0,
            "refunded_orders": 0,
            "gross_revenue": 0.0,
            "engagement_events": 0,
            "session_minutes": 0,
            "open_support_cases": 0,
            "low_csat_cases": 0,
            "marketing_unsubscribed": False,
            "lead_score": 0,
            "subscription_statuses": [],
            "plans": [],
            "lineage": set(),
        }
    )
    for event in events:
        payload = _payload(event)
        if not payload:
            continue
        canonical_id = canonical_by_source.get((_tenant_scope(event.tenant_id), event.source_system, event.record_primary_key))
        if canonical_id is None:
            continue
        row = features[canonical_id]
        row["lineage"].add(f"{event.source_system}:{event.record_primary_key}")
        if event.source_table == "orders":
            if payload.get("order_status") == "paid":
                row["paid_orders"] += 1
                row["gross_revenue"] += float(payload.get("gross_amount") or 0)
            if payload.get("order_status") == "refunded":
                row["refunded_orders"] += 1
        elif event.source_table == "engagement_events":
            row["engagement_events"] += int(payload.get("event_count") or 0)
            row["session_minutes"] += int(payload.get("session_minutes") or 0)
        elif event.source_table == "support_interactions":
            if payload.get("status") == "open":
                row["open_support_cases"] += 1
            if payload.get("csat_score") is not None and int(payload["csat_score"]) <= 2:
                row["low_csat_cases"] += 1
        elif event.source_table == "marketing_engagement":
            row["lead_score"] = max(int(payload.get("lead_score") or 0), row["lead_score"])
            if payload.get("engagement_status") == "unsubscribed":
                row["marketing_unsubscribed"] = True
        elif event.source_table == "subscriptions":
            row["subscription_statuses"].append(payload.get("subscription_status"))
            row["plans"].append(payload.get("plan_name"))
    return features


def _lifecycle_stage(customer: CanonicalCustomer, feature: dict[str, Any]) -> str:
    statuses = [s for s in feature["subscription_statuses"] if s]
    if "canceled" in statuses:
        return "churned"
    if "past_due" in statuses:
        return "at_risk"
    if "active" in statuses:
        return "active_customer"
    if "trialing" in statuses or customer.customer_status == "lead":
        return "trial_or_lead"
    return "unknown"


def _score(customer: CanonicalCustomer, feature: dict[str, Any]) -> tuple[int, int, str, str, str, str]:
    health = 55
    health += min(feature["engagement_events"], 30)
    health += min(int(feature["gross_revenue"] / 50), 20)
    health -= feature["open_support_cases"] * 15
    health -= feature["low_csat_cases"] * 20
    health -= feature["refunded_orders"] * 10
    health = max(0, min(100, health))

    churn = 100 - health
    statuses = set(feature["subscription_statuses"])
    if "past_due" in statuses:
        churn += 20
    if "canceled" in statuses:
        churn += 40
    if feature["marketing_unsubscribed"]:
        churn += 10
    churn = max(0, min(100, churn))
    band = "high" if churn >= 70 else "medium" if churn >= 40 else "low"

    segment = "enterprise_growth" if customer.business_unit == "enterprise" else "self_serve_core"
    if feature["gross_revenue"] >= 250:
        segment = "high_value"
    if feature["paid_orders"] == 0:
        segment = "activation_needed"

    support_priority = "p1_retention" if band == "high" and feature["open_support_cases"] else "standard"
    if feature["low_csat_cases"]:
        support_priority = "p2_csat_recovery"

    if band == "high":
        campaign = "save_offer_or_customer_success_outreach"
    elif feature["lead_score"] >= 70:
        campaign = "expansion_nurture"
    elif segment == "activation_needed":
        campaign = "onboarding_activation"
    else:
        campaign = "product_education"

    return health, churn, band, segment, support_priority, campaign


def build_activation_rows(
    events: list[NormalizedEnvelope],
    canonical: list[CanonicalCustomer],
    mappings: list[IdentityMapRow],
    *,
    exclude_suppressed: bool = True,
) -> list[ActivationExportRow]:
    export_time = datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")
    features = _collect_features(events, mappings)
    _, suppressed = build_suppressed_customers(events=events, canonical=canonical, mappings=mappings)
    suppressed_ids = {row.canonical_customer_id for row in suppressed} if exclude_suppressed else set()
    rows: list[ActivationExportRow] = []
    for customer in canonical:
        if customer.canonical_customer_id in suppressed_ids:
            continue
        feature = features.get(customer.canonical_customer_id, defaultdict(int))
        health, churn, band, segment, support_priority, campaign = _score(customer, feature)
        rows.append(
            ActivationExportRow(
                canonical_customer_id=customer.canonical_customer_id,
                tenant_id=customer.tenant_id,
                business_unit=customer.business_unit,
                email_sha256=hash_email(customer.primary_email),
                phone_sha256=hash_phone(customer.primary_phone),
                export_timestamp=export_time,
                lifecycle_stage=_lifecycle_stage(customer, feature),
                customer_segment=segment,
                health_score=health,
                churn_risk_score=churn,
                churn_risk_band=band,
                support_priority=support_priority,
                campaign_target=campaign,
                last_refresh_time=export_time,
                source_lineage_refs="|".join(sorted(feature["lineage"])),
            )
        )
    return rows


def write_exports(rows: list[ActivationExportRow], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    all_rows = [asdict(row) for row in rows]
    export_specs = {
        "customer_segment_export.csv": ["canonical_customer_id", "tenant_id", "business_unit", "email_sha256", "phone_sha256", "export_timestamp", "customer_segment", "source_lineage_refs"],
        "churn_risk_export.csv": ["canonical_customer_id", "tenant_id", "email_sha256", "phone_sha256", "export_timestamp", "churn_risk_score", "churn_risk_band", "last_refresh_time", "source_lineage_refs"],
        "lifecycle_stage_export.csv": ["canonical_customer_id", "tenant_id", "email_sha256", "phone_sha256", "export_timestamp", "lifecycle_stage", "last_refresh_time"],
        "customer_health_score_export.csv": ["canonical_customer_id", "tenant_id", "email_sha256", "phone_sha256", "export_timestamp", "health_score", "last_refresh_time", "source_lineage_refs"],
        "support_priority_export.csv": ["canonical_customer_id", "tenant_id", "email_sha256", "phone_sha256", "export_timestamp", "support_priority", "churn_risk_band", "source_lineage_refs"],
        "campaign_target_export.csv": ["canonical_customer_id", "tenant_id", "business_unit", "email_sha256", "phone_sha256", "export_timestamp", "campaign_target", "customer_segment", "churn_risk_band"],
    }
    for filename, fields in export_specs.items():
        with (output_dir / filename).open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields)
            writer.writeheader()
            for row in all_rows:
                writer.writerow({field: row[field] for field in fields})
    with (output_dir / "activation_payloads.jsonl").open("w", encoding="utf-8") as fh:
        for row in all_rows:
            fh.write(json.dumps(row, sort_keys=True) + "\n")


def load_activation_to_postgres(*, dsn: str, rows: list[ActivationExportRow]) -> None:
    with pg_connection(dsn) as conn:
        with conn.cursor() as cur:
            for row in rows:
                data = asdict(row)
                cur.execute(
                    """
                    insert into activation.export_customer_segment (
                        canonical_customer_id, tenant_id, business_unit, email_sha256,
                        phone_sha256, export_timestamp,
                        customer_segment, source_lineage_refs
                    )
                    values (
                        %(canonical_customer_id)s, %(tenant_id)s, %(business_unit)s,
                        %(email_sha256)s, %(phone_sha256)s, %(export_timestamp)s,
                        %(customer_segment)s, %(source_lineage_refs)s
                    )
                    on conflict (canonical_customer_id) do update set
                        tenant_id = excluded.tenant_id,
                        business_unit = excluded.business_unit,
                        email_sha256 = excluded.email_sha256,
                        phone_sha256 = excluded.phone_sha256,
                        export_timestamp = excluded.export_timestamp,
                        customer_segment = excluded.customer_segment,
                        source_lineage_refs = excluded.source_lineage_refs
                    """,
                    data,
                )
                cur.execute(
                    """
                    insert into activation.export_churn_risk (
                        canonical_customer_id, tenant_id, email_sha256, phone_sha256,
                        export_timestamp, churn_risk_score, churn_risk_band,
                        last_refresh_time, source_lineage_refs
                    )
                    values (
                        %(canonical_customer_id)s, %(tenant_id)s, %(email_sha256)s,
                        %(phone_sha256)s, %(export_timestamp)s, %(churn_risk_score)s,
                        %(churn_risk_band)s, %(last_refresh_time)s, %(source_lineage_refs)s
                    )
                    on conflict (canonical_customer_id) do update set
                        tenant_id = excluded.tenant_id,
                        email_sha256 = excluded.email_sha256,
                        phone_sha256 = excluded.phone_sha256,
                        export_timestamp = excluded.export_timestamp,
                        churn_risk_score = excluded.churn_risk_score,
                        churn_risk_band = excluded.churn_risk_band,
                        last_refresh_time = excluded.last_refresh_time,
                        source_lineage_refs = excluded.source_lineage_refs
                    """,
                    data,
                )
                cur.execute(
                    """
                    insert into activation.export_lifecycle_stage (
                        canonical_customer_id, tenant_id, email_sha256, phone_sha256,
                        export_timestamp, lifecycle_stage, last_refresh_time
                    )
                    values (
                        %(canonical_customer_id)s, %(tenant_id)s, %(email_sha256)s,
                        %(phone_sha256)s, %(export_timestamp)s, %(lifecycle_stage)s,
                        %(last_refresh_time)s
                    )
                    on conflict (canonical_customer_id) do update set
                        tenant_id = excluded.tenant_id,
                        email_sha256 = excluded.email_sha256,
                        phone_sha256 = excluded.phone_sha256,
                        export_timestamp = excluded.export_timestamp,
                        lifecycle_stage = excluded.lifecycle_stage,
                        last_refresh_time = excluded.last_refresh_time
                    """,
                    data,
                )
                cur.execute(
                    """
                    insert into activation.export_customer_health_score (
                        canonical_customer_id, tenant_id, email_sha256, phone_sha256,
                        export_timestamp, health_score, last_refresh_time, source_lineage_refs
                    )
                    values (
                        %(canonical_customer_id)s, %(tenant_id)s, %(email_sha256)s,
                        %(phone_sha256)s, %(export_timestamp)s, %(health_score)s,
                        %(last_refresh_time)s, %(source_lineage_refs)s
                    )
                    on conflict (canonical_customer_id) do update set
                        tenant_id = excluded.tenant_id,
                        email_sha256 = excluded.email_sha256,
                        phone_sha256 = excluded.phone_sha256,
                        export_timestamp = excluded.export_timestamp,
                        health_score = excluded.health_score,
                        last_refresh_time = excluded.last_refresh_time,
                        source_lineage_refs = excluded.source_lineage_refs
                    """,
                    data,
                )
                cur.execute(
                    """
                    insert into activation.export_support_priority (
                        canonical_customer_id, tenant_id, email_sha256, phone_sha256,
                        export_timestamp, support_priority, churn_risk_band, source_lineage_refs
                    )
                    values (
                        %(canonical_customer_id)s, %(tenant_id)s, %(email_sha256)s,
                        %(phone_sha256)s, %(export_timestamp)s, %(support_priority)s,
                        %(churn_risk_band)s, %(source_lineage_refs)s
                    )
                    on conflict (canonical_customer_id) do update set
                        tenant_id = excluded.tenant_id,
                        email_sha256 = excluded.email_sha256,
                        phone_sha256 = excluded.phone_sha256,
                        export_timestamp = excluded.export_timestamp,
                        support_priority = excluded.support_priority,
                        churn_risk_band = excluded.churn_risk_band,
                        source_lineage_refs = excluded.source_lineage_refs
                    """,
                    data,
                )
                cur.execute(
                    """
                    insert into activation.export_campaign_target (
                        canonical_customer_id, tenant_id, business_unit, email_sha256,
                        phone_sha256, export_timestamp, campaign_target, customer_segment,
                        churn_risk_band
                    )
                    values (
                        %(canonical_customer_id)s, %(tenant_id)s, %(business_unit)s,
                        %(email_sha256)s, %(phone_sha256)s, %(export_timestamp)s,
                        %(campaign_target)s, %(customer_segment)s, %(churn_risk_band)s
                    )
                    on conflict (canonical_customer_id) do update set
                        tenant_id = excluded.tenant_id,
                        business_unit = excluded.business_unit,
                        email_sha256 = excluded.email_sha256,
                        phone_sha256 = excluded.phone_sha256,
                        export_timestamp = excluded.export_timestamp,
                        campaign_target = excluded.campaign_target,
                        customer_segment = excluded.customer_segment,
                        churn_risk_band = excluded.churn_risk_band
                    """,
                    data,
                )
        conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate reverse ETL activation exports.")
    parser.add_argument("--input", default="ingestion/output/raw_cdc_events.jsonl")
    parser.add_argument("--output-dir", default="reverse_etl/exports")
    parser.add_argument("--dsn", help="Optional Postgres DSN for loading activation tables.")
    parser.add_argument(
        "--deletion-requests-path",
        default="privacy/output/deletion_requests.csv",
        help="Deletion requests appended by privacy.deletion_workflow / the API; loaded if present.",
    )
    args = parser.parse_args()
    events = load_events(Path(args.input))
    canonical, mappings, _ = resolve_identity(events)
    deletion_requests = load_deletion_requests(Path(args.deletion_requests_path))
    # Build every candidate row unsuppressed, then apply one deletion-aware
    # suppression pass below, so consent-only and deletion-request suppression are
    # computed exactly once, consistently, from the same consent history.
    all_rows = build_activation_rows(events, canonical, mappings, exclude_suppressed=False)
    consent_by_customer, suppressed = build_suppressed_customers(
        events=events, canonical=canonical, mappings=mappings, deletion_requests=deletion_requests
    )
    suppressed_ids = {row.canonical_customer_id for row in suppressed}
    rows = [row for row in all_rows if row.canonical_customer_id not in suppressed_ids]
    write_exports(rows, Path(args.output_dir))
    write_privacy_activation_outputs(
        consent_by_customer=consent_by_customer,
        suppressed=suppressed,
        output_dir=Path("privacy/output"),
    )
    if args.dsn:
        load_activation_to_postgres(dsn=args.dsn, rows=rows)
    print(f"activation_rows={len(rows)} output_dir={args.output_dir} loaded_to_postgres={bool(args.dsn)}")


if __name__ == "__main__":
    main()
