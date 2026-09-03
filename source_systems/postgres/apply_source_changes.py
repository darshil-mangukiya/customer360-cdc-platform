from __future__ import annotations

import argparse
import os
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
from typing import Any

from data_generation.cdc_generator import build_cdc_events
from ingestion.loader import pg_connection


TABLE_PRIMARY_KEYS = {
    "customers": "customer_id",
    "subscriptions": "subscription_id",
    "orders": "order_id",
    "engagement_events": "engagement_event_id",
    "support_interactions": "support_interaction_id",
    "marketing_engagement": "marketing_touch_id",
}


TABLE_COLUMNS = {
    "customers": [
        "customer_id",
        "external_account_id",
        "email",
        "phone",
        "first_name",
        "last_name",
        "tenant_id",
        "business_unit",
        "customer_status",
        "created_at",
        "updated_at",
        "source_updated_at",
    ],
    "subscriptions": [
        "subscription_id",
        "tenant_id",
        "business_unit",
        "customer_id",
        "external_account_id",
        "email",
        "plan_name",
        "subscription_status",
        "billing_period",
        "mrr",
        "start_date",
        "trial_end_date",
        "cancel_at",
        "updated_at",
    ],
    "orders": [
        "order_id",
        "tenant_id",
        "business_unit",
        "order_customer_ref",
        "email",
        "subscription_id",
        "order_status",
        "gross_amount",
        "currency",
        "ordered_at",
        "updated_at",
    ],
    "engagement_events": [
        "engagement_event_id",
        "tenant_id",
        "business_unit",
        "device_id",
        "customer_id",
        "email",
        "event_name",
        "event_count",
        "session_minutes",
        "event_timestamp",
        "updated_at",
    ],
    "support_interactions": [
        "support_interaction_id",
        "tenant_id",
        "business_unit",
        "support_customer_ref",
        "email",
        "phone",
        "reason",
        "priority",
        "status",
        "csat_score",
        "created_at",
        "updated_at",
    ],
    "marketing_engagement": [
        "marketing_touch_id",
        "tenant_id",
        "business_unit",
        "email",
        "external_account_id",
        "channel",
        "campaign_id",
        "engagement_status",
        "marketing_consent_status",
        "email_opt_in",
        "sms_opt_in",
        "push_opt_in",
        "unsubscribe_status",
        "do_not_contact_flag",
        "lead_score",
        "occurred_at",
        "updated_at",
    ],
}


def _sql_module() -> Any:
    try:
        from psycopg import sql
    except ImportError as exc:
        raise RuntimeError("Install psycopg[binary] to apply source changes into Postgres.") from exc
    return sql


def _coerce_value(value: Any) -> Any:
    if isinstance(value, float):
        return Decimal(str(value))
    return value


def _upsert_payload(cur: Any, table: str, payload: dict[str, Any]) -> None:
    sql = _sql_module()
    columns = TABLE_COLUMNS[table]
    pk = TABLE_PRIMARY_KEYS[table]
    row = {column: _coerce_value(payload.get(column)) for column in columns}
    updates = [column for column in columns if column != pk]
    query = sql.SQL(
        """
        insert into public.{table} ({columns})
        values ({values_sql})
        on conflict ({pk}) do update set {updates}
        """
    ).format(
        table=sql.Identifier(table),
        columns=sql.SQL(", ").join(sql.Identifier(column) for column in columns),
        values_sql=sql.SQL(", ").join(sql.Placeholder(column) for column in columns),
        pk=sql.Identifier(pk),
        updates=sql.SQL(", ").join(
            sql.SQL("{column} = excluded.{column}").format(column=sql.Identifier(column)) for column in updates
        ),
    )
    cur.execute(query, row)


def _delete_payload(cur: Any, table: str, record_primary_key: str) -> None:
    sql = _sql_module()
    pk = TABLE_PRIMARY_KEYS[table]
    query = sql.SQL("delete from public.{table} where {pk} = %(record_primary_key)s").format(
        table=sql.Identifier(table),
        pk=sql.Identifier(pk),
    )
    cur.execute(query, {"record_primary_key": record_primary_key})


def apply_demo_source_changes(*, dsn: str, seed: int = 42) -> dict[str, int]:
    events = [asdict(event) for event in build_cdc_events(seed=seed)]
    counts = {"insert": 0, "update": 0, "delete": 0, "skipped": 0}
    with pg_connection(dsn) as conn:
        with conn.cursor() as cur:
            for event in events:
                operation = event["operation_type"]
                table = event["source_table"]
                if operation not in counts or table not in TABLE_PRIMARY_KEYS:
                    counts["skipped"] += 1
                    continue
                if operation == "delete":
                    _delete_payload(cur, table, event["record_primary_key"])
                else:
                    payload = event["payload_after"]
                    if not payload:
                        counts["skipped"] += 1
                        continue
                    _upsert_payload(cur, table, payload)
                counts[operation] += 1
        conn.commit()
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply demo operational changes to the Debezium source Postgres.")
    parser.add_argument(
        "--dsn",
        default=os.getenv("SOURCE_DSN", "postgresql://c360:c360@localhost:5433/customer_ops"),
    )
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    counts = apply_demo_source_changes(dsn=args.dsn, seed=args.seed)
    print(" ".join(f"{key}={value}" for key, value in counts.items()))


if __name__ == "__main__":
    main()
