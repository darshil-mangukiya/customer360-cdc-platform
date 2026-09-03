from __future__ import annotations

import argparse
import csv
from collections import Counter
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from identity_resolution.resolver import CanonicalCustomer, IdentityMapRow, load_events, resolve_identity
from ingestion.cdc_envelope import NormalizedEnvelope
from ingestion.loader import pg_connection
from privacy.activation_policy import build_suppressed_customers
from reverse_etl.exporter import ActivationExportRow, build_activation_rows


@dataclass(frozen=True)
class ValidationFailure:
    check_name: str
    severity: str
    entity_key: str
    failure_reason: str
    observed_value: str
    detected_at: str


@dataclass(frozen=True)
class QualitySummary:
    check_name: str
    severity: str
    status: str
    failure_count: int
    checked_at: str


def _now() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def check_duplicate_canonical_emails(customers: list[CanonicalCustomer]) -> list[ValidationFailure]:
    detected = _now()
    email_counts = Counter(c.primary_email for c in customers if c.primary_email)
    return [
        ValidationFailure(
            check_name="duplicate_canonical_email",
            severity="high",
            entity_key=customer.canonical_customer_id,
            failure_reason="primary_email appears on multiple canonical customers",
            observed_value=str(customer.primary_email),
            detected_at=detected,
        )
        for customer in customers
        if customer.primary_email and email_counts[customer.primary_email] > 1
    ]


def check_broken_identity_mappings(customers: list[CanonicalCustomer], mappings: list[IdentityMapRow]) -> list[ValidationFailure]:
    detected = _now()
    valid_ids = {customer.canonical_customer_id for customer in customers}
    failures = []
    for mapping in mappings:
        if mapping.canonical_customer_id not in valid_ids:
            failures.append(
                ValidationFailure(
                    check_name="broken_identity_mapping",
                    severity="critical",
                    entity_key=f"{mapping.source_system}:{mapping.source_record_id}",
                    failure_reason="mapping references missing canonical customer",
                    observed_value=mapping.canonical_customer_id,
                    detected_at=detected,
                )
            )
    return failures


def check_missing_source_keys(events: list[NormalizedEnvelope]) -> list[ValidationFailure]:
    detected = _now()
    failures = []
    for event in events:
        if not event.record_primary_key or event.record_primary_key.lower() == "none":
            failures.append(
                ValidationFailure(
                    check_name="missing_source_key",
                    severity="critical",
                    entity_key=event.event_id,
                    failure_reason="CDC event has no record_primary_key",
                    observed_value=event.record_primary_key,
                    detected_at=detected,
                )
            )
    return failures


def check_missing_cdc_operations(events: list[NormalizedEnvelope]) -> list[ValidationFailure]:
    detected = _now()
    seen = {(event.source_table, event.operation_type) for event in events}
    failures = []
    for table in {event.source_table for event in events}:
        if (table, "insert") not in seen:
            failures.append(
                ValidationFailure(
                    check_name="missing_cdc_insert_operation",
                    severity="high",
                    entity_key=table,
                    failure_reason="source table has no insert CDC events in the load",
                    observed_value=str(sorted(op for tbl, op in seen if tbl == table)),
                    detected_at=detected,
                )
            )
    return failures


def check_invalid_subscription_states(events: list[NormalizedEnvelope]) -> list[ValidationFailure]:
    detected = _now()
    valid = {"trialing", "active", "past_due", "canceled"}
    failures = []
    for event in events:
        payload = event.payload_after or event.payload_before or {}
        status = payload.get("subscription_status")
        if event.source_table == "subscriptions" and status and status not in valid:
            failures.append(
                ValidationFailure(
                    check_name="invalid_subscription_state",
                    severity="high",
                    entity_key=event.record_primary_key,
                    failure_reason="subscription status is outside allowed domain",
                    observed_value=str(status),
                    detected_at=detected,
                )
            )
    return failures


def check_stale_activation_outputs(rows: list[ActivationExportRow]) -> list[ValidationFailure]:
    detected = _now()
    failures = []
    for row in rows:
        if not row.last_refresh_time:
            failures.append(
                ValidationFailure(
                    check_name="stale_activation_output",
                    severity="critical",
                    entity_key=row.canonical_customer_id,
                    failure_reason="activation row missing last_refresh_time",
                    observed_value="null",
                    detected_at=detected,
                )
            )
    return failures


def check_suppressed_customers_excluded(
    events: list[NormalizedEnvelope],
    customers: list[CanonicalCustomer],
    mappings: list[IdentityMapRow],
    activation_rows: list[ActivationExportRow],
) -> list[ValidationFailure]:
    detected = _now()
    _, suppressed = build_suppressed_customers(events=events, canonical=customers, mappings=mappings)
    suppressed_ids = {row.canonical_customer_id: row.activation_suppression_reason for row in suppressed}
    failures = []
    for row in activation_rows:
        reason = suppressed_ids.get(row.canonical_customer_id)
        if reason:
            failures.append(
                ValidationFailure(
                    check_name="suppressed_customer_exported",
                    severity="critical",
                    entity_key=row.canonical_customer_id,
                    failure_reason="suppressed customer appeared in activation export",
                    observed_value=reason,
                    detected_at=detected,
                )
            )
    return failures


def run_quality_checks(
    events: list[NormalizedEnvelope],
    customers: list[CanonicalCustomer],
    mappings: list[IdentityMapRow],
    activation_rows: list[ActivationExportRow],
) -> tuple[list[ValidationFailure], list[QualitySummary]]:
    checks = [
        ("duplicate_canonical_email", "high", check_duplicate_canonical_emails(customers)),
        ("broken_identity_mapping", "critical", check_broken_identity_mappings(customers, mappings)),
        ("missing_source_key", "critical", check_missing_source_keys(events)),
        ("missing_cdc_insert_operation", "high", check_missing_cdc_operations(events)),
        ("invalid_subscription_state", "high", check_invalid_subscription_states(events)),
        ("stale_activation_output", "critical", check_stale_activation_outputs(activation_rows)),
        (
            "suppressed_customer_exported",
            "critical",
            check_suppressed_customers_excluded(events, customers, mappings, activation_rows),
        ),
    ]
    checked_at = _now()
    failures = [failure for _, _, result in checks for failure in result]
    summary = [
        QualitySummary(
            check_name=name,
            severity=severity,
            status="pass" if not result else "fail",
            failure_count=len(result),
            checked_at=checked_at,
        )
        for name, severity, result in checks
    ]
    return failures, summary


def write_quality_outputs(
    failures: list[ValidationFailure],
    summary: list[QualitySummary],
    output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "validation_failures.csv": (
            [field.name for field in fields(ValidationFailure)],
            [asdict(row) for row in failures],
        ),
        "quality_summary.csv": (
            [field.name for field in fields(QualitySummary)],
            [asdict(row) for row in summary],
        ),
    }
    for filename, (fieldnames, rows) in outputs.items():
        with (output_dir / filename).open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)


def load_quality_to_postgres(
    *,
    dsn: str,
    failures: list[ValidationFailure],
    summary: list[QualitySummary],
) -> None:
    with pg_connection(dsn) as conn:
        with conn.cursor() as cur:
            for row in failures:
                cur.execute(
                    """
                    insert into observability.validation_failures (
                        check_name, severity, entity_key, failure_reason,
                        observed_value, detected_at
                    )
                    values (
                        %(check_name)s, %(severity)s, %(entity_key)s,
                        %(failure_reason)s, %(observed_value)s, %(detected_at)s
                    )
                    """,
                    asdict(row),
                )
            for row in summary:
                cur.execute(
                    """
                    insert into observability.quality_summary (
                        check_name, severity, status, failure_count, checked_at
                    )
                    values (
                        %(check_name)s, %(severity)s, %(status)s,
                        %(failure_count)s, %(checked_at)s
                    )
                    """,
                    asdict(row),
                )
        conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Customer 360 data quality checks.")
    parser.add_argument("--input", default="ingestion/output/raw_cdc_events.jsonl")
    parser.add_argument("--output-dir", default="validation/output")
    parser.add_argument("--dsn", help="Optional Postgres DSN for loading quality tables.")
    args = parser.parse_args()
    events = load_events(Path(args.input))
    customers, mappings, _ = resolve_identity(events)
    activation_rows = build_activation_rows(events, customers, mappings)
    failures, summary = run_quality_checks(events, customers, mappings, activation_rows)
    write_quality_outputs(failures, summary, Path(args.output_dir))
    if args.dsn:
        load_quality_to_postgres(dsn=args.dsn, failures=failures, summary=summary)
    failed_checks = [row.check_name for row in summary if row.status == "fail"]
    print(
        f"checks={len(summary)} failures={len(failures)} "
        f"failed_checks={failed_checks} loaded_to_postgres={bool(args.dsn)}"
    )


if __name__ == "__main__":
    main()
