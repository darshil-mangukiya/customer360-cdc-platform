from __future__ import annotations

import argparse
import csv
import hashlib
import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ingestion.loader import pg_connection


@dataclass(frozen=True)
class DestinationConfig:
    destination_name: str
    export_file: str
    destination_object: str
    max_rows_per_request: int
    max_retries: int
    required_fields: tuple[str, ...] = ()


@dataclass(frozen=True)
class SyncRunLog:
    sync_run_id: str
    destination_name: str
    export_file: str
    destination_object: str
    started_at: str
    ended_at: str
    attempted_count: int
    success_count: int
    failed_count: int
    inserted_count: int
    updated_count: int
    skipped_count: int
    retry_count: int
    rate_limit_events: int
    sync_status: str


@dataclass(frozen=True)
class SyncFailedRow:
    sync_run_id: str
    destination_name: str
    export_file: str
    canonical_customer_id: str | None
    idempotency_key: str
    failure_reason: str
    attempts: int
    is_retryable: bool
    failed_at: str
    payload_json: str
    tenant_id: str | None = None


@dataclass(frozen=True)
class DestinationSyncState:
    destination_name: str
    export_file: str
    canonical_customer_id: str
    destination_record_key: str
    idempotency_key: str
    payload_hash: str
    last_export_timestamp: str
    last_synced_at: str
    last_sync_run_id: str
    sync_action: str
    tenant_id: str | None = None


@dataclass(frozen=True)
class PayloadAuditRow:
    sync_run_id: str
    tenant_id: str | None
    destination_name: str
    export_file: str
    canonical_customer_id: str | None
    idempotency_key: str
    payload_hash: str
    sync_mode: str
    sync_status: str
    retry_count: int
    failure_reason: str | None
    audited_at: str


@dataclass(frozen=True)
class DestinationStatusRow:
    destination_name: str
    latest_sync_run_id: str
    export_file: str
    last_sync_status: str
    attempted_count: int
    success_count: int
    failed_count: int
    retry_count: int
    last_synced_at: str


DESTINATIONS = [
    DestinationConfig("braze", "campaign_target_export.csv", "users/track", 4, 3),
    DestinationConfig("hubspot", "customer_segment_export.csv", "crm/v3/objects/contacts", 5, 3),
    DestinationConfig("salesforce", "churn_risk_export.csv", "Customer_Health__c", 4, 3),
    DestinationConfig("zendesk", "support_priority_export.csv", "api/v2/users", 3, 3),
]

DEFAULT_REQUIRED_FIELDS = {
    "campaign_target_export.csv": (
        "canonical_customer_id",
        "export_timestamp",
        "campaign_target",
        "customer_segment",
        "churn_risk_band",
    ),
    "customer_segment_export.csv": (
        "canonical_customer_id",
        "export_timestamp",
        "customer_segment",
    ),
    "churn_risk_export.csv": (
        "canonical_customer_id",
        "export_timestamp",
        "churn_risk_score",
        "churn_risk_band",
    ),
    "support_priority_export.csv": (
        "canonical_customer_id",
        "export_timestamp",
        "support_priority",
        "churn_risk_band",
    ),
}


def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _load_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _deterministic_bucket(destination: str, row: dict[str, Any]) -> int:
    key = f"{destination}:{row.get('tenant_id', '')}:{row.get('canonical_customer_id', '')}"
    return int(hashlib.sha256(key.encode("utf-8")).hexdigest()[:8], 16)


def _payload_hash(row: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(row, sort_keys=True).encode("utf-8")).hexdigest()


def _destination_record_key(config: DestinationConfig, row: dict[str, Any]) -> str:
    return f"{config.destination_name}:{config.export_file}:{row.get('canonical_customer_id', '')}"


def _idempotency_key(config: DestinationConfig, row: dict[str, Any]) -> str:
    return hashlib.sha256(f"{_destination_record_key(config, row)}:{_payload_hash(row)}".encode("utf-8")).hexdigest()


def _required_fields(config: DestinationConfig) -> tuple[str, ...]:
    return config.required_fields or DEFAULT_REQUIRED_FIELDS.get(config.export_file, ("canonical_customer_id",))


def _classify_attempt(config: DestinationConfig, row: dict[str, Any], attempt: int) -> tuple[bool, str | None, bool]:
    missing_contract_fields = [field for field in _required_fields(config) if not row.get(field)]
    if missing_contract_fields:
        return False, f"destination_contract_missing_fields:{','.join(missing_contract_fields)}", False
    if not row.get("canonical_customer_id"):
        return False, "missing_canonical_customer_id", False
    if not row.get("email_sha256") and not row.get("phone_sha256"):
        return False, "missing_destination_match_key", False

    bucket = _deterministic_bucket(config.destination_name, row)
    if bucket % 29 == 0:
        return False, "destination_validation_error", False
    if bucket % 17 == 0 and attempt < 2:
        return False, "http_429_rate_limit", True
    if bucket % 23 == 0 and attempt < 3:
        return False, "transient_5xx", True
    return True, None, False


def _load_existing_state(path: Path) -> dict[str, DestinationSyncState]:
    if not path.exists():
        return {}
    rows = _load_csv(path)
    return {
        row["destination_record_key"]: DestinationSyncState(
            destination_name=row.get("destination_name", ""),
            export_file=row.get("export_file", ""),
            canonical_customer_id=row.get("canonical_customer_id", ""),
            destination_record_key=row.get("destination_record_key", ""),
            idempotency_key=row.get("idempotency_key", ""),
            payload_hash=row.get("payload_hash", ""),
            last_export_timestamp=row.get("last_export_timestamp", ""),
            last_synced_at=row.get("last_synced_at", ""),
            last_sync_run_id=row.get("last_sync_run_id", ""),
            sync_action=row.get("sync_action", ""),
            tenant_id=row.get("tenant_id"),
        )
        for row in rows
        if row.get("destination_record_key")
    }


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def simulate_destination_sync(
    *,
    export_dir: Path,
    output_dir: Path,
    destinations: list[DestinationConfig] | None = None,
) -> tuple[list[SyncRunLog], list[SyncFailedRow]]:
    configs = destinations or DESTINATIONS
    run_logs: list[SyncRunLog] = []
    failed_rows: list[SyncFailedRow] = []
    payload_audit_rows: list[PayloadAuditRow] = []
    state_path = output_dir / "destination_sync_state.csv"
    destination_state = _load_existing_state(state_path)

    for config in configs:
        rows = _load_csv(export_dir / config.export_file)
        sync_run_id = f"sync_{config.destination_name}_{uuid.uuid4().hex[:10]}"
        started_at = _utc_now()
        success_count = 0
        inserted_count = 0
        updated_count = 0
        skipped_count = 0
        retry_count = 0
        rate_limit_events = 0

        for idx, row in enumerate(rows):
            attempts = 0
            final_reason = None
            final_retryable = False
            idempotency_key = _idempotency_key(config, row)
            payload_hash = _payload_hash(row)
            sync_mode = "upsert"
            sync_status = "failed"

            if idx and idx % config.max_rows_per_request == 0:
                rate_limit_events += 1

            for attempt in range(1, config.max_retries + 1):
                attempts = attempt
                ok, reason, retryable = _classify_attempt(config, row, attempt)
                if ok:
                    success_count += 1
                    final_reason = None
                    record_key = _destination_record_key(config, row)
                    previous = destination_state.get(record_key)
                    if previous and previous.payload_hash == payload_hash:
                        action = "skipped_unchanged"
                        skipped_count += 1
                    elif previous:
                        action = "updated"
                        updated_count += 1
                    else:
                        action = "inserted"
                        inserted_count += 1
                    destination_state[record_key] = DestinationSyncState(
                        destination_name=config.destination_name,
                        export_file=config.export_file,
                        canonical_customer_id=row.get("canonical_customer_id", ""),
                        destination_record_key=record_key,
                        idempotency_key=idempotency_key,
                        payload_hash=payload_hash,
                        last_export_timestamp=row.get("export_timestamp", ""),
                        last_synced_at=_utc_now(),
                        last_sync_run_id=sync_run_id,
                        sync_action=action,
                        tenant_id=row.get("tenant_id"),
                    )
                    sync_mode = "skip" if action == "skipped_unchanged" else "upsert"
                    sync_status = action
                    break
                final_reason = reason
                final_retryable = retryable
                if retryable and attempt < config.max_retries:
                    retry_count += 1
                    if reason == "http_429_rate_limit":
                        rate_limit_events += 1
                    continue
                break

            if final_reason:
                failed_rows.append(
                    SyncFailedRow(
                        sync_run_id=sync_run_id,
                        destination_name=config.destination_name,
                        export_file=config.export_file,
                        canonical_customer_id=row.get("canonical_customer_id"),
                        idempotency_key=idempotency_key,
                        failure_reason=final_reason,
                        attempts=attempts,
                        is_retryable=final_retryable,
                        failed_at=_utc_now(),
                        payload_json=json.dumps(row, sort_keys=True),
                        tenant_id=row.get("tenant_id"),
                    )
                )
                sync_status = "failed"
            payload_audit_rows.append(
                PayloadAuditRow(
                    sync_run_id=sync_run_id,
                    tenant_id=row.get("tenant_id"),
                    destination_name=config.destination_name,
                    export_file=config.export_file,
                    canonical_customer_id=row.get("canonical_customer_id"),
                    idempotency_key=idempotency_key,
                    payload_hash=payload_hash,
                    sync_mode=sync_mode,
                    sync_status=sync_status,
                    retry_count=max(0, attempts - 1),
                    failure_reason=final_reason,
                    audited_at=_utc_now(),
                )
            )

        ended_at = _utc_now()
        failed_count = len([row for row in failed_rows if row.sync_run_id == sync_run_id])
        status = "success" if failed_count == 0 else "partial_success" if success_count else "failed"
        run_logs.append(
            SyncRunLog(
                sync_run_id=sync_run_id,
                destination_name=config.destination_name,
                export_file=config.export_file,
                destination_object=config.destination_object,
                started_at=started_at,
                ended_at=ended_at,
                attempted_count=len(rows),
                success_count=success_count,
                failed_count=failed_count,
                inserted_count=inserted_count,
                updated_count=updated_count,
                skipped_count=skipped_count,
                retry_count=retry_count,
                rate_limit_events=rate_limit_events,
                sync_status=status,
            )
        )

    run_log_rows = [asdict(row) for row in run_logs]
    failed_row_dicts = [asdict(row) for row in failed_rows]
    payload_audit_dicts = [asdict(row) for row in payload_audit_rows]
    config_rows = [asdict(row) for row in configs]
    status_rows = [
        asdict(
            DestinationStatusRow(
                destination_name=row.destination_name,
                latest_sync_run_id=row.sync_run_id,
                export_file=row.export_file,
                last_sync_status=row.sync_status,
                attempted_count=row.attempted_count,
                success_count=row.success_count,
                failed_count=row.failed_count,
                retry_count=row.retry_count,
                last_synced_at=row.ended_at,
            )
        )
        for row in run_logs
    ]
    _write_csv(output_dir / "sync_run_log.csv", run_log_rows, list(run_log_rows[0].keys()) if run_log_rows else [])
    _write_csv(output_dir / "destination_config.csv", config_rows, list(config_rows[0].keys()) if config_rows else [])
    _write_csv(
        output_dir / "payload_audit.csv",
        payload_audit_dicts,
        list(payload_audit_dicts[0].keys()) if payload_audit_dicts else [
            "sync_run_id",
            "tenant_id",
            "destination_name",
            "export_file",
            "canonical_customer_id",
            "idempotency_key",
            "payload_hash",
            "sync_mode",
            "sync_status",
            "retry_count",
            "failure_reason",
            "audited_at",
        ],
    )
    _write_csv(
        output_dir / "destination_status.csv",
        status_rows,
        list(status_rows[0].keys()) if status_rows else [
            "destination_name",
            "latest_sync_run_id",
            "export_file",
            "last_sync_status",
            "attempted_count",
            "success_count",
            "failed_count",
            "retry_count",
            "last_synced_at",
        ],
    )
    _write_csv(
        output_dir / "sync_failed_rows.csv",
        failed_row_dicts,
        list(failed_row_dicts[0].keys()) if failed_row_dicts else [
            "sync_run_id",
            "destination_name",
            "export_file",
            "canonical_customer_id",
            "idempotency_key",
            "failure_reason",
            "attempts",
            "is_retryable",
            "failed_at",
            "payload_json",
            "tenant_id",
        ],
    )
    state_rows = [asdict(row) for row in sorted(destination_state.values(), key=lambda r: r.destination_record_key)]
    _write_csv(
        state_path,
        state_rows,
        list(state_rows[0].keys()) if state_rows else [
            "destination_name",
            "export_file",
            "canonical_customer_id",
            "destination_record_key",
            "idempotency_key",
            "payload_hash",
            "last_export_timestamp",
            "last_synced_at",
            "last_sync_run_id",
            "sync_action",
            "tenant_id",
        ],
    )
    return run_logs, failed_rows


def load_sync_logs_to_postgres(
    *,
    dsn: str,
    run_logs: list[SyncRunLog],
    failed_rows: list[SyncFailedRow],
) -> None:
    with pg_connection(dsn) as conn:
        with conn.cursor() as cur:
            for row in run_logs:
                cur.execute(
                    """
                    insert into activation.reverse_etl_sync_run_log (
                        sync_run_id, destination_name, export_file, destination_object,
                        started_at, ended_at, attempted_count, success_count,
                        failed_count, inserted_count, updated_count, skipped_count,
                        retry_count, rate_limit_events, sync_status
                    )
                    values (
                        %(sync_run_id)s, %(destination_name)s, %(export_file)s,
                        %(destination_object)s, %(started_at)s, %(ended_at)s,
                        %(attempted_count)s, %(success_count)s, %(failed_count)s,
                        %(inserted_count)s, %(updated_count)s, %(skipped_count)s,
                        %(retry_count)s, %(rate_limit_events)s, %(sync_status)s
                    )
                    on conflict (sync_run_id) do nothing
                    """,
                    asdict(row),
                )
            for row in failed_rows:
                cur.execute(
                    """
                    insert into activation.reverse_etl_sync_failed_row (
                        sync_run_id, destination_name, export_file, canonical_customer_id,
                        idempotency_key, failure_reason, attempts, is_retryable, failed_at, payload_json
                    )
                    values (
                        %(sync_run_id)s, %(destination_name)s, %(export_file)s,
                        %(canonical_customer_id)s, %(idempotency_key)s, %(failure_reason)s,
                        %(attempts)s, %(is_retryable)s, %(failed_at)s, %(payload_json)s::jsonb
                    )
                    """,
                    asdict(row),
                )
        conn.commit()


def load_destination_state_to_postgres(*, dsn: str, state_path: Path) -> None:
    state_rows = _load_csv(state_path)
    with pg_connection(dsn) as conn:
        with conn.cursor() as cur:
            for row in state_rows:
                cur.execute(
                    """
                    insert into activation.reverse_etl_destination_state (
                        destination_record_key, destination_name, export_file,
                        canonical_customer_id, idempotency_key, payload_hash,
                        last_export_timestamp, last_synced_at, last_sync_run_id, sync_action
                    )
                    values (
                        %(destination_record_key)s, %(destination_name)s, %(export_file)s,
                        %(canonical_customer_id)s, %(idempotency_key)s, %(payload_hash)s,
                        %(last_export_timestamp)s, %(last_synced_at)s, %(last_sync_run_id)s,
                        %(sync_action)s
                    )
                    on conflict (destination_record_key) do update set
                        idempotency_key = excluded.idempotency_key,
                        payload_hash = excluded.payload_hash,
                        last_export_timestamp = excluded.last_export_timestamp,
                        last_synced_at = excluded.last_synced_at,
                        last_sync_run_id = excluded.last_sync_run_id,
                        sync_action = excluded.sync_action
                    """,
                    row,
                )
        conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulate reverse ETL syncs into downstream destinations.")
    parser.add_argument("--export-dir", default="reverse_etl/exports")
    parser.add_argument("--output-dir", default="reverse_etl/sync_logs")
    parser.add_argument("--dsn")
    args = parser.parse_args()
    run_logs, failed_rows = simulate_destination_sync(
        export_dir=Path(args.export_dir),
        output_dir=Path(args.output_dir),
    )
    if args.dsn:
        load_sync_logs_to_postgres(dsn=args.dsn, run_logs=run_logs, failed_rows=failed_rows)
        load_destination_state_to_postgres(dsn=args.dsn, state_path=Path(args.output_dir) / "destination_sync_state.csv")
    print(
        f"sync_runs={len(run_logs)} failed_rows={len(failed_rows)} "
        f"output_dir={args.output_dir} loaded_to_postgres={bool(args.dsn)}"
    )


if __name__ == "__main__":
    main()
