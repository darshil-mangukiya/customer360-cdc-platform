from __future__ import annotations

import argparse
import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ingestion.cdc_envelope import load_jsonl
from ingestion.loader import build_ingestion_summaries, load_to_postgres, normalize_records, write_local_audit
from ingestion.loader import pg_connection


def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _extract_raw_event(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("raw_event", row)
    if isinstance(raw, str):
        return json.loads(raw)
    return deepcopy(raw)


def repair_rejected_event(row: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    raw = _extract_raw_event(row)
    reasons: list[str] = []
    operation = str(raw.get("operation_type", "")).lower()
    if operation == "upsert":
        raw["operation_type"] = "update" if raw.get("payload_before") else "insert"
        reasons.append("mapped_upsert_to_insert_or_update")

    payload = raw.get("payload_after") if raw.get("operation_type") != "delete" else raw.get("payload_before")
    if raw.get("source_table") == "customers" and isinstance(payload, dict):
        now = raw.get("event_timestamp") or _utc_now()
        defaults = {
            "customer_id": raw.get("record_primary_key"),
            "external_account_id": f"unknown_{raw.get('record_primary_key')}",
            "tenant_id": "tenant_unknown",
            "business_unit": "unknown",
            "customer_status": "active",
            "updated_at": now,
            "source_updated_at": now,
        }
        for field, value in defaults.items():
            if payload.get(field) in {None, ""}:
                payload[field] = value
                reasons.append(f"filled_{field}")
    raw["batch_id"] = f"{raw.get('batch_id', 'unknown')}_dlq_reprocessed"
    return raw, reasons


def reprocess_dlq(
    *,
    input_path: Path,
    output_dir: Path,
    dsn: str | None = None,
    dry_run: bool = False,
) -> tuple[int, int, int]:
    rows = load_jsonl(str(input_path))
    repaired_rows: list[dict[str, Any]] = []
    repair_audit: list[dict[str, Any]] = []
    for row in rows:
        repaired, reasons = repair_rejected_event(row)
        repaired_rows.append(repaired)
        repair_audit.append(
            {
                "original_event_id": row.get("event_id") or repaired.get("event_id"),
                "repaired_event_id": repaired.get("event_id"),
                "repair_actions": reasons,
                "repaired_at": _utc_now(),
            }
        )

    landed, rejected = normalize_records(repaired_rows)
    started_at = _utc_now()
    ended_at = _utc_now()
    summaries = build_ingestion_summaries(repaired_rows, landed, rejected, started_at, ended_at)
    if not dry_run:
        write_local_audit(output_dir=output_dir, landed=landed, rejected=rejected, summaries=summaries)
        with (output_dir / "dlq_repair_audit.jsonl").open("w", encoding="utf-8") as fh:
            for row in repair_audit:
                fh.write(json.dumps(row, sort_keys=True, default=str) + "\n")
        with (output_dir / "repaired_events.jsonl").open("w", encoding="utf-8") as fh:
            for row in repaired_rows:
                fh.write(json.dumps(row, sort_keys=True, default=str) + "\n")
        if dsn:
            load_to_postgres(dsn=dsn, landed=landed, rejected=rejected, summaries=summaries)
            load_dlq_reprocess_audit(
                dsn=dsn,
                repair_audit=repair_audit,
                load_status="success" if landed and not rejected else "partial_success" if landed else "failed",
            )
    return len(repaired_rows), len(landed), len(rejected)


def load_dlq_reprocess_audit(*, dsn: str, repair_audit: list[dict[str, Any]], load_status: str) -> None:
    with pg_connection(dsn) as conn:
        with conn.cursor() as cur:
            for row in repair_audit:
                cur.execute(
                    """
                    insert into audit.dlq_reprocess_log (
                        original_event_id, repaired_event_id, repair_actions,
                        repaired_at, load_status
                    )
                    values (
                        %(original_event_id)s, %(repaired_event_id)s, %(repair_actions)s::jsonb,
                        %(repaired_at)s, %(load_status)s
                    )
                    """,
                    {
                        **row,
                        "repair_actions": json.dumps(row["repair_actions"], sort_keys=True),
                        "load_status": load_status,
                    },
                )
        conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Repair and reprocess rejected CDC events.")
    parser.add_argument("--input", default="ingestion/output/rejected_events.jsonl")
    parser.add_argument("--output-dir", default="ingestion/dlq_reprocessed")
    parser.add_argument("--dsn")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    repaired, landed, rejected = reprocess_dlq(
        input_path=Path(args.input),
        output_dir=Path(args.output_dir),
        dsn=args.dsn,
        dry_run=args.dry_run,
    )
    print(f"repaired={repaired} landed={landed} rejected={rejected} dry_run={args.dry_run}")


if __name__ == "__main__":
    main()
