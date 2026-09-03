from __future__ import annotations

import argparse
import csv
import json
import os
from collections import Counter
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from ingestion.cdc_envelope import CDCValidationError, NormalizedEnvelope, load_jsonl, normalize_event
from ingestion.cdc_state import filter_idempotent_events, write_cdc_audit_outputs


@dataclass
class IngestionSummary:
    batch_id: str
    source_system: str
    source_table: str
    event_count: int
    landed_count: int
    rejected_count: int
    load_start_time: str
    load_end_time: str
    schema_version: int
    load_status: str


@contextmanager
def pg_connection(dsn: str) -> Iterator[Any]:
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("Install psycopg[binary] to load PostgreSQL tables.") from exc

    with psycopg.connect(dsn) as conn:
        yield conn


def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def build_rejection_record(
    raw: dict[str, Any], exc: Exception, *, failure_stage: str = "contract_validation"
) -> dict[str, Any]:
    """Build one stable DLQ record for fixture or live Debezium failures."""
    category = exc.category if isinstance(exc, CDCValidationError) else "INVALID_PAYLOAD"
    retry_eligible = exc.retry_eligible if isinstance(exc, CDCValidationError) else False
    tenant_id = raw.get("tenant_id") or (raw.get("payload_after") or raw.get("payload_before") or {}).get("tenant_id")
    return {
        "event_id": raw.get("event_id"),
        "tenant_id": tenant_id,
        "source_system": raw.get("source_system"),
        "source_table": raw.get("source_table"),
        "batch_id": raw.get("batch_id"),
        "failure_stage": failure_stage,
        "failure_category": category,
        "error_detail": str(exc),
        "rejection_reason": str(exc),
        "event_reference": raw.get("event_hash") or raw.get("event_id"),
        "retry_eligible": retry_eligible,
        "raw_event": raw,
        "rejected_at": _utc_now(),
    }


def normalize_records(
    records: list[dict[str, Any]],
    *,
    deduplicate: bool = True,
) -> tuple[list[NormalizedEnvelope], list[dict[str, Any]]]:
    landed: list[NormalizedEnvelope] = []
    rejected: list[dict[str, Any]] = []
    for raw in records:
        try:
            landed.append(normalize_event(raw))
        except Exception as exc:
            rejected.append(build_rejection_record(raw, exc))
    return (filter_idempotent_events(landed) if deduplicate else landed), rejected


def build_ingestion_summaries(
    records: list[dict[str, Any]],
    landed: list[NormalizedEnvelope],
    rejected: list[dict[str, Any]],
    started_at: str,
    ended_at: str,
) -> list[IngestionSummary]:
    event_counts = Counter(
        (r.get("batch_id"), r.get("source_system"), r.get("source_table"), r.get("schema_version", 1))
        for r in records
    )
    landed_counts = Counter((e.batch_id, e.source_system, e.source_table, e.schema_version) for e in landed)
    rejected_counts = Counter(
        (r.get("batch_id"), r.get("source_system"), r.get("source_table"), 1) for r in rejected
    )
    summaries: list[IngestionSummary] = []
    for key, event_count in sorted(event_counts.items()):
        batch_id, source_system, source_table, schema_version = key
        landed_count = landed_counts[key]
        rejected_count = rejected_counts[(batch_id, source_system, source_table, 1)]
        status = "success"
        if rejected_count and landed_count:
            status = "partial_success"
        elif rejected_count and not landed_count:
            status = "failed"
        summaries.append(
            IngestionSummary(
                batch_id=str(batch_id),
                source_system=str(source_system),
                source_table=str(source_table),
                event_count=event_count,
                landed_count=landed_count,
                rejected_count=rejected_count,
                load_start_time=started_at,
                load_end_time=ended_at,
                schema_version=int(schema_version or 1),
                load_status=status,
            )
        )
    return summaries


def load_to_postgres(
    *,
    dsn: str,
    landed: list[NormalizedEnvelope],
    rejected: list[dict[str, Any]],
    summaries: list[IngestionSummary],
) -> None:
    with pg_connection(dsn) as conn:
        with conn.cursor() as cur:
            for event in landed:
                cur.execute(
                    """
                    insert into raw.raw_cdc_events (
                        event_id, tenant_id, source_system, source_table, operation_type,
                        event_timestamp, record_primary_key, payload_before,
                        payload_after, batch_id, schema_version, topic_name,
                        envelope_hash, ingested_at, event_sequence_number,
                        source_transaction_id, source_lsn, source_commit_timestamp,
                        ingestion_timestamp, kafka_topic, kafka_partition, kafka_offset,
                        event_hash, replay_batch_id, is_replay
                    )
                    values (
                        %(event_id)s, %(tenant_id)s, %(source_system)s, %(source_table)s, %(operation_type)s,
                        %(event_timestamp)s, %(record_primary_key)s, %(payload_before)s::jsonb,
                        %(payload_after)s::jsonb, %(batch_id)s, %(schema_version)s, %(topic_name)s,
                        %(envelope_hash)s, %(ingested_at)s, %(event_sequence_number)s,
                        %(source_transaction_id)s, %(source_lsn)s, %(source_commit_timestamp)s,
                        %(ingestion_timestamp)s, %(kafka_topic)s, %(kafka_partition)s, %(kafka_offset)s,
                        %(event_hash)s, %(replay_batch_id)s, %(is_replay)s
                    )
                    on conflict (event_id) do nothing
                    """,
                    {
                        **event.as_dict(),
                        "payload_before": json.dumps(event.payload_before),
                        "payload_after": json.dumps(event.payload_after),
                    },
                )
            for row in rejected:
                cur.execute(
                    """
                    insert into raw.rejected_events (
                        event_id, source_system, source_table, batch_id,
                        rejection_reason, raw_event, rejected_at
                    )
                    values (
                        %(event_id)s, %(source_system)s, %(source_table)s, %(batch_id)s,
                        %(rejection_reason)s, %(raw_event)s::jsonb, %(rejected_at)s
                    )
                    """,
                    {**row, "raw_event": json.dumps(row["raw_event"])},
                )
            for summary in summaries:
                cur.execute(
                    """
                    insert into audit.ingestion_log (
                        batch_id, source_system, source_table, event_count, landed_count,
                        rejected_count, load_start_time, load_end_time, schema_version, load_status
                    )
                    values (
                        %(batch_id)s, %(source_system)s, %(source_table)s, %(event_count)s,
                        %(landed_count)s, %(rejected_count)s, %(load_start_time)s,
                        %(load_end_time)s, %(schema_version)s, %(load_status)s
                    )
                    """,
                    asdict(summary),
                )
        conn.commit()


def write_local_audit(
    *,
    output_dir: Path,
    landed: list[NormalizedEnvelope],
    rejected: list[dict[str, Any]],
    summaries: list[IngestionSummary],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / "raw_cdc_events.jsonl").open("w", encoding="utf-8") as fh:
        for event in landed:
            fh.write(json.dumps(event.as_dict(), sort_keys=True, default=str) + "\n")
    with (output_dir / "rejected_events.jsonl").open("w", encoding="utf-8") as fh:
        for row in rejected:
            fh.write(json.dumps(row, sort_keys=True, default=str) + "\n")
    with (output_dir / "ingestion_log.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(asdict(summaries[0]).keys()) if summaries else [])
        if summaries:
            writer.writeheader()
            writer.writerows(asdict(summary) for summary in summaries)


def ingest_file(
    path: Path,
    *,
    output_dir: Path,
    dsn: str | None = None,
) -> tuple[list[NormalizedEnvelope], list[dict[str, Any]], list[IngestionSummary]]:
    started_at = _utc_now()
    records = load_jsonl(str(path))
    normalized_all, rejected = normalize_records(records, deduplicate=False)
    landed = filter_idempotent_events(normalized_all)
    ended_at = _utc_now()
    summaries = build_ingestion_summaries(records, landed, rejected, started_at, ended_at)
    write_local_audit(output_dir=output_dir, landed=landed, rejected=rejected, summaries=summaries)
    write_cdc_audit_outputs(normalized_all, output_dir)
    if dsn:
        load_to_postgres(dsn=dsn, landed=landed, rejected=rejected, summaries=summaries)
    return landed, rejected, summaries


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize and land CDC events.")
    parser.add_argument("--input", default="data_generation/output/cdc_events.jsonl")
    parser.add_argument("--output-dir", default="ingestion/output")
    parser.add_argument("--dsn", default=os.getenv("WAREHOUSE_DSN"))
    args = parser.parse_args()
    landed, rejected, summaries = ingest_file(Path(args.input), output_dir=Path(args.output_dir), dsn=args.dsn)
    print(
        " ".join(
            [
                f"landed={len(landed)}",
                f"rejected={len(rejected)}",
                f"batches={len(summaries)}",
                f"output_dir={args.output_dir}",
            ]
        )
    )


if __name__ == "__main__":
    main()
