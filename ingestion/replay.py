from __future__ import annotations

import argparse
import csv
import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ingestion.cdc_envelope import load_jsonl
from ingestion.loader import ingest_file, pg_connection


@dataclass(frozen=True)
class ReplayRunSummary:
    replay_run_id: str
    source_path: str
    output_path: str
    selected_count: int
    start_timestamp: str | None
    end_timestamp: str | None
    batch_ids: str
    source_tables: str
    reset_checkpoints: bool
    replayed_at: str
    status: str


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _event_ts(raw: dict[str, Any]) -> datetime:
    return datetime.fromisoformat(str(raw["event_timestamp"]).replace("Z", "+00:00")).astimezone(timezone.utc)


def select_replay_records(
    records: list[dict[str, Any]],
    *,
    start_timestamp: str | None = None,
    end_timestamp: str | None = None,
    batch_ids: set[str] | None = None,
    source_tables: set[str] | None = None,
) -> list[dict[str, Any]]:
    start = _parse_ts(start_timestamp)
    end = _parse_ts(end_timestamp)
    selected: list[dict[str, Any]] = []
    for raw in records:
        ts = _event_ts(raw)
        if start and ts < start:
            continue
        if end and ts >= end:
            continue
        if batch_ids and str(raw.get("batch_id")) not in batch_ids:
            continue
        if source_tables and str(raw.get("source_table")) not in source_tables:
            continue
        selected.append(raw)
    return sorted(selected, key=lambda row: (row["event_timestamp"], row["event_id"]))


def write_replay_records(records: list[dict[str, Any]], output_path: Path, *, replay_run_id: str) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        for row in records:
            replay_row = {
                **row,
                "is_replay": True,
                "replay_batch_id": replay_run_id,
            }
            fh.write(json.dumps(replay_row, sort_keys=True, default=str) + "\n")


def write_replay_manifest(summary: ReplayRunSummary, output_path: Path) -> None:
    manifest_path = output_path.with_suffix(".manifest.csv")
    with manifest_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(asdict(summary).keys()))
        writer.writeheader()
        writer.writerow(asdict(summary))


def log_replay_run(*, dsn: str, summary: ReplayRunSummary) -> None:
    with pg_connection(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into audit.replay_run_log (
                    replay_run_id, source_path, output_path, selected_count,
                    start_timestamp, end_timestamp, batch_ids, source_tables,
                    reset_checkpoints, replayed_at, status
                )
                values (
                    %(replay_run_id)s, %(source_path)s, %(output_path)s, %(selected_count)s,
                    %(start_timestamp)s, %(end_timestamp)s, %(batch_ids)s, %(source_tables)s,
                    %(reset_checkpoints)s, %(replayed_at)s, %(status)s
                )
                on conflict (replay_run_id) do nothing
                """,
                asdict(summary),
            )
        conn.commit()


def reset_watermark_checkpoints(
    *,
    dsn: str,
    source_tables: set[str] | None,
    replay_run_id: str,
    reset_before: str | None,
) -> None:
    with pg_connection(dsn) as conn:
        with conn.cursor() as cur:
            if source_tables:
                cur.execute(
                    """
                    delete from audit.watermark_checkpoint
                    where source_table = any(%(source_tables)s)
                    """,
                    {"source_tables": list(source_tables)},
                )
            else:
                cur.execute("delete from audit.watermark_checkpoint")
            cur.execute(
                """
                insert into observability.pipeline_alert (
                    alert_name, severity, alert_status, entity_name, alert_payload
                )
                values (
                    'checkpoint_reset_for_replay', 'info', 'resolved', 'audit.watermark_checkpoint',
                    %(payload)s::jsonb
                )
                """,
                {
                    "payload": json.dumps(
                        {
                            "replay_run_id": replay_run_id,
                            "source_tables": sorted(source_tables or []),
                            "reset_before": reset_before,
                        },
                        sort_keys=True,
                    )
                },
            )
        conn.commit()


def run_replay(
    *,
    input_path: Path,
    output_path: Path,
    start_timestamp: str | None = None,
    end_timestamp: str | None = None,
    batch_ids: set[str] | None = None,
    source_tables: set[str] | None = None,
    dsn: str | None = None,
    reset_checkpoints: bool = False,
    dry_run: bool = False,
) -> ReplayRunSummary:
    records = load_jsonl(str(input_path))
    selected = select_replay_records(
        records,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        batch_ids=batch_ids,
        source_tables=source_tables,
    )
    replay_run_id = f"replay_{uuid.uuid4().hex[:12]}"
    if not dry_run:
        write_replay_records(selected, output_path, replay_run_id=replay_run_id)
        if dsn and reset_checkpoints:
            reset_watermark_checkpoints(
                dsn=dsn,
                source_tables=source_tables,
                replay_run_id=replay_run_id,
                reset_before=start_timestamp,
            )
        if dsn:
            ingest_file(output_path, output_dir=output_path.parent / "audit", dsn=dsn)

    summary = ReplayRunSummary(
        replay_run_id=replay_run_id,
        source_path=str(input_path),
        output_path=str(output_path),
        selected_count=len(selected),
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        batch_ids=",".join(sorted(batch_ids or [])),
        source_tables=",".join(sorted(source_tables or [])),
        reset_checkpoints=reset_checkpoints,
        replayed_at=datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z"),
        status="dry_run" if dry_run else "success",
    )
    if not dry_run:
        write_replay_manifest(summary, output_path)
        if dsn:
            log_replay_run(dsn=dsn, summary=summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay/backfill selected CDC events by date range, batch, or table.")
    parser.add_argument("--input", default="data_generation/output/cdc_events.jsonl")
    parser.add_argument("--output", default="ingestion/replay_output/replay_events.jsonl")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--batch-id", action="append", dest="batch_ids")
    parser.add_argument("--source-table", action="append", dest="source_tables")
    parser.add_argument("--dsn")
    parser.add_argument("--reset-checkpoints", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    summary = run_replay(
        input_path=Path(args.input),
        output_path=Path(args.output),
        start_timestamp=args.start,
        end_timestamp=args.end,
        batch_ids=set(args.batch_ids or []),
        source_tables=set(args.source_tables or []),
        dsn=args.dsn,
        reset_checkpoints=args.reset_checkpoints,
        dry_run=args.dry_run,
    )
    print(
        f"replay_run_id={summary.replay_run_id} selected={summary.selected_count} "
        f"status={summary.status} output={summary.output_path}"
    )


if __name__ == "__main__":
    main()
