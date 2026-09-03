from __future__ import annotations

import argparse
import csv
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from identity_resolution.resolver import load_events, resolve_identity
from ingestion.loader import pg_connection
from reverse_etl.exporter import build_activation_rows
from validation.quality_checks import run_quality_checks


@dataclass(frozen=True)
class FreshnessStatus:
    tenant_id: str
    entity_name: str
    max_event_timestamp: str
    observed_at: str
    lag_minutes: int
    status: str


@dataclass(frozen=True)
class PipelineRunLog:
    run_id: str
    tenant_id: str
    stage_name: str
    started_at: str
    ended_at: str
    row_count: int
    status: str
    details: str


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def build_freshness(events) -> list[FreshnessStatus]:
    now = datetime.now(tz=timezone.utc)
    max_by_table: dict[tuple[str, str], str] = {}
    for event in events:
        key = (event.tenant_id, event.source_table)
        max_by_table[key] = max(max_by_table.get(key, event.event_timestamp), event.event_timestamp)
    statuses = []
    for (tenant_id, table), ts in sorted(max_by_table.items()):
        lag = int((now - _parse(ts)).total_seconds() // 60)
        statuses.append(
            FreshnessStatus(
                tenant_id=tenant_id,
                entity_name=table,
                max_event_timestamp=ts,
                observed_at=now.isoformat().replace("+00:00", "Z"),
                lag_minutes=lag,
                status="fresh" if lag < 24 * 60 else "stale_for_demo_clock",
            )
        )
    return statuses


def build_pipeline_logs(events, validation_failures) -> list[PipelineRunLog]:
    now = datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")
    counts = Counter((event.tenant_id, event.source_table) for event in events)
    logs = [
        PipelineRunLog(
            run_id="local_smoke_run",
            tenant_id=tenant_id,
            stage_name=f"raw_cdc_{table}",
            started_at=now,
            ended_at=now,
            row_count=count,
            status="success",
            details=f"landed {count} CDC events for {table}",
        )
        for (tenant_id, table), count in sorted(counts.items())
    ]
    logs.append(
        PipelineRunLog(
            run_id="local_smoke_run",
            tenant_id="all",
            stage_name="quality_checks",
            started_at=now,
            ended_at=now,
            row_count=len(validation_failures),
            status="success" if not validation_failures else "warning",
            details=f"validation_failures={len(validation_failures)}",
        )
    )
    return logs


def build_observability_outputs(input_path: Path) -> tuple[list[FreshnessStatus], list[PipelineRunLog]]:
    events = load_events(input_path)
    customers, mappings, _ = resolve_identity(events)
    activation_rows = build_activation_rows(events, customers, mappings)
    failures, _ = run_quality_checks(events, customers, mappings, activation_rows)
    return build_freshness(events), build_pipeline_logs(events, failures)


def write_observability_outputs(input_path: Path, output_dir: Path) -> tuple[list[FreshnessStatus], list[PipelineRunLog]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    freshness_rows, pipeline_logs = build_observability_outputs(input_path)
    outputs = {
        "freshness_status.csv": [asdict(row) for row in freshness_rows],
        "pipeline_run_log.csv": [asdict(row) for row in pipeline_logs],
    }
    for filename, rows in outputs.items():
        with (output_dir / filename).open("w", newline="", encoding="utf-8") as fh:
            if not rows:
                continue
            writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    return freshness_rows, pipeline_logs


def load_observability_to_postgres(
    *,
    dsn: str,
    freshness_rows: list[FreshnessStatus],
    pipeline_logs: list[PipelineRunLog],
) -> None:
    with pg_connection(dsn) as conn:
        with conn.cursor() as cur:
            for row in freshness_rows:
                cur.execute(
                    """
                    insert into observability.freshness_status (
                        tenant_id, entity_name, max_event_timestamp, observed_at, lag_minutes, status
                    )
                    values (
                        %(tenant_id)s, %(entity_name)s, %(max_event_timestamp)s, %(observed_at)s,
                        %(lag_minutes)s, %(status)s
                    )
                    """,
                    asdict(row),
                )
            for row in pipeline_logs:
                cur.execute(
                    """
                    insert into observability.pipeline_run_log (
                        run_id, tenant_id, stage_name, started_at, ended_at, row_count, status, details
                    )
                    values (
                        %(run_id)s, %(tenant_id)s, %(stage_name)s, %(started_at)s, %(ended_at)s,
                        %(row_count)s, %(status)s, %(details)s
                    )
                    """,
                    asdict(row),
                )
        conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build observability summaries.")
    parser.add_argument("--input", default="ingestion/output/raw_cdc_events.jsonl")
    parser.add_argument("--output-dir", default="observability/output")
    parser.add_argument("--dsn", help="Optional Postgres DSN for loading observability tables.")
    args = parser.parse_args()
    freshness_rows, pipeline_logs = write_observability_outputs(Path(args.input), Path(args.output_dir))
    if args.dsn:
        load_observability_to_postgres(
            dsn=args.dsn,
            freshness_rows=freshness_rows,
            pipeline_logs=pipeline_logs,
        )
    print(
        f"observability_output_dir={args.output_dir} freshness_rows={len(freshness_rows)} "
        f"pipeline_logs={len(pipeline_logs)} loaded_to_postgres={bool(args.dsn)}"
    )


if __name__ == "__main__":
    main()
