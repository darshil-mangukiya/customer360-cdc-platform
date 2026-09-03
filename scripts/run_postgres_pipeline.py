from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_generation.cdc_generator import build_cdc_events, write_jsonl
from identity_resolution.resolver import load_identity_to_postgres, resolve_identity, write_identity_outputs
from ingestion.loader import ingest_file
from observability.metrics import load_observability_to_postgres, write_observability_outputs
from reverse_etl.destinations.simulator import load_sync_logs_to_postgres, simulate_destination_sync
from reverse_etl.exporter import build_activation_rows, load_activation_to_postgres, write_exports
from validation.quality_checks import load_quality_to_postgres, run_quality_checks, write_quality_outputs


def run_command(command: list[str], *, cwd: Path) -> None:
    subprocess.run(command, cwd=cwd, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the full Customer 360 pipeline against Postgres.")
    parser.add_argument(
        "--dsn",
        default=os.getenv("WAREHOUSE_DSN", "postgresql://c360:c360@localhost:5432/c360"),
        help="Postgres warehouse DSN.",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--skip-dbt", action="store_true", help="Skip dbt run/snapshot/test steps.")
    args = parser.parse_args()

    generated_path = ROOT / "data_generation/output/cdc_events.jsonl"
    ingestion_dir = ROOT / "ingestion/output"
    identity_dir = ROOT / "identity_resolution/output"
    reverse_dir = ROOT / "reverse_etl/exports"
    sync_dir = ROOT / "reverse_etl/sync_logs"
    validation_dir = ROOT / "validation/output"
    observability_dir = ROOT / "observability/output"

    events = build_cdc_events(seed=args.seed)
    write_jsonl(events, generated_path)
    landed, rejected, summaries = ingest_file(generated_path, output_dir=ingestion_dir, dsn=args.dsn)

    canonical, mappings, audit = resolve_identity(landed)
    write_identity_outputs(canonical=canonical, mappings=mappings, audit=audit, output_dir=identity_dir)
    load_identity_to_postgres(dsn=args.dsn, canonical=canonical, mappings=mappings, audit=audit)

    if not args.skip_dbt:
        run_command(["dbt", "run", "--profiles-dir", "."], cwd=ROOT / "dbt")
        run_command(["dbt", "snapshot", "--profiles-dir", "."], cwd=ROOT / "dbt")
        run_command(["dbt", "test", "--profiles-dir", "."], cwd=ROOT / "dbt")

    activation_rows = build_activation_rows(landed, canonical, mappings)
    write_exports(activation_rows, reverse_dir)
    load_activation_to_postgres(dsn=args.dsn, rows=activation_rows)
    sync_runs, sync_failed = simulate_destination_sync(export_dir=reverse_dir, output_dir=sync_dir)
    load_sync_logs_to_postgres(dsn=args.dsn, run_logs=sync_runs, failed_rows=sync_failed)

    failures, quality_summary = run_quality_checks(landed, canonical, mappings, activation_rows)
    write_quality_outputs(failures, quality_summary, validation_dir)
    load_quality_to_postgres(dsn=args.dsn, failures=failures, summary=quality_summary)

    freshness_rows, pipeline_logs = write_observability_outputs(ingestion_dir / "raw_cdc_events.jsonl", observability_dir)
    load_observability_to_postgres(
        dsn=args.dsn,
        freshness_rows=freshness_rows,
        pipeline_logs=pipeline_logs,
    )

    failed_checks = [row.check_name for row in quality_summary if row.status == "fail"]
    print(
        " ".join(
            [
                "postgres_pipeline=success",
                f"generated={len(events)}",
                f"landed={len(landed)}",
                f"rejected={len(rejected)}",
                f"batches={len(summaries)}",
                f"canonical_customers={len(canonical)}",
                f"identity_mappings={len(mappings)}",
                f"activation_rows={len(activation_rows)}",
                f"sync_runs={len(sync_runs)}",
                f"sync_failed_rows={len(sync_failed)}",
                f"quality_failures={len(failures)}",
                f"failed_checks={failed_checks}",
                f"dbt_ran={not args.skip_dbt}",
            ]
        )
    )


if __name__ == "__main__":
    main()
