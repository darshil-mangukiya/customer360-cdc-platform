from __future__ import annotations

import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_generation.cdc_generator import build_cdc_events, write_jsonl
from identity_resolution.resolver import build_identity_graph_artifacts, resolve_identity, write_identity_outputs
from identity_resolution.stewardship import detect_review_candidates, write_stewardship_outputs
from ingestion.loader import ingest_file
from observability.metrics import write_observability_outputs
from observability.openlineage_events import build_lineage_edges, write_lineage_edges
from privacy.activation_policy import build_suppressed_customers, write_privacy_activation_outputs
from reverse_etl.destinations.simulator import simulate_destination_sync
from reverse_etl.exporter import build_activation_rows, write_exports
from reverse_etl.reconciliation import reconcile, write_reconciliation_outputs
from validation.quality_checks import run_quality_checks, write_quality_outputs
from validation.scorecard import write_local_scorecard


def main() -> None:
    root = ROOT
    generated_path = root / "data_generation/output/cdc_events.jsonl"
    ingestion_dir = root / "ingestion/output"
    identity_dir = root / "identity_resolution/output"
    reverse_dir = root / "reverse_etl/exports"
    sync_dir = root / "reverse_etl/sync_logs"
    reconciliation_dir = root / "reverse_etl/reconciliation"
    privacy_dir = root / "privacy/output"
    validation_dir = root / "validation/output"
    observability_dir = root / "observability/output"

    events = build_cdc_events(seed=42)
    write_jsonl(events, generated_path)
    landed, rejected, summaries = ingest_file(generated_path, output_dir=ingestion_dir, dsn=None)
    canonical, mappings, audit = resolve_identity(landed)
    graph_artifacts = build_identity_graph_artifacts(landed, canonical, mappings)
    write_identity_outputs(
        canonical=canonical,
        mappings=mappings,
        audit=audit,
        output_dir=identity_dir,
        graph_artifacts=graph_artifacts,
    )
    review_cases = detect_review_candidates(landed, canonical, mappings)
    write_stewardship_outputs(review_cases=review_cases, output_dir=identity_dir)
    activation_rows = build_activation_rows(landed, canonical, mappings)
    write_exports(activation_rows, reverse_dir)
    consent_by_customer, suppressed = build_suppressed_customers(events=landed, canonical=canonical, mappings=mappings)
    write_privacy_activation_outputs(
        consent_by_customer=consent_by_customer,
        suppressed=suppressed,
        output_dir=privacy_dir,
    )
    sync_runs, sync_failed = simulate_destination_sync(export_dir=reverse_dir, output_dir=sync_dir)
    reconciliations, reconciliation_findings = reconcile(
        identity_dir=identity_dir, privacy_dir=privacy_dir, export_dir=reverse_dir, sync_log_dir=sync_dir
    )
    write_reconciliation_outputs(
        reconciliations=reconciliations, findings=reconciliation_findings, output_dir=reconciliation_dir
    )
    failures, quality_summary = run_quality_checks(landed, canonical, mappings, activation_rows)
    write_quality_outputs(failures, quality_summary, validation_dir)
    write_observability_outputs(ingestion_dir / "raw_cdc_events.jsonl", observability_dir)
    observed_at = datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")
    tenant_counts = Counter(event.tenant_id for event in landed)
    write_lineage_edges(
        observability_dir / "lineage_edges.json",
        build_lineage_edges(run_id="local_smoke_run", tenant_counts=dict(tenant_counts), observed_at=observed_at),
    )
    _, quality_score = write_local_scorecard(root, validation_dir / "quality_scorecard.json")

    failed_checks = [row.check_name for row in quality_summary if row.status == "fail"]
    print(
        " ".join(
            [
                f"generated={len(events)}",
                f"landed={len(landed)}",
                f"rejected={len(rejected)}",
                f"batches={len(summaries)}",
                f"canonical_customers={len(canonical)}",
                f"identity_mappings={len(mappings)}",
                f"identity_graph_nodes={len(graph_artifacts.nodes)}",
                f"identity_review_cases={len(review_cases)}",
                f"activation_rows={len(activation_rows)}",
                f"suppressed_customers={len(suppressed)}",
                f"sync_runs={len(sync_runs)}",
                f"sync_failed_rows={len(sync_failed)}",
                f"reconciliation_runs={len(reconciliations)}",
                f"reconciliation_findings={len(reconciliation_findings)}",
                f"quality_failures={len(failures)}",
                f"failed_checks={failed_checks}",
                f"quality_score_pct={quality_score}",
            ]
        )
    )


if __name__ == "__main__":
    main()
