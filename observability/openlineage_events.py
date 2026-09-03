from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


NAMESPACE = "customer-360-cdc-platform"


@dataclass(frozen=True)
class LineageEdge:
    run_id: str
    tenant_id: str
    source_type: str
    source_name: str
    target_type: str
    target_name: str
    transformation: str
    record_count: int
    started_at: str
    completed_at: str
    status: str


def build_lineage_edges(*, run_id: str, tenant_counts: dict[str, int], observed_at: str) -> list[LineageEdge]:
    """Build compact run-level lineage from source CDC through destination simulation."""
    chain = [
        ("operational_source", "multi_source_records", "cdc", "raw.raw_cdc_events", "contract_validate_and_land"),
        ("cdc", "raw.raw_cdc_events", "identity", "identity.dim_customer_canonical", "tenant_scoped_identity_resolution"),
        ("identity", "identity.dim_customer_canonical", "warehouse", "mart.mart_customer_360_current", "dbt_customer_360_modeling"),
        ("warehouse", "mart.mart_customer_360_current", "activation", "reverse_etl.exports", "privacy_gate_and_export"),
        ("activation", "reverse_etl.exports", "destination_simulator", "destination_sync_state", "idempotent_destination_sync"),
    ]
    return [
        LineageEdge(
            run_id=run_id,
            tenant_id=tenant_id,
            source_type=source_type,
            source_name=source_name,
            target_type=target_type,
            target_name=target_name,
            transformation=transformation,
            record_count=count,
            started_at=observed_at,
            completed_at=observed_at,
            status="success",
        )
        for tenant_id, count in sorted(tenant_counts.items())
        for source_type, source_name, target_type, target_name, transformation in chain
    ]


def write_lineage_edges(path: Path, edges: list[LineageEdge]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps([asdict(row) for row in edges], indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _now() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def openlineage_event(
    *,
    job_name: str,
    event_type: str,
    inputs: list[str],
    outputs: list[str],
) -> dict[str, Any]:
    return {
        "eventType": event_type,
        "eventTime": _now(),
        "run": {"runId": str(uuid.uuid4())},
        "job": {"namespace": NAMESPACE, "name": job_name},
        "inputs": [{"namespace": NAMESPACE, "name": name} for name in inputs],
        "outputs": [{"namespace": NAMESPACE, "name": name} for name in outputs],
        "producer": "customer-360-local-openlineage-generator",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
    }


def build_openlineage_events() -> list[dict[str, Any]]:
    return [
        openlineage_event(
            job_name="generate_cdc_events",
            event_type="COMPLETE",
            inputs=["source_system_simulators"],
            outputs=["data_generation/output/cdc_events.jsonl"],
        ),
        openlineage_event(
            job_name="land_raw_cdc",
            event_type="COMPLETE",
            inputs=["data_generation/output/cdc_events.jsonl"],
            outputs=["raw.raw_cdc_events", "raw.rejected_events", "audit.ingestion_log"],
        ),
        openlineage_event(
            job_name="resolve_identity",
            event_type="COMPLETE",
            inputs=["raw.raw_cdc_events"],
            outputs=["identity.dim_customer_canonical", "identity.customer_identity_map", "identity.identity_resolution_audit"],
        ),
        openlineage_event(
            job_name="dbt_customer_360_marts",
            event_type="COMPLETE",
            inputs=["raw.raw_cdc_events", "identity.customer_identity_map"],
            outputs=["mart.mart_customer_360_current", "mart.fct_subscription_history", "mart.mart_customer_health"],
        ),
        openlineage_event(
            job_name="reverse_etl_exports",
            event_type="COMPLETE",
            inputs=["mart.mart_customer_360_current", "mart.mart_customer_health"],
            outputs=["activation.export_churn_risk", "activation.export_campaign_target", "reverse_etl/exports"],
        ),
        openlineage_event(
            job_name="reverse_etl_destination_sync",
            event_type="COMPLETE",
            inputs=["reverse_etl/exports"],
            outputs=["activation.reverse_etl_sync_run_log", "activation.reverse_etl_sync_failed_row"],
        ),
    ]


def write_openlineage_events(output: Path) -> list[dict[str, Any]]:
    output.parent.mkdir(parents=True, exist_ok=True)
    events = build_openlineage_events()
    with output.open("w", encoding="utf-8") as fh:
        for event in events:
            fh.write(json.dumps(event, sort_keys=True) + "\n")
    return events


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate OpenLineage-style metadata events for the local pipeline.")
    parser.add_argument("--output", default="observability/openlineage/openlineage_events.jsonl")
    args = parser.parse_args()
    events = write_openlineage_events(Path(args.output))
    print(f"openlineage_events={len(events)} output={args.output}")


if __name__ == "__main__":
    main()
