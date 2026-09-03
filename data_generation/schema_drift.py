from __future__ import annotations

import argparse
import csv
import json
from copy import deepcopy
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from data_generation.cdc_generator import build_cdc_events
from ingestion.cdc_envelope import normalize_event


@dataclass(frozen=True)
class DriftScenarioResult:
    scenario_name: str
    source_table: str
    expected_status: str
    actual_status: str
    passed_expectation: bool
    drift_type: str
    details: str


def _base_event(source_table: str) -> dict[str, Any]:
    for event in build_cdc_events(seed=42):
        if event.source_table == source_table and event.operation_type in {"insert", "update"}:
            return deepcopy(event.__dict__)
    raise ValueError(f"no base event found for {source_table}")


def build_drift_scenarios() -> list[tuple[str, str, str, str, dict[str, Any]]]:
    customer = _base_event("customers")
    additive = deepcopy(customer)
    additive["payload_after"]["loyalty_tier"] = "gold"

    removed_required = deepcopy(customer)
    removed_required["payload_after"].pop("customer_id", None)

    renamed_field = deepcopy(customer)
    renamed_field["payload_after"]["account_email"] = renamed_field["payload_after"].pop("email", None)

    subscription = _base_event("subscriptions")
    invalid_enum = deepcopy(subscription)
    invalid_enum["payload_after"]["subscription_status"] = "paused_forever"

    engagement = _base_event("engagement_events")
    type_change = deepcopy(engagement)
    type_change["payload_after"]["event_count"] = "five"

    return [
        ("additive_optional_field", "customers", "accepted", "backward_compatible_additive_field", additive),
        ("removed_required_field", "customers", "rejected", "breaking_removed_required_field", removed_required),
        ("renamed_nullable_field", "customers", "accepted", "nullable_field_rename_tolerated_but_lineage_degraded", renamed_field),
        ("invalid_enum_value", "subscriptions", "rejected", "invalid_domain_value", invalid_enum),
        ("numeric_type_changed_to_string", "engagement_events", "rejected", "breaking_type_change", type_change),
    ]


def run_schema_drift_suite() -> tuple[list[DriftScenarioResult], list[dict[str, Any]]]:
    results: list[DriftScenarioResult] = []
    scenario_events: list[dict[str, Any]] = []
    for name, source_table, expected_status, drift_type, raw in build_drift_scenarios():
        scenario_events.append(raw)
        try:
            normalize_event(raw)
            actual_status = "accepted"
            details = "contract accepted event"
        except Exception as exc:
            actual_status = "rejected"
            details = str(exc)
        results.append(
            DriftScenarioResult(
                scenario_name=name,
                source_table=source_table,
                expected_status=expected_status,
                actual_status=actual_status,
                passed_expectation=actual_status == expected_status,
                drift_type=drift_type,
                details=details,
            )
        )
    return results, scenario_events


def write_schema_drift_outputs(output_dir: Path) -> list[DriftScenarioResult]:
    output_dir.mkdir(parents=True, exist_ok=True)
    results, scenario_events = run_schema_drift_suite()
    result_rows = [asdict(row) for row in results]
    with (output_dir / "schema_drift_results.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(result_rows[0].keys()))
        writer.writeheader()
        writer.writerows(result_rows)
    with (output_dir / "schema_drift_events.jsonl").open("w", encoding="utf-8") as fh:
        for row in scenario_events:
            fh.write(json.dumps(row, sort_keys=True, default=str) + "\n")
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Run CDC schema drift contract scenarios.")
    parser.add_argument("--output-dir", default="data_generation/schema_drift_output")
    args = parser.parse_args()
    results = write_schema_drift_outputs(Path(args.output_dir))
    failed = [row.scenario_name for row in results if not row.passed_expectation]
    print(f"schema_drift_scenarios={len(results)} failed_expectations={failed} output_dir={args.output_dir}")


if __name__ == "__main__":
    main()

