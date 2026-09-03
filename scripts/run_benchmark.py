from __future__ import annotations

import argparse
import csv
import sys
import time
import uuid
import platform
from copy import deepcopy
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_generation.cdc_generator import build_cdc_events
from identity_resolution.resolver import resolve_identity
from identity_resolution.stewardship import detect_review_candidates
from ingestion.loader import normalize_records
from reverse_etl.exporter import build_activation_rows
from validation.quality_checks import run_quality_checks


LOAD_PROFILES = {
    "smoke": 1,
    "small": 50,
    "medium": 500,
    "large": 1000,
}


@dataclass(frozen=True)
class BenchmarkMetric:
    benchmark_run_id: str
    copies: int
    input_events: int
    stage_name: str
    row_count: int
    elapsed_seconds: float
    rows_per_second: float
    environment_notes: str


def _rewrite_payload_identifiers(payload: dict[str, Any] | None, suffix: str) -> dict[str, Any] | None:
    if payload is None:
        return None
    rewritten = deepcopy(payload)
    for field, value in list(rewritten.items()):
        if not isinstance(value, str):
            continue
        if value.startswith(("cust_", "sub_", "ord_", "eng_", "case_", "mkt_", "acct_ext_", "dev_")):
            rewritten[field] = f"{value}_{suffix}"
        elif value.startswith("+"):
            rewritten[field] = f"{value}{suffix[-4:]}"
        elif "@example" in value:
            local, domain = value.split("@", 1)
            rewritten[field] = f"{local}.{suffix}@{domain}"
    return rewritten


def build_scaled_events(copies: int) -> list[dict[str, Any]]:
    base = [event.__dict__ for event in build_cdc_events(seed=42)]
    scaled: list[dict[str, Any]] = []
    for copy_idx in range(copies):
        suffix = f"bench{copy_idx:05d}"
        for raw in base:
            row = deepcopy(raw)
            row["event_id"] = f"evt_{uuid.uuid4().hex}"
            row["batch_id"] = f"{row['batch_id']}_{suffix}"
            row["record_primary_key"] = f"{row['record_primary_key']}_{suffix}"
            row["payload_before"] = _rewrite_payload_identifiers(row.get("payload_before"), suffix)
            row["payload_after"] = _rewrite_payload_identifiers(row.get("payload_after"), suffix)
            # Each scaled copy models a distinct source record and delivery. Retaining
            # the base fixture's hash/offset would correctly deduplicate nearly the
            # entire benchmark and therefore measure the wrong workload.
            row["event_hash"] = None
            if row.get("kafka_offset") is not None:
                row["kafka_offset"] = int(row["kafka_offset"]) + copy_idx * 100_000
            scaled.append(row)
    return scaled


def _metric(run_id: str, copies: int, input_events: int, stage_name: str, row_count: int, elapsed: float) -> BenchmarkMetric:
    return BenchmarkMetric(
        benchmark_run_id=run_id,
        copies=copies,
        input_events=input_events,
        stage_name=stage_name,
        row_count=row_count,
        elapsed_seconds=round(elapsed, 6),
        rows_per_second=round(row_count / elapsed, 2) if elapsed > 0 else 0.0,
        environment_notes=f"Python {platform.python_version()} on {platform.system()} {platform.machine()}; local single process",
    )


def run_benchmark(copies: int) -> list[BenchmarkMetric]:
    run_id = f"bench_{uuid.uuid4().hex[:10]}"
    metrics: list[BenchmarkMetric] = []

    start = time.perf_counter()
    raw_events = build_scaled_events(copies)
    metrics.append(_metric(run_id, copies, len(raw_events), "generate_scaled_events", len(raw_events), time.perf_counter() - start))

    start = time.perf_counter()
    landed, _rejected = normalize_records(raw_events)
    metrics.append(_metric(run_id, copies, len(raw_events), "normalize_cdc", len(landed), time.perf_counter() - start))

    start = time.perf_counter()
    canonical, mappings, audit = resolve_identity(landed)
    metrics.append(_metric(run_id, copies, len(raw_events), "resolve_identity", len(mappings), time.perf_counter() - start))

    start = time.perf_counter()
    review_cases = detect_review_candidates(landed, canonical, mappings)
    metrics.append(_metric(run_id, copies, len(raw_events), "detect_identity_reviews", len(review_cases), time.perf_counter() - start))

    start = time.perf_counter()
    activation_rows = build_activation_rows(landed, canonical, mappings)
    metrics.append(_metric(run_id, copies, len(raw_events), "build_activation_exports", len(activation_rows), time.perf_counter() - start))

    start = time.perf_counter()
    _failures, summary = run_quality_checks(landed, canonical, mappings, activation_rows)
    metrics.append(_metric(run_id, copies, len(raw_events), "run_quality_checks", len(summary), time.perf_counter() - start))
    return metrics


def write_benchmark(metrics: list[BenchmarkMetric], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    rows = [asdict(row) for row in metrics]
    with output.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run local CDC platform performance benchmark.")
    parser.add_argument("--profile", choices=sorted(LOAD_PROFILES), help="Named load profile from config/load_profiles.yml.")
    parser.add_argument("--copies", type=int, default=50, help="Number of copies of the 152-event synthetic history.")
    parser.add_argument("--output", default="benchmark/output/benchmark_summary.csv")
    args = parser.parse_args()
    copies = LOAD_PROFILES[args.profile] if args.profile else args.copies
    metrics = run_benchmark(copies)
    write_benchmark(metrics, Path(args.output))
    total_events = metrics[0].input_events if metrics else 0
    print(f"benchmark_events={total_events} stages={len(metrics)} output={args.output}")


if __name__ == "__main__":
    main()
