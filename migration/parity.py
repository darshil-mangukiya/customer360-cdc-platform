"""Deterministic, value-level parity checks without pretending a target was run."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class ParityResult:
    dataset: str
    status: str
    source_rows: int
    target_rows: int | None
    missing_keys: tuple[str, ...]
    extra_keys: tuple[str, ...]
    mismatched_values: tuple[str, ...]
    limitation: str | None = None


def _canonical(value: Any) -> Any:
    if isinstance(value, float):
        return str(Decimal(str(value)).quantize(Decimal("0.000001")))
    if isinstance(value, dict):
        return {key: _canonical(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_canonical(item) for item in value]
    return value


def compare_rows(
    dataset: str,
    source_rows: Iterable[dict[str, Any]],
    target_rows: Iterable[dict[str, Any]] | None,
    *,
    key_fields: tuple[str, ...],
) -> ParityResult:
    source = list(source_rows)
    if target_rows is None:
        return ParityResult(dataset, "NOT_RUN", len(source), None, (), (), (), "Snowflake result was not supplied")
    target = list(target_rows)
    def key(row: dict[str, Any]) -> str:
        return "|".join(str(row.get(field)) for field in key_fields)
    source_map, target_map = {key(row): _canonical(row) for row in source}, {key(row): _canonical(row) for row in target}
    missing = tuple(sorted(source_map.keys() - target_map.keys()))
    extra = tuple(sorted(target_map.keys() - source_map.keys()))
    mismatched = tuple(sorted(item for item in source_map.keys() & target_map.keys() if source_map[item] != target_map[item]))
    status = "PASS" if not (missing or extra or mismatched) else "FAIL"
    return ParityResult(dataset, status, len(source), len(target), missing, extra, mismatched)


def render_markdown(results: Iterable[ParityResult]) -> str:
    rows = list(results)
    lines = ["# Migration Parity Report", "", "| Dataset | Status | PostgreSQL rows | Snowflake rows | Missing | Extra | Mismatched |", "| --- | --- | ---: | ---: | ---: | ---: | ---: |"]
    for row in rows:
        lines.append(f"| `{row.dataset}` | **{row.status}** | {row.source_rows} | {row.target_rows if row.target_rows is not None else 'NOT RUN'} | {len(row.missing_keys)} | {len(row.extra_keys)} | {len(row.mismatched_values)} |")
    lines.extend(["", "## Limitations", ""])
    limitations = [row.limitation for row in rows if row.limitation]
    lines.extend(f"- {item}" for item in limitations or ["None; both sides were supplied to this validator."])
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--target", type=Path)
    parser.add_argument("--dataset", default="customer_360")
    parser.add_argument("--keys", default="tenant_id,canonical_customer_id")
    parser.add_argument("--json-output", type=Path, default=Path("migration/output/parity_result.json"))
    parser.add_argument("--markdown-output", type=Path, default=Path("migration/MIGRATION_PARITY_REPORT.md"))
    args = parser.parse_args()
    source = json.loads(args.source.read_text(encoding="utf-8"))
    target = json.loads(args.target.read_text(encoding="utf-8")) if args.target else None
    result = compare_rows(args.dataset, source, target, key_fields=tuple(args.keys.split(",")))
    args.json_output.parent.mkdir(parents=True, exist_ok=True)
    args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
    args.json_output.write_text(json.dumps(asdict(result), indent=2) + "\n", encoding="utf-8")
    args.markdown_output.write_text(render_markdown([result]), encoding="utf-8")
    print(f"migration_parity={result.status}")


if __name__ == "__main__":
    main()
