from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any


def _load_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def render_scale_report(rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Scale Report",
        "",
        "This report summarizes local benchmark throughput and the production scaling direction.",
        "",
        "| Stage | Rows | Seconds | Rows/Second |",
        "| --- | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(
            f"| `{row.get('stage_name')}` | {row.get('row_count')} | {row.get('elapsed_seconds')} | {row.get('rows_per_second')} |"
        )
    lines.extend(
        [
            "",
            "## Production Levers",
            "",
            "- Partition raw CDC by `source_table` and event date.",
            "- Keep activation marts narrow and incrementally refreshed.",
            "- Use late-arriving windows for source tables with delayed business events.",
            "- Move long-term CDC replay storage to object storage after the hot warehouse window.",
            "- Track rejected event rate, connector lag, destination rate limits, and identity merge volume as cost drivers.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a markdown scale report from benchmark output.")
    parser.add_argument("--input", default="benchmark/output/benchmark_summary.csv")
    parser.add_argument("--output", default="benchmark/output/scale_report.md")
    args = parser.parse_args()
    rows = _load_csv(Path(args.input))
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(render_scale_report(rows), encoding="utf-8")
    print(f"benchmark_rows={len(rows)} output={args.output}")


if __name__ == "__main__":
    main()
