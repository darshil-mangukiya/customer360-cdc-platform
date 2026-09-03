from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _model_rows(manifest: dict[str, Any]) -> list[dict[str, str]]:
    rows = []
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        rows.append(
            {
                "name": node.get("name", ""),
                "schema": node.get("schema", ""),
                "materialized": node.get("config", {}).get("materialized", ""),
                "description": (node.get("description") or "").replace("\n", " "),
                "depends_on": ", ".join(dep.split(".")[-1] for dep in node.get("depends_on", {}).get("nodes", [])),
            }
        )
    return sorted(rows, key=lambda row: (row["schema"], row["name"]))


def render_docs_index(manifest_path: Path) -> str:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    lines = [
        "# dbt Docs Lineage Index",
        "",
        "Generated from `dbt/target/manifest.json`. Run `dbt docs generate --profiles-dir .` for the full interactive catalog.",
        "",
        "| Model | Schema | Materialized | Upstream Nodes | Description |",
        "| --- | --- | --- | --- | --- |",
    ]
    for row in _model_rows(manifest):
        lines.append(
            f"| `{row['name']}` | {row['schema']} | {row['materialized']} | {row['depends_on']} | {row['description']} |"
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a markdown dbt docs lineage index.")
    parser.add_argument("--manifest", default="dbt/target/manifest.json")
    parser.add_argument("--output", default="reports/dbt_docs_lineage_index.md")
    args = parser.parse_args()
    output = render_docs_index(Path(args.manifest))
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(output, encoding="utf-8")
    print(f"output={args.output}")


if __name__ == "__main__":
    main()
