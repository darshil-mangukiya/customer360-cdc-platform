from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def render_catalog(catalog: dict[str, Any]) -> str:
    lines = [
        "# Generated Data Product Catalog",
        "",
        "| Product | Domain | Owner | Tier | Grain | SLA | Freshness | Contract | PII | Consumers |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for product in catalog.get("products", []):
        rendered = {**product, "consumers": ", ".join(product.get("consumers", []))}
        lines.append(
            "| `{name}` | {domain} | {owner} | {tier} | {grain} | {sla} | {freshness_minutes} min | "
            "{contract_status} | {pii_classification} | {consumers} |".format(**rendered)
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a markdown data product catalog.")
    parser.add_argument("--catalog", default="catalog/data_products.json")
    parser.add_argument("--output", default="reports/generated_data_product_catalog.md")
    args = parser.parse_args()
    catalog = json.loads(Path(args.catalog).read_text(encoding="utf-8"))
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_catalog(catalog), encoding="utf-8")
    print(f"products={len(catalog.get('products', []))} output={args.output}")


if __name__ == "__main__":
    main()
