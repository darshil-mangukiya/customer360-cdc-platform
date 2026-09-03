from __future__ import annotations

import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _ids(path: Path) -> set[str]:
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open(encoding="utf-8") as handle:
        return {
            row["canonical_customer_id"]
            for row in csv.DictReader(handle)
            if row.get("canonical_customer_id")
        }


def verify(root: Path = ROOT) -> tuple[int, int, int]:
    suppressed = _ids(root / "privacy/output/activation_suppression_list.csv")
    exported: set[str] = set()
    export_files = sorted((root / "reverse_etl/exports").glob("*_export.csv"))
    if not export_files:
        raise RuntimeError("no activation exports found")
    for path in export_files:
        exported.update(_ids(path))
    leaked = suppressed & exported
    if leaked:
        raise RuntimeError(f"privacy gate failed: {len(leaked)} suppressed customer(s) exported")
    return len(suppressed), len(exported), len(export_files)


def main() -> None:
    suppressed, exported, files = verify()
    print(
        f"privacy_gate=PASS suppressed_customers={suppressed} "
        f"exported_customers={exported} export_files={files} leaked_customers=0"
    )


if __name__ == "__main__":
    main()
