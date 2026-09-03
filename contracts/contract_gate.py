from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parent / "cdc_payload_contracts.json"


@dataclass(frozen=True)
class ContractCompatibilityFinding:
    source_table: str
    change_type: str
    compatibility: str
    field_name: str
    previous_value: str
    candidate_value: str
    decision: str


@dataclass(frozen=True)
class ContractCompatibilityReport:
    entity_type: str
    old_version: str
    new_version: str
    status: str
    breaking_changes: list[str]
    warnings: list[str]
    checked_at: str


def load_contract(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as fh:
        return json.load(fh)


def _stringify(value: Any) -> str:
    return json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else str(value)


def compare_contracts(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
) -> list[ContractCompatibilityFinding]:
    findings: list[ContractCompatibilityFinding] = []
    all_tables = sorted(set(baseline) | set(candidate))
    for table in all_tables:
        previous = baseline.get(table, {})
        current = candidate.get(table, {})
        if table not in baseline:
            findings.append(
                ContractCompatibilityFinding(
                    table,
                    "new_source_table",
                    "compatible",
                    "*",
                    "missing",
                    "present",
                    "new table is additive when no downstream contract exists",
                )
            )
            continue
        if table not in candidate:
            findings.append(
                ContractCompatibilityFinding(
                    table,
                    "removed_source_table",
                    "breaking",
                    "*",
                    "present",
                    "missing",
                    "source table removal breaks raw CDC consumers",
                )
            )
            continue

        previous_required = set(previous.get("required", []))
        current_required = set(current.get("required", []))
        previous_nullable = set(previous.get("nullable", []))
        current_nullable = set(current.get("nullable", []))
        previous_types = previous.get("types", {})
        current_types = current.get("types", {})
        previous_enums = previous.get("enums", {})
        current_enums = current.get("enums", {})

        for field in sorted(previous_required - current_required):
            findings.append(
                ContractCompatibilityFinding(
                    table,
                    "removed_required_field",
                    "breaking",
                    field,
                    "required",
                    "missing_or_optional",
                    "required field removals make existing consumers unsafe",
                )
            )
        for field in sorted(current_required - previous_required):
            findings.append(
                ContractCompatibilityFinding(
                    table,
                    "added_required_field",
                    "breaking",
                    field,
                    "missing",
                    "required",
                    "new required fields can reject older producers",
                )
            )
        for field in sorted(current_nullable - previous_nullable - previous_required):
            findings.append(
                ContractCompatibilityFinding(
                    table,
                    "added_nullable_field",
                    "compatible",
                    field,
                    "missing",
                    "nullable",
                    "nullable additions are backward compatible",
                )
            )

        for field, previous_type in sorted(previous_types.items()):
            candidate_type = current_types.get(field)
            if candidate_type is None:
                findings.append(
                    ContractCompatibilityFinding(
                        table,
                        "removed_typed_field",
                        "breaking" if field in previous_required else "review",
                        field,
                        previous_type,
                        "missing",
                        "typed field removal can degrade lineage or break consumers",
                    )
                )
            elif candidate_type != previous_type:
                findings.append(
                    ContractCompatibilityFinding(
                        table,
                        "type_changed",
                        "breaking",
                        field,
                        previous_type,
                        candidate_type,
                        "type changes must use a new versioned field",
                    )
                )
        for field in sorted(set(current_types) - set(previous_types)):
            compatibility = "breaking" if field in current_required else "compatible"
            findings.append(
                ContractCompatibilityFinding(
                    table,
                    "added_typed_field",
                    compatibility,
                    field,
                    "missing",
                    current_types[field],
                    "optional typed additions are allowed; required additions are not",
                )
            )

        for field, previous_values in sorted(previous_enums.items()):
            candidate_values = current_enums.get(field, [])
            removed = sorted(set(previous_values) - set(candidate_values))
            added = sorted(set(candidate_values) - set(previous_values))
            if removed:
                findings.append(
                    ContractCompatibilityFinding(
                        table,
                        "removed_enum_value",
                        "breaking",
                        field,
                        _stringify(removed),
                        _stringify(candidate_values),
                        "enum removals can invalidate historical CDC values",
                    )
                )
            if added:
                findings.append(
                    ContractCompatibilityFinding(
                        table,
                        "added_enum_value",
                        "review",
                        field,
                        _stringify(previous_values),
                        _stringify(candidate_values),
                        "new enum values require downstream model and activation review",
                    )
                )
    if not findings:
        findings.append(
            ContractCompatibilityFinding(
                "*",
                "no_contract_changes",
                "compatible",
                "*",
                "same",
                "same",
                "candidate contract matches baseline",
            )
        )
    return findings


def summarize_compatibility(
    findings: list[ContractCompatibilityFinding], *, old_version: str, new_version: str
) -> list[ContractCompatibilityReport]:
    checked_at = datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")
    tables = sorted({row.source_table for row in findings})
    reports: list[ContractCompatibilityReport] = []
    for table in tables:
        table_findings = [row for row in findings if row.source_table == table]
        breaking = [f"{row.change_type}:{row.field_name}" for row in table_findings if row.compatibility == "breaking"]
        warnings = [f"{row.change_type}:{row.field_name}" for row in table_findings if row.compatibility == "review"]
        reports.append(
            ContractCompatibilityReport(
                entity_type=table,
                old_version=old_version,
                new_version=new_version,
                status="breaking" if breaking else "compatible_with_warnings" if warnings else "compatible",
                breaking_changes=breaking,
                warnings=warnings,
                checked_at=checked_at,
            )
        )
    return reports


def write_summary(path: Path, reports: list[ContractCompatibilityReport]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps([asdict(row) for row in reports], indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_findings(path: Path, findings: list[ContractCompatibilityFinding]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(asdict(findings[0]).keys()))
        writer.writeheader()
        writer.writerows(asdict(finding) for finding in findings)


def main() -> None:
    parser = argparse.ArgumentParser(description="Check CDC contract compatibility.")
    parser.add_argument("--baseline", default=str(DEFAULT_CONTRACT_PATH))
    parser.add_argument("--candidate", default=str(DEFAULT_CONTRACT_PATH))
    parser.add_argument("--output", default="contracts/output/contract_compatibility_report.csv")
    parser.add_argument("--old-version", default="current")
    parser.add_argument("--new-version", default="candidate")
    parser.add_argument("--fail-on-breaking", action="store_true")
    args = parser.parse_args()

    findings = compare_contracts(load_contract(Path(args.baseline)), load_contract(Path(args.candidate)))
    write_findings(Path(args.output), findings)
    write_summary(
        Path(args.output).with_suffix(".summary.json"),
        summarize_compatibility(findings, old_version=args.old_version, new_version=args.new_version),
    )
    breaking = [finding for finding in findings if finding.compatibility == "breaking"]
    review = [finding for finding in findings if finding.compatibility == "review"]
    print(
        f"contract_findings={len(findings)} breaking={len(breaking)} "
        f"review={len(review)} output={args.output}"
    )
    if args.fail_on_breaking and breaking:
        sys.exit(1)


if __name__ == "__main__":
    main()
