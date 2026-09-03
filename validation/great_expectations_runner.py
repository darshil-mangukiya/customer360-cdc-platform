from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SUITE = ROOT / "great_expectations/expectations/customer_360_activation_suite.json"


@dataclass(frozen=True)
class ExpectationResult:
    expectation_type: str
    table: str
    column: str
    success: bool
    observed_value: str
    details: str


def _load_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _evaluate_expectation(root: Path, expectation: dict[str, Any]) -> ExpectationResult:
    expectation_type = expectation["expectation_type"]
    kwargs = expectation.get("kwargs", {})
    table = kwargs["table"]
    column = kwargs.get("column", "")
    rows = _load_csv(root / table)

    if expectation_type == "expect_table_row_count_to_be_between":
        min_value = int(kwargs.get("min_value", 0))
        max_value = kwargs.get("max_value")
        observed = len(rows)
        success = observed >= min_value and (max_value is None or observed <= int(max_value))
        return ExpectationResult(expectation_type, table, column, success, str(observed), f"expected >= {min_value}")

    if expectation_type == "expect_column_values_to_not_be_null":
        null_count = sum(1 for row in rows if row.get(column) in {None, ""})
        return ExpectationResult(expectation_type, table, column, null_count == 0, str(null_count), "null_count")

    if expectation_type == "expect_column_values_to_be_in_set":
        allowed = set(kwargs["value_set"])
        bad = sorted({row.get(column, "") for row in rows if row.get(column, "") not in allowed})
        return ExpectationResult(expectation_type, table, column, not bad, ",".join(bad), f"allowed={sorted(allowed)}")

    if expectation_type == "expect_column_values_to_be_unique":
        values = [row.get(column, "") for row in rows if row.get(column, "")]
        duplicate_count = len(values) - len(set(values))
        return ExpectationResult(expectation_type, table, column, duplicate_count == 0, str(duplicate_count), "duplicate_count")

    return ExpectationResult(expectation_type, table, column, False, "unsupported", "unsupported expectation type")


def run_suite(suite_path: Path = DEFAULT_SUITE, root: Path = ROOT) -> list[ExpectationResult]:
    suite = json.loads(suite_path.read_text(encoding="utf-8"))
    return [_evaluate_expectation(root, expectation) for expectation in suite.get("expectations", [])]


def write_results(results: list[ExpectationResult], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(asdict(results[0]).keys()) if results else [])
        if results:
            writer.writeheader()
            writer.writerows(asdict(result) for result in results)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run lightweight Great Expectations-compatible validation suite.")
    parser.add_argument("--suite", default=str(DEFAULT_SUITE))
    parser.add_argument("--output", default="validation/output/great_expectations_results.csv")
    parser.add_argument("--fail-on-error", action="store_true")
    args = parser.parse_args()
    results = run_suite(Path(args.suite))
    write_results(results, Path(args.output))
    failed = [result.expectation_type for result in results if not result.success]
    print(f"expectations={len(results)} failed={failed} output={args.output}")
    if args.fail_on_error and failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
