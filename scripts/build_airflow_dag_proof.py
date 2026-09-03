from __future__ import annotations

import argparse
import importlib.util
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DAG_PATH = ROOT / "airflow/dags/customer_360_cdc_platform.py"


def _quoted_value(pattern: str, text: str) -> str:
    match = re.search(pattern, text)
    if not match:
        raise ValueError(f"could not parse pattern: {pattern}")
    return match.group(1)


def parse_dag_source(path: Path = DAG_PATH) -> dict[str, object]:
    text = path.read_text(encoding="utf-8")
    compile(text, str(path), "exec")
    groups: dict[str, list[str]] = {}
    current_group: str | None = None
    for line in text.splitlines():
        if line.startswith("    end = EmptyOperator"):
            current_group = None
            continue
        group_match = re.search(r'TaskGroup\(group_id="([^"]+)"\)', line)
        if group_match:
            current_group = group_match.group(1)
            groups[current_group] = []
            continue
        task_match = re.search(r'task_id="([^"]+)"', line)
        if task_match and current_group:
            groups[current_group].append(task_match.group(1))

    dependency_match = re.search(r"\(\s*start\s*>>(.*?)>>\s*end\s*\)", text, flags=re.DOTALL)
    if not dependency_match:
        raise ValueError("could not parse top-level DAG dependency chain")
    dependency_order = ["start"] + [part.strip() for part in dependency_match.group(1).split(">>")] + ["end"]
    return {
        "dag_id": _quoted_value(r'dag_id="([^"]+)"', text),
        "schedule": _quoted_value(r'schedule="([^"]+)"', text),
        "retries": _quoted_value(r'CUSTOMER360_TASK_RETRIES", "([^"]+)"', text),
        "retry_delay_seconds": _quoted_value(r'CUSTOMER360_RETRY_DELAY_SECONDS", "([^"]+)"', text),
        "task_groups": groups,
        "dependency_order": dependency_order,
    }


def attempt_airflow_import(path: Path = DAG_PATH) -> dict[str, Any]:
    spec = importlib.util.spec_from_file_location("customer_360_cdc_platform_proof", path)
    if spec is None or spec.loader is None:
        return {"status": "failed", "detail": "could not create import spec"}
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        return {"status": "skipped", "detail": f"{type(exc).__name__}: {exc}"}
    dag = getattr(module, "dag", None)
    task_ids = sorted(getattr(task, "task_id", str(task)) for task in getattr(dag, "tasks", []))
    return {
        "status": "passed",
        "dag_id": getattr(dag, "dag_id", None),
        "task_count": len(task_ids),
        "task_ids": task_ids,
    }


def build_proof_markdown() -> str:
    dag = parse_dag_source()
    import_result = attempt_airflow_import()
    task_group_rows = "\n".join(
        f"| `{group}` | {len(tasks)} | {', '.join(f'`{task}`' for task in tasks)} |"
        for group, tasks in dag["task_groups"].items()
    )
    dependency_order = " -> ".join(f"`{item}`" for item in dag["dependency_order"])
    task_count = sum(len(tasks) for tasks in dag["task_groups"].values()) + 2
    return f"""# Sample Airflow DAG Proof

Local synthetic proof output.

Related command:

```bash
make airflow-proof
```

## Proof Type

Airflow DAG structure proof. The command validates that the DAG Python source compiles, extracts the DAG ID, task groups, task list, retry settings, and top-level dependency order, and attempts a direct Airflow DAG import when the Airflow package is available in the local environment.

This proof does not claim a successful scheduled DAG run.

## Import Validation

- Python source compile: `passed`
- Airflow DAG import status: `{import_result["status"]}`
- Import detail: `{import_result.get("detail", "dag imported and inspected")}`

## DAG Summary

- DAG file: `airflow/dags/customer_360_cdc_platform.py`
- DAG name: `{dag["dag_id"]}`
- Schedule: `{dag["schedule"]}`
- Default retries: `{dag["retries"]}`
- Retry delay seconds: `{dag["retry_delay_seconds"]}`
- Parsed task count including start/end: `{task_count}`

## Dependency Order

{dependency_order}

## Task Groups

| Task Group | Task Count | Tasks |
| --- | ---: | --- |
{task_group_rows}
"""


def write_proof(output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(build_proof_markdown(), encoding="utf-8")
    print(f"airflow_dag_proof={output}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Airflow DAG structure proof artifact.")
    parser.add_argument("--output", default="reports/sample_airflow_dag_proof.md")
    args = parser.parse_args()
    write_proof(ROOT / args.output)


if __name__ == "__main__":
    main()
