"""One offline-safe platform validation command with JSON and Markdown outputs."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]


def _dbt_executable() -> str:
    explicit = os.getenv("DBT_EXECUTABLE")
    if explicit:
        return explicit
    discovered = shutil.which("dbt")
    if discovered:
        return discovered
    local = ROOT / ".venv/bin/dbt"
    if local.is_file():
        return str(local)
    raise RuntimeError("dbt executable not found; set DBT_EXECUTABLE or install dbt")


def _dbt_command() -> list[str]:
    explicit = os.getenv("DBT_EXECUTABLE")
    if explicit:
        return [explicit]
    # Prefer the repository environment over a different system dbt installation.
    # Its console-script shebang can become stale after a directory is moved, while
    # invoking the installed module through the environment's Python remains safe.
    local_python = ROOT / ".venv/bin/python"
    if local_python.is_file():
        return [str(local_python), "-m", "dbt.cli.main"]
    return [_dbt_executable()]


def _run(name: str, command: list[str], env: dict[str, str] | None = None) -> dict[str, Any]:
    completed = subprocess.run(command, cwd=ROOT, env=env, text=True, capture_output=True, check=False)
    return {"check": name, "status": "PASS" if completed.returncode == 0 else "FAIL", "command": " ".join(command), "return_code": completed.returncode, "output_tail": (completed.stdout + completed.stderr)[-2000:]}


def validate() -> dict[str, Any]:
    python = sys.executable
    dbt = _dbt_command()
    checks = [
        _run("pytest", [python, "-m", "pytest", "-q"]),
        _run("python_compile", [python, "-m", "compileall", "-q", "api", "customer360", "identity_resolution", "ingestion", "migration", "privacy", "reverse_etl", "streaming", "validation"]),
        _run("docs", ["make", "docs-check"]),
        _run("sql_lint", ["make", "sql-lint"]),
        _run("dbt_postgres_parse", [*dbt, "parse", "--project-dir", "dbt", "--profiles-dir", "dbt", "--target", "dev"]),
    ]
    sf_env = os.environ.copy()
    live = bool(sf_env.get("SNOWFLAKE_ACCOUNT") and sf_env.get("SNOWFLAKE_USER") and sf_env.get("SNOWFLAKE_PASSWORD"))
    sf_env.update({
        "SNOWFLAKE_ACCOUNT": sf_env.get("SNOWFLAKE_ACCOUNT", "offline.invalid"),
        "SNOWFLAKE_USER": sf_env.get("SNOWFLAKE_USER", "offline"),
        "SNOWFLAKE_PASSWORD": sf_env.get("SNOWFLAKE_PASSWORD", "offline"),
    })
    checks.append(_run("dbt_snowflake_parse", [*dbt, "parse", "--project-dir", "dbt", "--profiles-dir", "dbt", "--target", "snowflake"], sf_env))
    checks.append({"check": "snowflake_live", "status": "NOT_RUN" if not live else "AVAILABLE_NOT_AUTORUN", "command": "credential gate", "return_code": None, "output_tail": "Live mutation is a separate protected/manual path."})
    return {"overall_status": "PASS" if all(row["status"] in {"PASS", "NOT_RUN", "AVAILABLE_NOT_AUTORUN"} for row in checks) else "FAIL", "checks": checks}


def main() -> None:
    result = validate()
    output = ROOT / "validation/output"
    output.mkdir(parents=True, exist_ok=True)
    (output / "platform_validation.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    lines = ["# Platform Validation", "", f"Overall: **{result['overall_status']}**", "", "| Check | Status |", "| --- | --- |"]
    lines.extend(f"| `{row['check']}` | **{row['status']}** |" for row in result["checks"])
    (output / "platform_validation.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"overall_status": result["overall_status"], "checks": {row["check"]: row["status"] for row in result["checks"]}}))
    raise SystemExit(0 if result["overall_status"] == "PASS" else 1)


if __name__ == "__main__":
    main()
