from __future__ import annotations

from pathlib import Path

import yaml

from scripts.verify_activation_privacy import verify


ROOT = Path(__file__).resolve().parents[1]
DAG_PATH = ROOT / "airflow/dags/customer_360_cdc_platform.py"


def test_airflow_dag_is_offline_safe_and_blocks_downstream_work() -> None:
    source = DAG_PATH.read_text(encoding="utf-8")
    compile(source, str(DAG_PATH), "exec")
    assert 'dag_id="customer_360_cdc_platform"' in source
    assert 'schedule="@hourly"' in source
    assert "catchup=False" in source
    assert "CUSTOMER360_CONTRACT_CANDIDATE" in source
    assert "publish_to_kafka" not in source
    assert "WAREHOUSE_DSN" not in source
    assert "SNOWFLAKE" not in source
    assert "contract_gate >> schema_drift >> land_raw_cdc" in source
    assert "build_exports >> verify_privacy_gate >> simulate_syncs >> reconcile_activation" in source
    assert "TriggerRule.ALL_DONE" not in source
    assert "shlex.quote(str(PROJECT_DIR))" in source
    assert "PYTHON_CMD = shlex.quote(PYTHON)" in source
    assert "CONTRACT_CANDIDATE_ARG = shlex.quote(CONTRACT_CANDIDATE)" in source


def test_airflow_dag_has_meaningful_retry_configuration() -> None:
    source = DAG_PATH.read_text(encoding="utf-8")
    assert '"retries": int(os.getenv("CUSTOMER360_TASK_RETRIES", "1"))' in source
    assert "retries=3" in source
    assert "retry_delay=timedelta(seconds=10)" in source


def test_privacy_gate_rejects_suppressed_export(tmp_path: Path) -> None:
    privacy = tmp_path / "privacy/output"
    exports = tmp_path / "reverse_etl/exports"
    privacy.mkdir(parents=True)
    exports.mkdir(parents=True)
    (privacy / "activation_suppression_list.csv").write_text(
        "canonical_customer_id\ncc_suppressed\n", encoding="utf-8"
    )
    (exports / "customer_segment_export.csv").write_text(
        "canonical_customer_id\ncc_suppressed\n", encoding="utf-8"
    )
    try:
        verify(tmp_path)
    except RuntimeError as exc:
        assert "privacy gate failed" in str(exc)
    else:
        raise AssertionError("suppressed customer was accepted by privacy gate")


def test_ci_and_snowflake_workflow_boundaries() -> None:
    loader = yaml.BaseLoader
    ci = yaml.load((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"), Loader=loader)
    snowflake = yaml.load(
        (ROOT / ".github/workflows/snowflake-validation.yml").read_text(encoding="utf-8"),
        Loader=loader,
    )
    assert set(ci["jobs"]) == {
        "python-security",
        "contracts-and-pipeline",
        "warehouse-sql-docs",
        "repository-hygiene",
    }
    assert set(ci["on"]) == {"push", "pull_request"}
    assert set(snowflake["on"]) == {"workflow_dispatch"}
    assert snowflake["jobs"]["validate"]["environment"] == "snowflake-test"
    ci_text = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert "dbt debug" not in ci_text
    assert "dbt run --project-dir dbt --profiles-dir dbt --target snowflake" not in ci_text
    assert "python -m pip_audit -r requirements-dev.txt" in ci_text
    assert "pip-audit==2.10.1" in ci_text
