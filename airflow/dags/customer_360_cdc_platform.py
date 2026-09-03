from __future__ import annotations

import os
import shlex
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup


PROJECT_DIR = Path(os.getenv("CUSTOMER360_PROJECT_DIR", "/opt/customer-360-cdc-platform"))
PYTHON = os.getenv("CUSTOMER360_PYTHON", "python3")
CONTRACT_CANDIDATE = os.getenv(
    "CUSTOMER360_CONTRACT_CANDIDATE", "contracts/cdc_payload_contracts.json"
)
PYTHON_CMD = shlex.quote(PYTHON)
CONTRACT_CANDIDATE_ARG = shlex.quote(CONTRACT_CANDIDATE)


def _notify_failure(context: dict[str, Any]) -> None:
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    task_id = getattr(task_instance, "task_id", "unknown_task")
    run_id = getattr(dag_run, "run_id", "unknown_run")
    print(f"ALERT customer_360_cdc_platform task_failed task_id={task_id} run_id={run_id}")


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": int(os.getenv("CUSTOMER360_TASK_RETRIES", "1")),
    "retry_delay": timedelta(seconds=int(os.getenv("CUSTOMER360_RETRY_DELAY_SECONDS", "30"))),
    "email_on_failure": False,
    "on_failure_callback": _notify_failure,
}


def project_cmd(command: str) -> str:
    return f"cd {shlex.quote(str(PROJECT_DIR))} && {command}"


with DAG(
    dag_id="customer_360_cdc_platform",
    description="Offline-safe batch/control-plane orchestration for the Customer 360 platform.",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["customer-360", "cdc-control-plane", "privacy", "dbt"],
    doc_md="""
    ### Enterprise Customer 360 batch/control-plane DAG

    Kafka owns continuous CDC transport. This DAG coordinates an offline-safe batch
    proof: fixture readiness, contracts/schema drift, landing, identity, dbt parsing,
    quality, privacy-gated activation, reconciliation, lineage, and health evidence.
    It never connects to Snowflake or an external SaaS destination.
    """,
) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id="source_readiness") as source_readiness:
        generate_cdc_events = BashOperator(
            task_id="generate_cdc_events",
            bash_command=project_cmd(
                f"{PYTHON_CMD} -m data_generation.cdc_generator "
                "--output data_generation/output/cdc_events.jsonl"
            ),
        )
        wait_for_fixture = BashOperator(
            task_id="wait_for_fixture",
            bash_command=project_cmd("test -s data_generation/output/cdc_events.jsonl"),
            retries=3,
            retry_delay=timedelta(seconds=10),
        )
        generate_cdc_events >> wait_for_fixture

    with TaskGroup(group_id="cdc_validation") as cdc_validation:
        contract_gate = BashOperator(
            task_id="contract_compatibility_gate",
            bash_command=project_cmd(
                f"{PYTHON_CMD} -m contracts.contract_gate "
                f"--candidate {CONTRACT_CANDIDATE_ARG} --fail-on-breaking"
            ),
        )
        schema_drift = BashOperator(
            task_id="schema_drift_validation",
            bash_command=project_cmd(f"{PYTHON_CMD} -m data_generation.schema_drift"),
        )
        land_raw_cdc = BashOperator(
            task_id="land_raw_cdc",
            bash_command=project_cmd(
                f"{PYTHON_CMD} -m ingestion.loader "
                "--input data_generation/output/cdc_events.jsonl --output-dir ingestion/output"
            ),
        )
        contract_gate >> schema_drift >> land_raw_cdc

    with TaskGroup(group_id="identity_and_model_validation") as identity_and_model_validation:
        resolve_identity = BashOperator(
            task_id="resolve_identity",
            bash_command=project_cmd(
                f"{PYTHON_CMD} -m identity_resolution.resolver "
                "--input ingestion/output/raw_cdc_events.jsonl --output-dir identity_resolution/output"
            ),
        )
        materialize_identity_review = BashOperator(
            task_id="materialize_identity_review",
            bash_command=project_cmd(
                f"{PYTHON_CMD} -m identity_resolution.stewardship "
                "--input ingestion/output/raw_cdc_events.jsonl --output-dir identity_resolution/output"
            ),
        )
        dbt_postgres_parse = BashOperator(
            task_id="dbt_postgres_parse",
            bash_command=project_cmd("dbt parse --project-dir dbt --profiles-dir dbt --target dev"),
        )
        resolve_identity >> materialize_identity_review >> dbt_postgres_parse

    with TaskGroup(group_id="quality") as quality:
        custom_validation = BashOperator(
            task_id="run_custom_validation",
            bash_command=project_cmd(
                f"{PYTHON_CMD} -m validation.quality_checks "
                "--input ingestion/output/raw_cdc_events.jsonl --output-dir validation/output"
            ),
        )
        expectations = BashOperator(
            task_id="run_expectations_suite",
            bash_command=project_cmd(
                f"{PYTHON_CMD} -m validation.great_expectations_runner --fail-on-error"
            ),
        )
        custom_validation >> expectations

    with TaskGroup(group_id="privacy_and_activation") as privacy_and_activation:
        build_exports = BashOperator(
            task_id="build_privacy_filtered_exports",
            bash_command=project_cmd(
                f"{PYTHON_CMD} -m reverse_etl.exporter "
                "--input ingestion/output/raw_cdc_events.jsonl --output-dir reverse_etl/exports"
            ),
        )
        verify_privacy_gate = BashOperator(
            task_id="verify_privacy_gate",
            bash_command=project_cmd(f"{PYTHON_CMD} scripts/verify_activation_privacy.py"),
        )
        simulate_syncs = BashOperator(
            task_id="simulate_destination_syncs",
            bash_command=project_cmd(
                f"{PYTHON_CMD} -m reverse_etl.destinations.simulator "
                "--export-dir reverse_etl/exports --output-dir reverse_etl/sync_logs"
            ),
        )
        reconcile_activation = BashOperator(
            task_id="reconcile_activation",
            bash_command=project_cmd(f"{PYTHON_CMD} -m reverse_etl.reconciliation"),
        )
        build_exports >> verify_privacy_gate >> simulate_syncs >> reconcile_activation

    with TaskGroup(group_id="observability") as observability:
        build_metrics = BashOperator(
            task_id="build_metrics",
            bash_command=project_cmd(
                f"{PYTHON_CMD} -m observability.metrics "
                "--input ingestion/output/raw_cdc_events.jsonl --output-dir observability/output"
            ),
        )
        emit_lineage = BashOperator(
            task_id="emit_lineage",
            bash_command=project_cmd(
                f"{PYTHON_CMD} -m observability.openlineage_events "
                "--output observability/openlineage/openlineage_events.jsonl"
            ),
        )
        quality_scorecard = BashOperator(
            task_id="quality_scorecard",
            bash_command=project_cmd(f"{PYTHON_CMD} -m validation.scorecard"),
        )
        health_report = BashOperator(
            task_id="build_health_report",
            bash_command=project_cmd(
                f"{PYTHON_CMD} scripts/build_e2e_health_report.py --fail-on-critical"
            ),
        )
        build_metrics >> emit_lineage >> quality_scorecard >> health_report

    end = EmptyOperator(task_id="end")

    (
        start
        >> source_readiness
        >> cdc_validation
        >> identity_and_model_validation
        >> quality
        >> privacy_and_activation
        >> observability
        >> end
    )
