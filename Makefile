PYTHON ?= python3

.PHONY: install install-dev generate ingest replay dlq publish-dry identity exports sync validate scorecard gx-validate observe lineage report e2e-report benchmark scale-report drift contract-gate source-seed cdc-demo debezium-register catalog dbt-docs-index incidents privacy-delete airflow-proof airflow-runtime-up airflow-runtime-down api-smoke smoke postgres-pipeline bootstrap verify-stack docker-e2e test docs-check sql-lint dbt-run dbt-test snowflake-check reconcile migration-parity validate-platform docker-up docker-down

install:
	python3 -m pip install -r requirements.txt

install-dev:
	python3 -m pip install -r requirements-dev.txt

generate:
	python3 -m data_generation.cdc_generator --output data_generation/output/cdc_events.jsonl

ingest:
	python3 -m ingestion.loader --input data_generation/output/cdc_events.jsonl --output-dir ingestion/output

replay:
	python3 -m ingestion.replay --input data_generation/output/cdc_events.jsonl --output ingestion/replay_output/replay_events.jsonl --dry-run

dlq:
	python3 -m ingestion.dlq_reprocessor --input ingestion/output/rejected_events.jsonl --output-dir ingestion/dlq_reprocessed

publish-dry:
	python3 -m streaming.producer --input data_generation/output/cdc_events.jsonl --dry-run

identity:
	python3 -m identity_resolution.resolver --input ingestion/output/raw_cdc_events.jsonl --output-dir identity_resolution/output
	python3 -m identity_resolution.stewardship --input ingestion/output/raw_cdc_events.jsonl --output-dir identity_resolution/output

exports:
	python3 -m reverse_etl.exporter --input ingestion/output/raw_cdc_events.jsonl --output-dir reverse_etl/exports

sync:
	python3 -m reverse_etl.destinations.simulator --export-dir reverse_etl/exports --output-dir reverse_etl/sync_logs

validate:
	python3 -m validation.quality_checks --input ingestion/output/raw_cdc_events.jsonl --output-dir validation/output

scorecard:
	python3 -m validation.scorecard --output validation/output/quality_scorecard.json

gx-validate:
	python3 -m validation.great_expectations_runner --fail-on-error

observe:
	python3 -m observability.metrics --input ingestion/output/raw_cdc_events.jsonl --output-dir observability/output

lineage:
	python3 -m observability.openlineage_events --output observability/openlineage/openlineage_events.jsonl

report:
	python3 scripts/build_operational_report.py --output reports/operational_report.html

e2e-report:
	python3 scripts/build_e2e_health_report.py --fail-on-critical

benchmark:
	python3 scripts/run_benchmark.py --profile small --output benchmark/output/benchmark_summary.csv

scale-report:
	python3 scripts/build_scale_report.py --input benchmark/output/benchmark_summary.csv --output benchmark/output/scale_report.md

drift:
	python3 -m data_generation.schema_drift --output-dir data_generation/schema_drift_output

contract-gate:
	python3 -m contracts.contract_gate --output contracts/output/contract_compatibility_report.csv --fail-on-breaking

source-seed:
	python3 -m source_systems.postgres.apply_source_changes

cdc-demo:
	python3 scripts/build_cdc_demo_proof.py --output reports/sample_debezium_cdc_demo.md

debezium-register:
	./scripts/register_debezium_connector.sh

catalog:
	python3 scripts/generate_data_product_catalog.py

dbt-docs-index:
	python3 scripts/build_dbt_docs_index.py

incidents:
	python3 scripts/simulate_incident.py

privacy-delete:
	@test -n "$(TENANT_ID)" || (echo "TENANT_ID is required" >&2; exit 2)
	@test -n "$(CUSTOMER_ID)" || (echo "CUSTOMER_ID is required" >&2; exit 2)
	@test -n "$(CUSTOMER_EMAIL)" || (echo "CUSTOMER_EMAIL is required" >&2; exit 2)
	$(PYTHON) -m privacy.deletion_workflow --tenant-id "$(TENANT_ID)" --canonical-customer-id "$(CUSTOMER_ID)" --email "$(CUSTOMER_EMAIL)"

airflow-proof:
	python3 scripts/build_airflow_dag_proof.py --output reports/sample_airflow_dag_proof.md

airflow-runtime-up:
	docker compose up -d --build postgres airflow-webserver airflow-scheduler

airflow-runtime-down:
	docker compose stop airflow-scheduler airflow-webserver postgres

api-smoke:
	python3 scripts/run_local_pipeline.py >/tmp/customer360_api_smoke_pipeline.log
	python3 scripts/run_api_smoke.py --output reports/sample_api_responses.md

smoke:
	python3 scripts/run_local_pipeline.py

postgres-pipeline:
	python3 scripts/run_postgres_pipeline.py

bootstrap:
	./scripts/bootstrap_local_stack.sh

verify-stack:
	./scripts/verify_stack.sh

docker-e2e:
	./scripts/docker_e2e_proof.sh

test:
	pytest

docs-check:
	test -f README.md
	test -f docs/architecture_overview.md
	test -f docs/cdc_design.md
	test -f docs/identity_resolution.md
	test -f docs/warehouse_modeling.md
	test -f docs/master_data_management.md
	test -f docs/reverse_etl_design.md
	test -f docs/activation_reconciliation.md
	test -f docs/privacy_governance.md
	test -f docs/data_quality_observability.md
	test -f docs/semantic_layer/customer_metrics.md
	test -f docs/runbooks/README.md
	test -f docs/COST_AND_PERFORMANCE_REPORT.md
	test -f docs/environment_promotion.md
	test -f migration/README.md
	test -f migration/MIGRATION_PARITY_REPORT.md
	test -f docs/runbooks/cdc_replay.md
	test -f docs/runbooks/activation_reconciliation.md
	test -f docs/runbooks/customer_360_freshness.md
	test -f docs/runbooks/tenant_isolation.md
	test -f docs/proof/snowflake_validation_matrix.md
	test -f docs/proof/AIRFLOW_RUNTIME_EVIDENCE.md
	test -f docs/proof/real_kafka_debezium_execution.md

sql-lint:
	sqlfluff lint warehouse/sql/00_schemas.sql warehouse/sql/08_rls_tenant_policies.sql source_systems/postgres/init/00_operational_sources.sql source_systems/postgres/init/01_cdc_demo_changes.sql --dialect postgres --templater raw --config .sqlfluff
	sqlfluff lint snowflake/sql --dialect snowflake --templater raw --config .sqlfluff

dbt-run:
	cd dbt && dbt run --profiles-dir .

dbt-test:
	cd dbt && dbt test --profiles-dir .

snowflake-check:
	@if [ -z "$$SNOWFLAKE_ACCOUNT" ]; then \
		echo "SNOWFLAKE_ACCOUNT not set - skipping. Export SNOWFLAKE_* vars (see .env.example / snowflake/README.md) to run this."; \
	else \
		cd dbt && dbt debug --profiles-dir . --target snowflake && dbt parse --profiles-dir . --target snowflake; \
	fi

reconcile:
	python3 -m reverse_etl.reconciliation --export-dir reverse_etl/exports --sync-log-dir reverse_etl/sync_logs --output-dir reverse_etl/reconciliation

migration-parity:
	python3 -m migration.parity --source migration/fixtures/postgres_customer_360.json

validate-platform:
	$(PYTHON) -m validation.platform_validator

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down -v
