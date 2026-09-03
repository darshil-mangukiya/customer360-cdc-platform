#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for docker-e2e-proof" >&2
  exit 1
fi

WAREHOUSE_DSN="${WAREHOUSE_DSN:-postgresql://c360:c360@localhost:5432/c360}"
SOURCE_DSN="${SOURCE_DSN:-postgresql://c360:c360@localhost:5433/customer_ops}"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"

docker compose config >/dev/null
docker compose up -d --build postgres source_postgres zookeeper kafka kafka-connect activation_api airflow-init airflow-webserver airflow-scheduler

for service in postgres source_postgres; do
  echo "Waiting for $service..."
  for _ in $(seq 1 45); do
    if docker compose exec -T "$service" pg_isready >/dev/null 2>&1; then
      break
    fi
    sleep 2
  done
done

echo "Waiting for Kafka Connect..."
for _ in $(seq 1 45); do
  if curl -fsS "$CONNECT_URL/connectors" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

SOURCE_DSN="$SOURCE_DSN" python3 -m source_systems.postgres.apply_source_changes
CONNECT_URL="$CONNECT_URL" python3 scripts/register_debezium_connector.py
python3 scripts/run_postgres_pipeline.py --dsn "$WAREHOUSE_DSN"
python3 -m validation.great_expectations_runner --fail-on-error
python3 scripts/build_e2e_health_report.py --fail-on-critical

echo "Docker E2E proof complete. See reports/e2e_health_report.md"
