#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for verify-stack" >&2
  exit 1
fi

WAREHOUSE_DSN="${WAREHOUSE_DSN:-postgresql://c360:c360@localhost:5432/c360}"
SOURCE_DSN="${SOURCE_DSN:-postgresql://c360:c360@localhost:5433/customer_ops}"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"

echo "Validating docker-compose.yml..."
docker compose config >/dev/null

echo "Starting core services..."
docker compose up -d --build postgres source_postgres zookeeper kafka kafka-connect

echo "Waiting for warehouse Postgres..."
for _ in $(seq 1 40); do
  if docker compose exec -T postgres pg_isready -U c360 -d c360 >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "Waiting for source Postgres..."
for _ in $(seq 1 40); do
  if docker compose exec -T source_postgres pg_isready -U c360 -d customer_ops >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "Creating Kafka topics..."
while read -r topic; do
  docker compose exec -T kafka kafka-topics \
    --bootstrap-server kafka:9092 \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions 3 \
    --replication-factor 1 >/dev/null
done <<'TOPICS'
cdc.customers
cdc.subscriptions
cdc.orders
cdc.engagement_events
cdc.support_interactions
cdc.marketing_engagement
dlq.raw_cdc_events
TOPICS

echo "Waiting for Kafka Connect..."
for _ in $(seq 1 40); do
  if curl -fsS "$CONNECT_URL/connectors" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "Applying operational source changes..."
SOURCE_DSN="$SOURCE_DSN" python3 -m source_systems.postgres.apply_source_changes

echo "Registering Debezium source connector..."
CONNECT_URL="$CONNECT_URL" python3 scripts/register_debezium_connector.py

echo "Running warehouse pipeline and checks..."
python3 scripts/run_postgres_pipeline.py --dsn "$WAREHOUSE_DSN"
python3 -m contracts.contract_gate --fail-on-breaking
python3 -m data_generation.schema_drift
pytest
cd dbt
dbt parse --profiles-dir .
cd ..

echo "Stack verification complete."
