#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for bootstrap_local_stack.sh" >&2
  exit 1
fi

WAREHOUSE_DSN="${WAREHOUSE_DSN:-postgresql://c360:c360@localhost:5432/c360}"

docker compose up -d --build postgres source_postgres zookeeper kafka kafka-connect

echo "Waiting for Postgres..."
for _ in $(seq 1 30); do
  if docker compose exec -T postgres pg_isready -U c360 -d c360 >/dev/null 2>&1; then
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
TOPICS

python3 scripts/run_postgres_pipeline.py --dsn "$WAREHOUSE_DSN"

echo "Customer 360 stack is ready."
echo "Postgres: localhost:5432"
echo "Operational source Postgres: localhost:5433"
echo "Kafka: localhost:29092"
echo "Kafka Connect: localhost:8083"
echo "Activation API: run 'uvicorn api.main:app --reload --port 8000'"
