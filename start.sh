#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [ ! -f .env ]; then
  cp .env.example .env
  echo "Created .env from .env.example (local demo password is in docker-compose defaults)."
fi

docker compose up -d --build

echo "Waiting for Postgres..."
for i in $(seq 1 40); do
  if docker compose exec -T postgres pg_isready -U finflux -d finflux >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "Waiting for Kafka Connect..."
for i in $(seq 1 60); do
  if curl -sf http://localhost:8083/connectors >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "Waiting for FinFlux app..."
for i in $(seq 1 60); do
  if curl -sf http://localhost:8080/api/v1/health >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

PASSWORD="${DATABASE_PASSWORD:-finflux}"
if [ -f .env ]; then
  # shellcheck disable=SC1091
  set -a
  source .env
  set +a
  PASSWORD="${DATABASE_PASSWORD:-finflux}"
fi

echo "Registering Debezium connector (idempotent)..."
curl -sf -X PUT http://localhost:8083/connectors/finflux-transactions-connector/config \
  -H "Content-Type: application/json" \
  -d "{
    \"connector.class\": \"io.debezium.connector.postgresql.PostgresConnector\",
    \"database.hostname\": \"postgres\",
    \"database.port\": \"5432\",
    \"database.user\": \"finflux\",
    \"database.password\": \"${PASSWORD}\",
    \"database.dbname\": \"finflux\",
    \"topic.prefix\": \"finflux\",
    \"table.include.list\": \"public.transactions\",
    \"plugin.name\": \"pgoutput\",
    \"slot.name\": \"finflux_debezium\",
    \"publication.name\": \"finflux_publication\",
    \"publication.autocreate.mode\": \"filtered\",
    \"tombstones.on.delete\": \"false\",
    \"decimal.handling.mode\": \"string\",
    \"time.precision.mode\": \"adaptive\",
    \"snapshot.mode\": \"initial\",
    \"slot.drop.on.stop\": \"false\",
    \"key.converter\": \"org.apache.kafka.connect.json.JsonConverter\",
    \"value.converter\": \"org.apache.kafka.connect.json.JsonConverter\",
    \"key.converter.schemas.enable\": \"false\",
    \"value.converter.schemas.enable\": \"false\",
    \"message.key.columns\": \"public.transactions:transaction_id\"
  }" >/dev/null

echo
echo "FinFlux is up."
echo "Dashboard:  http://localhost:8080"
echo "API health: http://localhost:8080/api/v1/health"
echo "Actuator:   http://localhost:8080/actuator/health"
echo "Connect:    http://localhost:8083/connectors"
echo
echo "Next: ./scripts/generate-transactions.sh suspicious"
