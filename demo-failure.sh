#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
API="${FINFLUX_API:-http://localhost:8080}"

echo "=== 1. Generate a normal transaction while the system is healthy ==="
./scripts/generate-transactions.sh normal || true
sleep 3
echo "Health:"
curl -s "$API/api/v1/health" || true
echo
echo "Metrics:"
curl -s "$API/api/v1/metrics/summary" || true
echo

echo
echo "=== 2. Stop PostgreSQL (source + serving DB in this demo) ==="
docker compose stop postgres
sleep 2
echo "App health (expect database DOWN on actuator):"
curl -s "$API/actuator/health" || echo "(app may return 503)"
echo
echo "=== 3. Try to insert while Postgres is down (API should fail cleanly) ==="
if curl -sf -X POST "$API/api/v1/transactions" \
  -H "Content-Type: application/json" \
  -d "{\"transactionId\":\"TXN-DOWN-$(date +%s)\",\"customerId\":\"CUST-101\",\"accountId\":\"ACC-101\",\"amount\":10,\"currency\":\"INR\",\"timestamp\":\"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\",\"type\":\"PAYMENT\"}"; then
  echo "Unexpected success"
else
  echo "Insert failed as expected while database is down."
fi

echo
echo "Look at app logs for retry / DATABASE_UNAVAILABLE (Ctrl+C to skip wait):"
docker compose logs app --tail 30 || true

echo
echo "=== 4. Restore PostgreSQL ==="
docker compose start postgres
echo "Waiting for Postgres..."
for i in $(seq 1 40); do
  if docker compose exec -T postgres pg_isready -U finflux -d finflux >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
sleep 5
echo "Health after restore:"
curl -s "$API/api/v1/health" || true
echo
./scripts/generate-transactions.sh normal || true

echo
echo "=== 5. Send a malformed Kafka message (permanent failure -> DLQ) ==="
echo 'this is not valid json' | docker compose exec -T kafka \
  /kafka/bin/kafka-console-producer.sh --broker-list kafka:9092 --topic finflux.public.transactions
sleep 5
echo "DLQ topic sample:"
docker compose exec -T kafka \
  /kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic transactions.dlq --from-beginning --timeout-ms 8000 || true
echo
echo "Processing error rows from API:"
curl -s "$API/api/v1/errors" || true
echo
echo "Done. Unprocessed CDC events stay in Kafka until Postgres/app recover."
echo "Malformed JSON should not block the partition forever; it is sent to transactions.dlq."
