# Failure handling

## Retry vs DLQ

| Failure | Class | Action |
| --- | --- | --- |
| Malformed JSON | Permanent | No retry. DLQ. |
| Missing transactionId / amount <= 0 / bad timestamp | Permanent | DLQ. |
| Unknown CDC shape (missing `op` / `after`) | Permanent | DLQ. |
| PostgreSQL connection errors | Transient | Retry 3 times with exponential backoff (1s, 2s, 4s), then DLQ. |
| Kafka publish of processed/alert event fails | Transient | Same retry, then DLQ. |
| Local disk / S3 write fails | Transient | Same retry, then DLQ. |
| Enrichment exception | Transient if `fail-open=false`; otherwise continue with fallback flags | Demo default is fail-open. |

The CDC controller retries transient errors, then publishes to `transactions.dlq` and **commits** the Kafka offset so a poison message does not block the partition.

## DLQ envelope

```json
{
  "originalEvent": "...",
  "transactionId": "TXN-1001",
  "eventId": "cdc:...",
  "errorType": "MALFORMED_JSON",
  "errorMessage": "Kafka payload is not valid JSON",
  "service": "finflux-processor",
  "timestamp": "...",
  "correlationId": "...",
  "retryCount": 3
}
```

A row is also written to `processing_errors` when the database is available.

## Idempotency

Each CDC event gets `eventId = cdc:{lsn}:{txId}:{op}:{transactionId}`.

`processed_events.event_id` is the primary key. If Kafka delivers the same event twice:

1. The processor sees the row already exists.
2. It logs `Duplicate CDC event ignored`.
3. It does not insert a second alert.
4. It does not run risk scoring again.

Retries of the **same** transaction id in the in-memory velocity window replace the previous sample so a retry does not inflate HIGH_VELOCITY.

This is **at-least-once** processing with an idempotency table. It is not a two-phase commit across Kafka and PostgreSQL. Duplicate messages can still appear on `transactions.processed` if the process crashes after publish and before the offset commit. The dashboard data (Postgres) is protected.

## What we do not claim

We do not claim zero data loss, exactly-once across all topics, or multi-AZ Kafka replication. Docker Compose uses replication factor 1.

## Health

- `GET /api/v1/health` — process is up.
- `GET /actuator/health` — includes datasource and a Kafka admin check.
