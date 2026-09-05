# HTTP API

Base URL: `http://localhost:8080`

All JSON error bodies look like:

```json
{
  "timestamp": "2026-01-01T10:00:00Z",
  "status": 400,
  "error": "VALIDATION_ERROR",
  "message": "amount must be greater than zero",
  "correlationId": "..."
}
```

Send `X-Correlation-Id` if you want a stable id in logs. The API always echoes one back.

## Endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| POST | `/api/v1/transactions` | Insert a source row (202 Accepted). Scoring happens asynchronously via CDC. |
| GET | `/api/v1/transactions` | Latest 50 source transactions plus score if ready. |
| GET | `/api/v1/transactions/{id}` | One transaction. |
| GET | `/api/v1/risk/{transactionId}` | Score, rules, explanation. 404 if CDC has not been processed yet. |
| GET | `/api/v1/alerts` | Latest medium/high alerts. |
| GET | `/api/v1/errors` | Rows that were sent to the DLQ (when DB write succeeded). |
| GET | `/api/v1/health` | Liveness. |
| GET | `/api/v1/metrics/summary` | Counts + in-process counters. |
| GET | `/actuator/health` | App health (Postgres + Kafka). |
| GET | `/actuator/prometheus` | Micrometer scrape endpoint. |

## Create transaction

```json
{
  "transactionId": "TXN-1001",
  "customerId": "CUST-101",
  "accountId": "ACC-1001",
  "amount": 95000,
  "currency": "INR",
  "merchantId": "MERCHANT-55",
  "merchantCategory": "ELECTRONICS",
  "deviceId": "DEVICE-77",
  "ipAddress": "192.168.1.10",
  "location": "Mumbai",
  "timestamp": "2026-01-01T10:00:00Z",
  "type": "PAYMENT",
  "status": "SUCCESS"
}
```

`type` must be `PAYMENT`, `TRANSFER`, `WITHDRAWAL`, or `REFUND`.  
`status` is optional (`SUCCESS`, `FAILED`, `PENDING`).

Duplicate `transactionId` returns **409**.

## Auth

There is no authentication in this demo. Treat the API as local/private. Auth would be a production follow-up.
