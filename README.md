# FinFlux

Real-time capture of payment changes, a small stream processor, and explainable suspicious-activity scoring.

This is a student engineering demo (Razorpay-style evaluation): a **working vertical slice**, not a fake bank-wide platform.

## What is FinFlux?

Banks and payment systems write transactions into databases continuously. Asking that database “what just changed?” with polling is slow and easy to miss. **Change Data Capture (CDC)** reads the database log, turns row changes into events, and lets another program score those events as they arrive.

FinFlux inserts a transaction into **PostgreSQL**. **Debezium** publishes the change to **Kafka**. A **Python Flask** processor (MVC) validates the event, adds a little enrichment, computes velocity features, and runs a **rule engine plus a simple z-score anomaly check**. Results are stored in Postgres (and optional lake files), exposed over **REST**, and shown on a **small dashboard**. Poison messages go to a **dead-letter topic**.

## Problem

Financial systems produce many transaction inserts and updates. Operations and risk teams need those changes quickly, with a clear reason when something looks unusual. Temporary outages should not throw the events away.

## What this project demonstrates

- Event-driven flow: DB write → CDC → Kafka → processor → API
- Debezium on PostgreSQL (INSERT/UPDATE/DELETE envelope)
- Kafka topics, keys, consumer group, manual offset ack
- Validation, enrichment, windowed velocity features
- Explainable risk score (LOW / MEDIUM / HIGH)
- Idempotency so duplicate Kafka deliveries do not double-alert
- Retries for transient errors, DLQ for permanent errors
- Structured logs, correlation id, Actuator health, basic metrics
- Docker Compose on a laptop or a single AWS EC2 host
- Pytest for parser, validator, risk, processor idempotency, and API
- MVC layout: models, views, controllers

## Architecture

See [docs/architecture.md](docs/architecture.md) for the large target diagram vs this demo.

```text
Client / generator
    → REST POST /api/v1/transactions
    → PostgreSQL (transactions)
    → Debezium
    → Kafka topic finflux.public.transactions
    → Stream processor (Flask CDC controller + model layer)
         validate → enrich → risk engine
    → PostgreSQL (risk_assessments, alerts, processed_events)
    → Kafka transactions.processed / transactions.alerts
    → REST + dashboard
    → failures after retry: transactions.dlq
```

## What is actually implemented

- PostgreSQL 16 with logical replication
- SQLAlchemy tables: customers, transactions, processed_events, risk_assessments, alerts, processing_errors
- Debezium Postgres connector (JSON, no Schema Registry)
- Kafka (Debezium Kafka + ZooKeeper images in Compose)
- One Python 3.12 Flask app (MVC): ingest API, CDC consumer thread, risk engine, lake writer, Jinja dashboard
- Rule-based detection + amount z-score when there is enough customer history
- Local filesystem lake zones; optional AWS S3 if configured
- REST API listed in [docs/api.md](docs/api.md)
- Dashboard at `/`
- Scripts to generate traffic and to demo failure
- Pytest (parser, validator, risk, duplicate CDC event, API)

## What is not implemented

These appear on many “target architecture” slides. They are **not** in this repo:

- Oracle, MySQL, SQL Server, core banking, card management hosts
- Spark / Flink jobs
- Schema Registry / Avro
- Real KYC, sanctions, device-intelligence, or external fraud APIs
- Case management, Customer 360, Salesforce
- Multi-region Kafka, exactly-once transactions across all systems
- Login / OAuth for the API
- A trained neural network or LLM “agent”

Claiming those would be dishonest. They are reasonable **production extensions**.

## Tech stack

| Area | Choice | Why |
| --- | --- | --- |
| Language | Python 3.12 | Easy to read and explain in a student interview |
| App shape | Flask MVC | Controllers take HTTP/Kafka input, models hold data+rules, views render JSON/HTML |
| DB | PostgreSQL + SQLAlchemy | Native logical decoding for Debezium |
| CDC | Debezium 2.7 | Standard tool for this problem |
| Messaging | Apache Kafka + kafka-python | Buffer between capture and scoring |
| Processing | CDC consumer thread | One process so velocity state stays consistent |
| Tests | Pytest | Behavior tests without Docker |
| Run | Docker Compose | Same path on laptop and EC2 |

## Project structure

```text
finflux/
  README.md
  docker-compose.yml
  docs/
  infrastructure/
  scripts/
  services/finflux-app/
    finflux/
      models/         # SQLAlchemy entities, CDC parse, risk engine
      views/          # Jinja dashboard + JSON payloads
      controllers/    # HTTP API + Kafka CDC consumer
    tests/
    wsgi.py
```

### MVC in this project

| Layer | Folder | Responsibility |
| --- | --- | --- |
| Model | `finflux/models/` | Tables, CDC parse/validate, risk score, lake writes, idempotency |
| View | `finflux/views/` | Dashboard HTML and JSON response shapes |
| Controller | `finflux/controllers/` | REST routes and the Kafka consumer loop |

## How the system works

1. `POST /api/v1/transactions` writes a row. That mimics a payment service writing to OLTP.
2. Debezium reads the WAL, emits a JSON CDC envelope to `finflux.public.transactions`. The **Kafka key** is `transaction_id` so one transaction stays on one partition.
3. The processor parses `op` / `after`, validates fields, maps city → region, updates a per-customer sliding window, and scores.
4. Score bands: **0–39 LOW**, **40–69 MEDIUM**, **70–100 HIGH**. MEDIUM and HIGH create an alert row.
5. `processed_events` stores the CDC `eventId` (LSN + tx + op + transaction id). A second delivery is logged and skipped.
6. Bad JSON never retries forever; it is published to `transactions.dlq`.

### Risk rules (demo)

- `HIGH_VALUE_TRANSACTION` — amount ≥ 50,000 INR
- `HIGH_VELOCITY` — 5+ events for the customer in 60 seconds (+40)
- `GEO_VELOCITY` — different city within 5 minutes
- `UNUSUAL_MERCHANT_CATEGORY` — CRYPTO, GAMBLING, MONEY_TRANSFER, GIFT_CARDS
- `REPEATED_FAILED_ATTEMPTS` — 3+ FAILED in 10 minutes
- `ROUND_AMOUNT_PATTERN` — large round thousands
- `AMOUNT_ANOMALY` — z-score vs recent amounts when history ≥ 5

The explanation string is built from the rules that actually fired.

## Prerequisites

- Docker and Docker Compose v2
- Git
- For tests: Python 3.12+
- Optional: Git Bash if you prefer `.sh` scripts on Windows

Versions used in Compose: PostgreSQL 16, Debezium 2.7.3, Python 3.12.

## Run locally

```bash
git clone <your-gitlab-url> finflux
cd finflux
cp .env.example .env
```

Windows PowerShell:

```powershell
.\scripts\start.ps1
.\scripts\seed-data.ps1
.\scripts\generate-transactions.ps1 -Mode suspicious
```

Linux / macOS / Git Bash:

```bash
chmod +x scripts/*.sh
./scripts/start.sh
./scripts/seed-data.sh
./scripts/generate-transactions.sh suspicious
```

Then open:

- Dashboard: http://localhost:8080
- Health: http://localhost:8080/api/v1/health
- Actuator: http://localhost:8080/actuator/health
- Connector: http://localhost:8083/connectors

Stop: `.\scripts\stop.ps1` or `./scripts/stop.sh`

First start builds the JAR inside Docker and can take several minutes.

## Run on AWS EC2

Keep secrets in the instance environment or a local `.env` file. Do not paste AWS keys into this README.

1. Launch a small Ubuntu instance (2 vCPU / 4 GB is enough for a demo). Open security group ports **22** and **8080** (and 8083 only if you need Connect from your laptop).
2. Install Docker and the Compose plugin.
3. `git clone` this GitLab repository.
4. `cp .env.example .env` and set `DATABASE_PASSWORD` to a unique value.
5. Run `./scripts/start.sh` (or the PowerShell equivalents if you use that).
6. Check `http://<public-ip>:8080/api/v1/health`.
7. Open the dashboard on port 8080.
8. Optional: set `STORAGE_TYPE=s3` and `AWS_S3_BUCKET`, and attach an IAM role that can `s3:PutObject`. Do not commit access keys.

## Demo

Follow [docs/demo.md](docs/demo.md). Short version: start → generate `normal` → generate `suspicious` → read an explanation → optional `./scripts/demo-failure.sh`.

## What happens if something breaks at 2 AM?

At 02:00 the processor cannot reach PostgreSQL.

1. The next CDC record fails with a **transient** database error.
2. Actuator health for the database goes **DOWN**. Kafka itself is still up, so **events stay on the topic**.
3. The consumer retries with backoff (about 1s, 2s, 4s) instead of exiting the JVM on the first failure.
4. New REST inserts also fail with `DATABASE_UNAVAILABLE` / 503. That is expected: the source DB is the same instance in this demo.
5. When Postgres is back, Hikari reconnects, the consumer continues from the last **committed** offset, and scoring resumes.
6. If the same CDC record is seen again, **idempotency** skips a second alert.
7. If the payload is illegal JSON, retries will not help. That record is written to **`transactions.dlq`** and the offset moves on.

Temporary failure should not mean “the Kafka message disappeared.” It also does **not** mean a formal zero-loss guarantee (replication factor is 1 in Compose).

## Idempotency

Financial processing cannot assume Kafka will deliver a message once. See [docs/failure-handling.md](docs/failure-handling.md). Test: `test_processor_duplicate_and_alert`.

## Observability

Logs include `correlationId`, `transactionId`, and `eventId` when known.

Metrics (in-process counters, also in `/api/v1/metrics/summary`):

- `transactionsProcessedTotal`
- `transactionsFailedTotal`
- `transactionsDlqTotal`
- `transactionsDuplicateTotal`
- `highRiskTransactionsTotal`
- `lastProcessingLatencyMs`

Summary: `GET /api/v1/metrics/summary`.

## Testing

From the repo root:

```bash
pip install -r services/finflux-app/requirements-dev.txt
cd services/finflux-app && pytest -q
```

## Configuration

Copy `.env.example`. Important variables: `DATABASE_*`, `KAFKA_BOOTSTRAP_SERVERS`, topic names, `STORAGE_TYPE`, `AWS_S3_BUCKET`. Risk thresholds are environment variables with defaults in `finflux/config.py`.

## Known limitations (say this in an interview)

- One processor instance; velocity state is in memory.
- Source OLTP and serving tables share one Postgres.
- Kafka publish of “processed” events is not in the same transaction as the database write.
- No authentication on the API.

## Interview notes

Be ready to draw the slice, explain **why the Kafka key is transactionId**, **why malformed JSON must not retry**, **how eventId is built from LSN**, and **how LOW/MEDIUM/HIGH is computed**. Read [docs/decisions.md](docs/decisions.md).

## License

MIT. See [LICENSE](LICENSE).
