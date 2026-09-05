# Architecture

This file describes the **target** picture and the **slice that actually runs**.

## Target architecture (from the reference diagram)

A production financial CDC platform would look like this:

```text
Sources (Postgres, MySQL, Oracle, core banking, cards, logs, files)
        ↓
Debezium (change capture, schema history)
        ↓
Kafka (topics, partitions, consumer groups, optional Schema Registry)
        ↓
Stream processing (Spark / Flink) → lake zones (raw / processed / curated / alerts)
        ↓
Serving (SQL, APIs) + AI layer (feature store, models, case management)
        ↓
Dashboard, alerts, reports, KYC / watchlist / fraud-db integrations
```

That full picture is useful as a roadmap. It is **not** what this repository runs.

## Implemented demo slice

```text
POST /api/v1/transactions
        ↓
PostgreSQL table `transactions`  (logical replication)
        ↓
Debezium Postgres connector
        ↓
Kafka topic `finflux.public.transactions`  (key = transactionId)
        ↓
Python CDC controller + model processor  (validate → enrich → velocity → risk)
        ↓
PostgreSQL  (risk_assessments, alerts, processed_events)
Local files or optional S3  (raw / processed / curated / alerts)
Kafka  `transactions.processed`  and  `transactions.alerts`
        ↓
REST API + dashboard at http://localhost:8080
        ↓
On permanent failure: Kafka `transactions.dlq`
```

## Why one Flask app, not four microservices?

The code follows MVC (`models`, `views`, `controllers`). Running four processes would add network failure without teaching more CDC.

If this grew, the CDC controller could become its own worker service.

## MVC mapping

- **Controller:** `POST /api/v1/transactions` and the Kafka CDC loop
- **Model:** SQLAlchemy tables, `parse_cdc`, `RiskEngine`, `TransactionProcessor`
- **View:** `dashboard.html` plus JSON helpers in `views/json_views.py`

## Why not Spark yet?

Spark Structured Streaming is a good next step for large windowed aggregations. For this demo, a Kafka listener plus an in-memory velocity window is enough to show stream processing ideas, and it is easy to run on a laptop or one EC2 instance.

## Kafka keys and ordering

Debezium is configured with `message.key.columns=public.transactions:transaction_id`. Events for the same transaction go to the same partition, so their order is preserved **per transaction**. Kafka does **not** give global order across all customers.

Velocity is tracked **per customer** in process memory. Gunicorn is started with **one worker** so that window stays consistent. Several processes would need Redis.

## Storage zones

Each scored event writes JSON objects:

| Zone | Meaning |
| --- | --- |
| `raw` | original CDC payload |
| `processed` | normalized transaction + enrichment |
| `curated` | risk decision |
| `alerts` | non-LOW decisions only |

Local disk is the default. Set `STORAGE_TYPE=s3` and `AWS_S3_BUCKET` to use AWS S3 (instance role or standard AWS env vars, not hardcoded keys).
