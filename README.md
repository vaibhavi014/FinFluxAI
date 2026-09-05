Topics used by the demo:

- `finflux.public.transactions` — Debezium CDC (created by the connector)
- `transactions.processed` — scored events (3 partitions, RF=1)
- `transactions.alerts` — non-LOW scores
- `transactions.dlq` — poison / exhausted retries (1 partition)

Consumer group: `finflux-processor`.
