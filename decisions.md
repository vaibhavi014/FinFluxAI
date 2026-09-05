# Design decisions

1. **Flask MVC, one process**  
   Controllers / models / views instead of Kubernetes microservices.

2. **Debezium over "the app publishes to Kafka"**  
   The point of the project is CDC: the database is the source of truth, and the log is captured.

3. **kafka-python consumer thread instead of Spark**  
   Fits a student laptop and EC2. Spark remains the documented evolution for heavy aggregations.

4. **JSON, not Avro / Schema Registry**  
   Fewer moving parts. The parser still accepts schema-wrapped or unwrapped envelopes.

5. **Rules + z-score, not a black-box neural net**  
   Every HIGH score has `triggeredRules` and an English `explanation`.

6. **In-memory velocity window**  
   Honest limitation: one Gunicorn worker. Production would use Redis or a feature store.

7. **Same Postgres for source and serving**  
   Simpler demo. A real design would separate OLTP from an analytics store.

8. **Local lake files by default**  
   S3 is an interface (`ObjectStore`) so the code path exists without requiring AWS.

9. **Manual commit + idempotency table**  
   Protects alerts from duplicate Kafka deliveries.

10. **Credentials from environment**  
    Compose / `.env`. Nothing secret belongs in git.
