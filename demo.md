# Demo script (about five minutes)

1. Start Docker Desktop.
2. From the repo root:
   - Windows: `.\scripts\start.ps1`
   - Linux/macOS: `./scripts/start.sh`
3. Open [http://localhost:8080](http://localhost:8080). The dashboard should load with zeros.
4. Generate normal traffic:
   - `.\scripts\generate-transactions.ps1 -Mode normal`
5. Refresh the dashboard. Rows appear as `PENDING`, then `SCORED` with LOW (or no alert).
6. Generate suspicious traffic:
   - `.\scripts\generate-transactions.ps1 -Mode suspicious`
7. Show a HIGH/MEDIUM row. Read the explanation out loud. Point at `triggeredRules` in `GET /api/v1/alerts`.
8. Optional: `.\scripts\demo-failure.sh` (Git Bash) or narrate:
   - stop Postgres → API returns 503 / validation of dependency
   - Kafka still holds CDC messages
   - start Postgres → processing continues
   - malformed JSON → `transactions.dlq` and `/api/v1/errors`
9. Run tests: `pytest services/finflux-app/tests -q`

## What the evaluator should see

- Inserts go to PostgreSQL, not directly to the risk table.
- Debezium connector is `RUNNING` at [http://localhost:8083/connectors/finflux-transactions-connector/status](http://localhost:8083/connectors/finflux-transactions-connector/status).
- Scores show **why**, not a random number.
- Duplicate CDC events do not create two alerts (unit + integration tests).
