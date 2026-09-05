.PHONY: up down test seed suspicious logs

up:
	./scripts/start.sh

down:
	./scripts/stop.sh

test:
	cd services/finflux-app && pip install -r requirements-dev.txt && pytest -q

seed:
	./scripts/seed-data.sh

suspicious:
	./scripts/generate-transactions.sh suspicious

logs:
	docker compose logs -f app
