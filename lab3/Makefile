COMPOSE ?= docker compose

.PHONY: up demo report check stop

up:
	$(COMPOSE) up -d --wait --wait-timeout 240

demo: up
	$(COMPOSE) exec -T airflow-web python - < scripts/demo.py

report:
	$(COMPOSE) exec -T postgres psql -X -v ON_ERROR_STOP=1 -U admin -d lab3 < scripts/reports.sql

check:
	$(COMPOSE) exec -T postgres psql -X -v ON_ERROR_STOP=1 -U admin -d lab3 < scripts/check.sql

stop:
	$(COMPOSE) stop
