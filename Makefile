include .env
export

PSQL = docker compose exec -T postgres-dwh psql -v ON_ERROR_STOP=1 -U $(DWH_DB_USER) -d $(DWH_DB_NAME)

.PHONY: up down reset logs gen seed psql

up:
	mkdir -p logs plugins
	docker compose up -d

down:
	docker compose down

reset:
	docker compose down -v

logs:
	docker compose logs -f airflow-scheduler

gen:
	python3 data/generate_data.py

seed:
	$(PSQL) -f /sql/01_ddl_craft_market.sql
	$(PSQL) -f /sql/04_seed_craft_market.sql

psql:
	docker compose exec postgres-dwh psql -U $(DWH_DB_USER) -d $(DWH_DB_NAME)
