include .env
export

.PHONY: up down reset logs build vsql

up:
	mkdir -p logs plugins
	docker compose up -d --wait --wait-timeout 400

down:
	docker compose down

reset:
	docker compose down -v

logs:
	docker compose logs -f airflow-scheduler

build:
	docker compose build

vsql:
	docker compose exec vertica vsql -U dbadmin -w $(VERTICA_PASSWORD) -d $(VERTICA_DB_NAME)
