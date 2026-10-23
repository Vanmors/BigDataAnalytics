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
	$(PSQL) --single-transaction -f /sql/04_seed_craft_market.sql

psql:
	docker compose exec postgres-dwh psql -U $(DWH_DB_USER) -d $(DWH_DB_NAME)

.PHONY: build task3-setup task3-mongo-init task3-seed task3-baseline task3-consume task3-test
build:
	docker compose build

task3-mongo-init:
	docker compose up -d --wait --wait-timeout 90 mongo
	docker compose exec -T mongo mongosh --quiet /scripts/task3-init.js

task3-setup: task3-mongo-init
	$(PSQL) -f /sql/01_ddl_craft_market.sql
	$(PSQL) -f /sql/05_task3.sql

task3-seed:
	docker compose run --rm airflow-scheduler python /opt/airflow/mongo/task3-seed.py

task3-baseline:
	docker compose run --rm airflow-scheduler python /opt/airflow/plugins/task3_mongo_cdc.py baseline --pause-held

task3-consume:
	docker compose run --rm airflow-scheduler python /opt/airflow/plugins/task3_mongo_cdc.py consume

task3-test:
	docker compose -p bda-task3-tests -f tests/compose.yml up -d --wait --wait-timeout 90 mongo pg
	docker compose -p bda-task3-tests -f tests/compose.yml exec -T mongo mongosh --quiet --file /dev/stdin < mongo/task3-init.js
	docker compose -p bda-task3-tests -f tests/compose.yml run --rm runner -m unittest discover -s tests -v
