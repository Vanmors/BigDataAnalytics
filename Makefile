include .env
export

PSQL = docker compose exec -T postgres-dwh psql -v ON_ERROR_STOP=1 -U $(DWH_DB_USER) -d $(DWH_DB_NAME)

.PHONY: help up down restart logs ps gen ref-ddl ref-seed drop reset psql shell

help:
	@echo "make up       - start Airflow + PostgreSQL (UI: http://localhost:8080, airflow/airflow)"
	@echo "make down     - stop containers (data is kept)"
	@echo "make reset    - stop containers and DROP all data volumes"
	@echo "make gen      - generate test CSV data into ./data"
	@echo "make ref-ddl  - create schemas and tables from reference/sql (playground data model)"
	@echo "make ref-seed - load ./data/source/*.csv into schema source (task 1)"
	@echo "make drop     - drop schemas source / staging / external_source / mart"
	@echo "make psql     - open psql against the warehouse"
	@echo "make ps       - container status"
	@echo "make logs     - follow Airflow scheduler logs"

up:
	mkdir -p logs plugins
	docker compose up -d
	@echo "Airflow UI: http://localhost:8080  (login: $(AIRFLOW_WWW_USER) / $(AIRFLOW_WWW_PASSWORD))"
	@echo "Warehouse:  localhost:$(DWH_HOST_PORT)  db=$(DWH_DB_NAME) user=$(DWH_DB_USER) password=$(DWH_DB_PASSWORD)"

down:
	docker compose down

restart:
	docker compose restart airflow-scheduler airflow-webserver

reset:
	docker compose down -v

ps:
	docker compose ps

logs:
	docker compose logs -f airflow-scheduler

gen:
	python3 data/generate_data.py

# The DDL lives in reference/ on purpose: the data model is part of the lab, write your own
# files in ./sql. These targets are here so there is something to run queries against.
ref-ddl:
	$(PSQL) -f /reference/sql/task1/01_ddl_source.sql
	$(PSQL) -f /reference/sql/task1/02_ddl_mart.sql
	$(PSQL) -f /reference/sql/task2/01_ddl_external_source.sql
	@echo "DDL applied."

drop:
	$(PSQL) -c "DROP SCHEMA IF EXISTS mart, external_source, staging, source CASCADE;"

ref-seed:
	$(PSQL) -f /reference/sql/task1/03_seed_source.sql
	@echo "Source tables loaded. Row counts:"
	@$(PSQL) -c "select 'master' t, count(*) from source.master union all \
	             select 'product', count(*) from source.product union all \
	             select 'master_product', count(*) from source.master_product union all \
	             select 'client', count(*) from source.client union all \
	             select 'orders', count(*) from source.orders;"

psql:
	docker compose exec postgres-dwh psql -U $(DWH_DB_USER) -d $(DWH_DB_NAME)

shell:
	docker compose exec airflow-scheduler bash
