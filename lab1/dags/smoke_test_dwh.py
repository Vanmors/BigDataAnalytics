"""Smoke test of the environment - not part of the lab.

Proves that Airflow sees the warehouse through the `dwh_postgres` connection and that
the ./sql folder is mounted inside the containers. Write your own DAGs next to this file.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DWH_CONN_ID = "dwh_postgres"


def ping_warehouse() -> None:
    hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    version = hook.get_first("SELECT version()")[0]
    schemas = hook.get_records(
        "SELECT nspname FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' AND nspname <> 'information_schema'"
    )
    print(f"connected to: {version}")
    print("schemas in the warehouse:", ", ".join(s[0] for s in schemas))


def list_mounted_files() -> None:
    for folder in ("/opt/airflow/sql", "/opt/airflow/data"):
        files = sorted(str(p) for p in Path(folder).rglob("*") if p.is_file())
        print(f"{folder}: {len(files)} files")
        for path in files[:10]:
            print("   ", path)


with DAG(
    dag_id="smoke_test_dwh",
    description="Environment check: Airflow -> PostgreSQL connection and mounted folders",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["infra"],
) as dag:
    PythonOperator(task_id="ping_warehouse", python_callable=ping_warehouse) >> \
        PythonOperator(task_id="list_mounted_files", python_callable=list_mounted_files)
