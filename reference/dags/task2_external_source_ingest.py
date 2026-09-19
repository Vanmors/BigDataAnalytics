"""Task 2: load three CSV files into the `external_source` schema of the warehouse.

Flow:  DDL  ->  CSV -> staging (COPY)  ->  staging -> external_source (INSERT ... SELECT)  ->  row count check

Everything is a full reload, so the DAG can be re-run safely.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

DWH_CONN_ID = "dwh_postgres"
DATA_DIR = Path("/opt/airflow/data/external_source")

# CSV file -> staging table. Column order in the file matches the table definition.
FILES = {
    "order_statuses": "staging.order_statuses_raw",
    "user_behavior": "staging.user_behavior_raw",
    "marketing_research": "staging.marketing_research_raw",
}


def load_csv_to_staging(file_name: str, table: str) -> None:
    """COPY one CSV file into its staging table (truncate first)."""
    csv_path = DATA_DIR / f"{file_name}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"{csv_path} not found - run `make gen` on the host first")

    hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table}")
        with csv_path.open("r", encoding="utf-8") as f:
            cur.copy_expert(f"COPY {table} FROM STDIN WITH (FORMAT csv, HEADER true)", f)
        cur.execute(f"SELECT count(*) FROM {table}")
        print(f"{table}: {cur.fetchone()[0]} rows loaded from {csv_path.name}")
        conn.commit()


def check_row_counts() -> None:
    """Simple data quality gate: every target table must contain rows."""
    hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    for table in ("external_source.order_statuses",
                  "external_source.user_behavior",
                  "external_source.marketing_research"):
        count = hook.get_first(f"SELECT count(*) FROM {table}")[0]
        print(f"{table}: {count} rows")
        if count == 0:
            raise ValueError(f"{table} is empty after the load")


with DAG(
    dag_id="task2_external_source_ingest",
    description="Task 2: CSV files -> staging -> external_source",
    start_date=datetime(2024, 1, 1),
    schedule=None,          # triggered manually
    catchup=False,
    # .sql files are mounted at /opt/airflow/reference, so the paths below resolve.
    template_searchpath="/opt/airflow",
    tags=["bda", "task2"],
) as dag:
    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id=DWH_CONN_ID,
        sql="reference/sql/task2/01_ddl_external_source.sql",
        split_statements=True,
    )

    transform = SQLExecuteQueryOperator(
        task_id="staging_to_external_source",
        conn_id=DWH_CONN_ID,
        sql="reference/sql/task2/02_staging_to_external_source.sql",
        split_statements=True,
    )

    check = PythonOperator(task_id="check_row_counts", python_callable=check_row_counts)

    for file_name, staging_table in FILES.items():
        extract = PythonOperator(
            task_id=f"load_{file_name}",
            python_callable=load_csv_to_staging,
            op_kwargs={"file_name": file_name, "table": staging_table},
        )
        create_tables >> extract >> transform

    transform >> check
