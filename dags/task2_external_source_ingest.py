import csv
import io
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

DWH_CONN_ID = "dwh_postgres"
DATA_DIR = Path("/opt/airflow/data/external_source")
SCHEMA = "external_source"

TABLES = {
    "user_order_log": ("user_order_log.csv", ["date_time", "customer_id", "quantity", "payment_amount"]),
    "user_activity_log": ("user_activity_log.csv", ["date_time", "customer_id"]),
    "customer_research": ("customer_research.csv", ["date_id", "geo_id", "sales_qty", "sales_amt"]),
}


def load_csv(table: str, file_name: str, columns: list[str]) -> None:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    with (DATA_DIR / file_name).open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        missing = set(columns) - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{file_name}: no columns {sorted(missing)} in header {reader.fieldnames}")
        file_rows = 0
        for row in reader:
            writer.writerow(row[c] for c in columns)
            file_rows += 1
    buffer.seek(0)

    target = f"{SCHEMA}.{table}"
    hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    conn = hook.get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {target}")
            cur.copy_expert(f"COPY {target} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv)", buffer)
            cur.execute(f"SELECT count(*) FROM {target}")
            table_rows = cur.fetchone()[0]
            if table_rows != file_rows:
                raise ValueError(f"{target}: {table_rows} rows loaded, {file_rows} rows in {file_name}")
    finally:
        conn.close()

    print(f"{target}: {table_rows} rows loaded from {file_name}")


with DAG(
    dag_id="task2_external_source_ingest",
    description="Task 2: load external CSV files into the external_source schema",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bda", "task2"],
    template_searchpath="/opt/airflow",
) as dag:
    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id=DWH_CONN_ID,
        sql="sql/05_ddl_external_source.sql",
        split_statements=True,
    )

    for table, (file_name, columns) in TABLES.items():
        create_tables >> PythonOperator(
            task_id=f"load_{table}",
            python_callable=load_csv,
            op_kwargs={"table": table, "file_name": file_name, "columns": columns},
        )