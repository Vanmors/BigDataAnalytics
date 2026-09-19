"""Task 1: rebuild the client data mart (mart.customer_report) for one reporting month.

Flow:  DDL (source + mart)  ->  load mart for the period  ->  sanity check

The reporting period comes from the DAG run date (logical_date), so a monthly schedule
plus catchup would backfill the whole history. The load deletes the period before inserting,
which makes every re-run idempotent.

Note: the source tables are filled by `make seed` on the host, not by this DAG - the OLTP
system is "someone else's" database in the story of the task.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

DWH_CONN_ID = "dwh_postgres"
SQL_DIR = Path("/opt/airflow/reference/sql/task1")


def load_mart(logical_date=None, **_) -> None:
    """Run 04_mart_load.sql for the month of the run date.

    The file uses psql-style :report_year / :report_month placeholders so that it can also be
    executed by hand (`make psql` -> \\i /reference/sql/task1/04_mart_load.sql). Here they are substituted
    before execution.
    """
    year, month = logical_date.year, logical_date.month
    sql = (SQL_DIR / "04_mart_load.sql").read_text(encoding="utf-8")
    sql = sql.replace(":report_year", str(year)).replace(":report_month", str(month))

    hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    hook.run(sql, autocommit=True)

    rows = hook.get_first(
        "SELECT count(*) FROM mart.customer_report WHERE report_year = %s AND report_month = %s",
        parameters=(year, month),
    )[0]
    print(f"mart.customer_report: {rows} rows for {year}-{month:02d}")


def check_mart(logical_date=None, **_) -> None:
    """Sanity checks that are easy to defend at the lab review."""
    hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    year, month = logical_date.year, logical_date.month

    duplicates = hook.get_first(
        """SELECT count(*) FROM (
               SELECT client_id FROM mart.customer_report
               WHERE report_year = %s AND report_month = %s
               GROUP BY client_id HAVING count(*) > 1
           ) d""",
        parameters=(year, month),
    )[0]
    if duplicates:
        raise ValueError(f"{duplicates} clients have more than one row for {year}-{month:02d}")

    broken = hook.get_first(
        """SELECT count(*) FROM mart.customer_report
           WHERE report_year = %s AND report_month = %s
             AND orders_count <> orders_done_count + orders_not_done_count""",
        parameters=(year, month),
    )[0]
    if broken:
        raise ValueError(f"{broken} rows where orders_count <> done + not_done")

    print("mart checks passed")


with DAG(
    dag_id="task1_customer_report_mart",
    description="Task 1: build mart.customer_report per reporting month",
    start_date=datetime(2024, 1, 1),
    schedule="@monthly",
    catchup=False,          # set to True to backfill every month since start_date
    tags=["bda", "task1"],
    template_searchpath="/opt/airflow",
) as dag:
    create_source = SQLExecuteQueryOperator(
        task_id="create_source_tables",
        conn_id=DWH_CONN_ID,
        sql="reference/sql/task1/01_ddl_source.sql",
        split_statements=True,
    )

    create_mart = SQLExecuteQueryOperator(
        task_id="create_mart_table",
        conn_id=DWH_CONN_ID,
        sql="reference/sql/task1/02_ddl_mart.sql",
        split_statements=True,
    )

    build = PythonOperator(task_id="load_mart", python_callable=load_mart)
    check = PythonOperator(task_id="check_mart", python_callable=check_mart)

    create_source >> create_mart >> build >> check
