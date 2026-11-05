import logging
import time
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

DWH_CONN_ID = "dwh_postgres"
SQL_DIR = Path("/opt/airflow/sql")
MART_TABLE = "mart.craft_market_customer_report"

MART_COLUMNS = (
    "customer_id, customer_name, customer_address, customer_birthday, customer_email, "
    "customer_spent, platform_money, count_order, avg_price_order, median_time_order_completed, "
    "top_product_category, top_craftsman_id, count_order_created, count_order_in_progress, "
    "count_order_delivery, count_order_done, count_order_not_done, report_period"
)

log = logging.getLogger(__name__)


def load_mart(logical_date=None, **_) -> None:
    started = time.monotonic()
    year, month = logical_date.year, logical_date.month
    period = f"{year}-{month:02d}"

    query = (SQL_DIR / "02_query.sql").read_text(encoding="utf-8")
    query = query.replace(":report_year", str(year)).replace(":report_month", str(month))
    query = query.strip().rstrip(";")

    hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    conn = hook.get_connection(DWH_CONN_ID)
    source = (f"postgres://{conn.host}:{conn.port}/{conn.schema} "
              f"source3.craft_market_orders + source3.craft_market_customers, orders created in {period}")
    target = f"postgres://{conn.host}:{conn.port}/{conn.schema} {MART_TABLE}, report_period = {period}"
    source_orders = hook.get_first(
        "SELECT count(*) FROM source3.craft_market_orders "
        "WHERE to_char(order_created_date, 'YYYY-MM') = %s",
        parameters=(period,),
    )[0]
    log.info("Load started: %s -> %s, %d source orders", source, target, source_orders)
    # The mart is built by one INSERT ... SELECT, so only the final row count is known.
    hook.run(
        [
            f"DELETE FROM {MART_TABLE} WHERE report_period = '{period}'",
            f"INSERT INTO {MART_TABLE} ({MART_COLUMNS}) SELECT {MART_COLUMNS} FROM ({query}) q",
        ]
    )

    rows = hook.get_first(
        f"SELECT count(*) FROM {MART_TABLE} WHERE report_period = %s",
        parameters=(period,),
    )[0]
    log.info("Load finished in %.1f s: %s -> %s, %d rows", time.monotonic() - started, source, target, rows)


with DAG(
    dag_id="task1_customer_report_mart",
    description="Task 1: build mart.craft_market_customer_report per reporting month",
    start_date=datetime(2024, 1, 1),
    schedule="@monthly",
    catchup=False,
    tags=["bda", "task1"],
    template_searchpath="/opt/airflow",
) as dag:
    create_source = SQLExecuteQueryOperator(
        task_id="create_source_tables",
        conn_id=DWH_CONN_ID,
        sql="sql/01_ddl_craft_market.sql",
        split_statements=True,
    )

    create_mart = SQLExecuteQueryOperator(
        task_id="create_mart_table",
        conn_id=DWH_CONN_ID,
        sql="sql/03_ddl_mart.sql",
        split_statements=True,
    )

    build = PythonOperator(task_id="load_mart", python_callable=load_mart)

    create_source >> create_mart >> build
