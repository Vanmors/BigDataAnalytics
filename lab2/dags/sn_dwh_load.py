import csv
import logging
import time
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.vertica.hooks.vertica import VerticaHook

DWH_CONN_ID = "vertica_dwh"
DATA_DIR = Path("/opt/airflow/data/social_network")
STG_SCHEMA = "stg"
DEAD_GROUP_DAYS = 21

STG_TABLES = {
    "people": ("people.csv", ["person_id", "name", "date_of_birthday", "country", "phone", "email"]),
    "groups": ("groups.csv", ["group_id", "title", "status", "created_date", "owner_id"]),
    "messages": ("messages.csv", ["msg_id", "msg_time", "msg_from", "msg_to", "group_id", "text_msg", "status"]),
}

log = logging.getLogger(__name__)


def read_header_and_count(path: Path) -> tuple[list[str], int]:
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        return header, sum(1 for _ in reader)


def load_stg_table(table: str, file_name: str, columns: list[str]) -> None:
    started = time.monotonic()
    path = DATA_DIR / file_name
    target = f"{STG_SCHEMA}.{table}"

    header, file_rows = read_header_and_count(path)
    if header != columns:
        raise ValueError(f"{file_name}: header {header}, expected {columns}")
    log.info("Load started: %s -> %s, %d rows in file", path, target, file_rows)

    copy_sql = (
        f"COPY {target} ({', '.join(columns)}) FROM STDIN "
        "DELIMITER ',' ENCLOSED BY '\"' NULL '' SKIP 1 ABORT ON ERROR NO COMMIT"
    )
    conn = VerticaHook(vertica_conn_id=DWH_CONN_ID).get_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {target}")
        with path.open("rb") as f:
            cur.copy(copy_sql, f)
        cur.execute(f"SELECT count(*) FROM {target}")
        table_rows = cur.fetchone()[0]
        if table_rows != file_rows:
            raise ValueError(f"{target}: {table_rows} rows loaded, {file_rows} rows in {file_name}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    log.info("Load finished in %.1f s: %s -> %s, %d rows", time.monotonic() - started, path, target, table_rows)


with DAG(
    dag_id="sn_dwh_load",
    description="Lab 2: load social network CSV files into Vertica: stg layer, Data Vault (dds), analytic marts (cdm)",
    start_date=datetime(2026, 11, 1),
    schedule="0 3 * * *",
    catchup=False,
    template_searchpath="/opt/airflow",
    tags=["lab2", "stg", "dds", "cdm"],
) as dag:
    create_stg_tables = SQLExecuteQueryOperator(
        task_id="create_stg_tables",
        conn_id=DWH_CONN_ID,
        sql="sql/01_stg_ddl.sql",
        split_statements=True,
    )

    load_stg_tables = [
        PythonOperator(
            task_id=f"load_stg_{table}",
            python_callable=load_stg_table,
            op_kwargs={"table": table, "file_name": file_name, "columns": columns},
        )
        for table, (file_name, columns) in STG_TABLES.items()
    ]

    create_dds_tables, load_dds_hubs, load_dds_links, load_dds_satellites = (
        SQLExecuteQueryOperator(task_id=task_id, conn_id=DWH_CONN_ID, sql=sql, split_statements=True)
        for task_id, sql in [
            ("create_dds_tables", "sql/02_dds_ddl.sql"),
            ("load_dds_hubs", "sql/03_dds_load_hubs.sql"),
            ("load_dds_links", "sql/04_dds_load_links.sql"),
            ("load_dds_satellites", "sql/05_dds_load_satellites.sql"),
        ]
    )

    create_cdm_tables = SQLExecuteQueryOperator(
        task_id="create_cdm_tables",
        conn_id=DWH_CONN_ID,
        sql="sql/06_cdm_ddl.sql",
        split_statements=True,
    )

    build_marts = [
        SQLExecuteQueryOperator(
            task_id=f"build_{mart}",
            conn_id=DWH_CONN_ID,
            sql=f"sql/{sql_file}",
            split_statements=True,
            params={"dead_group_days": DEAD_GROUP_DAYS},
        )
        for mart, sql_file in [
            ("dm_user_activity", "07_dm_user_activity.sql"),
            ("dm_group_activity", "08_dm_group_activity.sql"),
            ("dm_demography", "09_dm_demography.sql"),
            ("dm_messages_daily", "10_dm_messages_daily.sql"),
        ]
    ]

    create_stg_tables >> load_stg_tables >> create_dds_tables >> load_dds_hubs >> [load_dds_links, load_dds_satellites]
    [load_dds_links, load_dds_satellites] >> create_cdm_tables >> build_marts
