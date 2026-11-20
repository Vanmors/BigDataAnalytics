import csv
import io
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.vertica.hooks.vertica import VerticaHook
from croniter import croniter

DWH_CONN_ID = "vertica_dwh"
METADATA_CONN_ID = "airflow_metadata"
EXPECTED_SCHEDULES_FILE = Path("/opt/airflow/data/airflow/expected_schedules.csv")
STG_SCHEMA = "stg_airflow"

LOOKBACK = timedelta(days=7)
SCHEDULING_GRACE = timedelta(minutes=5)
STUCK_AFTER_MINUTES = 10

UTC = "AT TIME ZONE 'UTC'"
METADATA_EXTRACTS = {
    "dag": (
        f"""SELECT dag_id, is_paused, is_active, schedule_interval::json #>> '{{}}', timetable_description,
                   next_dagrun {UTC}, last_parsed_time {UTC}, fileloc
            FROM dag""",
        ["dag_id", "is_paused", "is_active", "schedule_interval", "timetable_description",
         "next_dagrun", "last_parsed_time", "fileloc"],
    ),
    "dag_run": (
        f"""SELECT id, dag_id, run_id, run_type, state, execution_date {UTC}, data_interval_start {UTC},
                   data_interval_end {UTC}, queued_at {UTC}, start_date {UTC}, end_date {UTC}
            FROM dag_run""",
        ["id", "dag_id", "run_id", "run_type", "state", "execution_date", "data_interval_start",
         "data_interval_end", "queued_at", "start_date", "end_date"],
    ),
    "task_instance": (
        f"""SELECT dag_id, task_id, run_id, map_index, state, try_number, max_tries, operator,
                   queued_dttm {UTC}, start_date {UTC}, end_date {UTC}, duration
            FROM task_instance""",
        ["dag_id", "task_id", "run_id", "map_index", "state", "try_number", "max_tries", "operator",
         "queued_dttm", "start_date", "end_date", "duration"],
    ),
    "import_error": (
        f"""SELECT id, filename, timestamp {UTC},
                   left(reverse(split_part(reverse(rtrim(stacktrace, E' \\n')), E'\\n', 1)), 2000)
            FROM import_error""",
        ["id", "filename", "error_time", "error_message"],
    ),
}

log = logging.getLogger(__name__)


def replace_table(target: str, columns: list[str], rows: list[tuple]) -> None:
    buffer = io.StringIO()
    csv.writer(buffer).writerows(rows)
    buffer.seek(0)
    copy_sql = (
        f"COPY {target} ({', '.join(columns)}) FROM STDIN "
        "DELIMITER ',' ENCLOSED BY '\"' NULL '' ABORT ON ERROR NO COMMIT"
    )
    conn = VerticaHook(vertica_conn_id=DWH_CONN_ID).get_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {target}")
        cur.copy(copy_sql, buffer)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    log.info("%s: %d rows loaded", target, len(rows))


def extract_metadata_table(table: str) -> None:
    sql, columns = METADATA_EXTRACTS[table]
    rows = PostgresHook(postgres_conn_id=METADATA_CONN_ID).get_records(sql)
    replace_table(f"{STG_SCHEMA}.{table}", columns, rows)


def load_expected_schedules() -> None:
    with EXPECTED_SCHEDULES_FILE.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        columns = next(reader)
        rows = [tuple(row) for row in reader]
    replace_table(f"{STG_SCHEMA}.expected_schedules", columns, rows)


def is_valid_cron(schedule: str | None) -> bool | None:
    return croniter.is_valid(schedule) if schedule else None


def count_runs(schedule: str, run_ends: list[datetime], check_to: datetime) -> tuple[int, int, int]:
    """Compare CRON times with data_interval_end of scheduled runs: a scheduled run for CRON time T ends its
    data interval at T. The check starts from the first scheduled run, as earlier CRON times precede the DAG."""
    check_from = max(min(run_ends), check_to - LOOKBACK)
    expected = set()
    cron = croniter(schedule, check_from - timedelta(seconds=1))
    while (fire_time := cron.get_next(datetime)) <= check_to - SCHEDULING_GRACE:
        expected.add(fire_time)
    actual = {end for end in run_ends if check_from <= end <= check_to}
    return len(expected), len(actual), len(expected - actual)


def check_schedules() -> None:
    hook = VerticaHook(vertica_conn_id=DWH_CONN_ID)
    check_to = hook.get_first(f"SELECT max(load_dt) FROM {STG_SCHEMA}.dag")[0]
    schedules = dict(hook.get_records(f"SELECT dag_id, schedule_interval FROM {STG_SCHEMA}.dag"))
    run_ends: dict[str, list[datetime]] = {}
    for dag_id, end in hook.get_records(
        f"SELECT dag_id, data_interval_end FROM {STG_SCHEMA}.dag_run "
        "WHERE run_type = 'scheduled' AND data_interval_end IS NOT NULL"
    ):
        run_ends.setdefault(dag_id, []).append(end)

    rows = []
    for dag_id, schedule in sorted(schedules.items()):
        cron_valid = is_valid_cron(schedule)
        expected_runs = actual_runs = missed_runs = None
        if cron_valid and dag_id in run_ends:
            expected_runs, actual_runs, missed_runs = count_runs(schedule, run_ends[dag_id], check_to)
        rows.append((dag_id, cron_valid, missed_runs))
        log.info("%s: schedule %r, cron valid %s, expected/actual/missed runs %s/%s/%s",
                 dag_id, schedule, cron_valid, expected_runs, actual_runs, missed_runs)

    replace_table(f"{STG_SCHEMA}.dag_cron_check", ["dag_id", "cron_valid", "missed_runs"], rows)


with DAG(
    dag_id="sn_sys_marts",
    description="Lab 2: copy Airflow metadata into Vertica and build the system marts",
    start_date=datetime(2026, 11, 1),
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    template_searchpath="/opt/airflow",
    tags=["lab2", "system"],
) as dag:
    create_tables = [
        SQLExecuteQueryOperator(task_id=task_id, conn_id=DWH_CONN_ID, sql=sql, split_statements=True)
        for task_id, sql in [
            ("create_stg_airflow_tables", "sql/11_stg_airflow_ddl.sql"),
            ("create_cdm_sys_tables", "sql/12_cdm_sys_ddl.sql"),
        ]
    ]

    extracts = [
        PythonOperator(
            task_id=f"extract_{table}",
            python_callable=extract_metadata_table,
            op_kwargs={"table": table},
        )
        for table in METADATA_EXTRACTS
    ]
    extracts.append(PythonOperator(task_id="load_expected_schedules", python_callable=load_expected_schedules))

    check_schedules_task = PythonOperator(task_id="check_schedules", python_callable=check_schedules)

    build_dm_sys_schedules = SQLExecuteQueryOperator(
        task_id="build_dm_sys_schedules",
        conn_id=DWH_CONN_ID,
        sql="sql/13_dm_sys_schedules.sql",
        split_statements=True,
    )
    build_dm_sys_task_runs = SQLExecuteQueryOperator(
        task_id="build_dm_sys_task_runs",
        conn_id=DWH_CONN_ID,
        sql="sql/14_dm_sys_task_runs.sql",
        split_statements=True,
        params={"stuck_after_minutes": STUCK_AFTER_MINUTES},
    )

    create_tables[0] >> extracts
    create_tables[1] >> [build_dm_sys_schedules, build_dm_sys_task_runs]
    extracts >> check_schedules_task >> build_dm_sys_schedules
    extracts >> build_dm_sys_task_runs
