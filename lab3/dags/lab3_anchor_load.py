from contextlib import closing
from datetime import date
import logging

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)


@dag(
    dag_id="lab3_anchor_load",
    description="Lab 3 · Anchor Modeling / SCD Type 2",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["lab3", "anchor", "scd2"],
)
def daily_anchor_load():
    @task
    def apply_source_day():
        context = get_current_context()
        source_day = context["data_interval_start"].date()
        if "load_date" in context["dag_run"].conf:
            source_day = date.fromisoformat(context["dag_run"].conf["load_date"])

        hook = PostgresHook(postgres_conn_id="lab3_warehouse")
        with closing(hook.get_conn()) as conn, conn, conn.cursor() as cursor:
            cursor.execute("SELECT anchor.load_day(%s)", (source_day,))
            (result,) = cursor.fetchone()
            for notice in conn.notices:
                log.info("%s", notice.strip())

        log.info(
            "Source day %s: %s · %d new keys · %d new versions",
            source_day, result["state"], result["anchors"], result["versions"],
        )
        return result

    apply_source_day()


daily_anchor_load()
