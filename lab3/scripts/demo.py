import base64
from contextlib import closing
import json
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from airflow.models.xcom import XCom
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "lab3_anchor_load"
DAG_API = f"http://localhost:8080/api/v1/dags/{DAG_ID}"
AUTH = base64.b64encode(b"admin:admin").decode()


def api(path="", method="GET", body=None):
    request = Request(
        DAG_API + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": f"Basic {AUTH}", "Content-Type": "application/json"},
        method=method,
    )
    with urlopen(request, timeout=10) as response:
        return json.load(response)


def run_day(day, *, replay=False):
    label = "replay" if replay else "load"
    run_id = f"demo_{label}__{day}__{time.time_ns()}"
    api("/dagRuns", "POST", {"dag_run_id": run_id, "conf": {"load_date": day.isoformat()}})
    path = f"/dagRuns/{run_id}"
    for _ in range(90):
        state = api(path)["state"]
        if state == "failed":
            raise RuntimeError(f"{run_id} failed. Open its task log in Airflow.")
        if state == "success":
            result = XCom.get_one(dag_id=DAG_ID, task_id="apply_source_day", run_id=run_id, key="return_value")
            expected = ("already_applied",) if replay else ("applied", "already_applied")
            if result is None or result["state"] not in expected:
                raise RuntimeError(f"{run_id} returned unexpected result {result!r}")
            print(f"  {day}  {result['state']:<16}  ключей +{result['anchors']}, версий +{result['versions']}")
            return
        time.sleep(2)
    raise TimeoutError(f"{run_id} did not finish. Open its task log in Airflow.")


def main():
    for _ in range(45):
        try:
            dag = api()
            break
        except HTTPError as error:
            if error.code != 404:
                raise
            time.sleep(2)
    else:
        raise TimeoutError("Airflow did not discover the Lab 3 DAG.")

    hook = PostgresHook(postgres_conn_id="lab3_warehouse")
    with closing(hook.get_conn()) as conn, conn.cursor() as cursor:
        cursor.execute("SELECT load_date FROM stg.load_dates WHERE load_date <= CURRENT_DATE ORDER BY load_date")
        days = [day for (day,) in cursor.fetchall()]
        if not days:
            raise RuntimeError("No staged dates are available.")
        if dag["is_paused"]:
            api(method="PATCH", body={"is_paused": False})
        print("Airflow · загрузка по датам источника")
        for day in days:
            run_day(day)
        run_day(days[-1], replay=True)

        cursor.execute("""
            SELECT valid_from, valid_to, nightly_rate
            FROM report.hotel_price_history WHERE hotel_id = 1 ORDER BY valid_from
        """)
        print("\nЦена за ночь · отель «Кедр»")
        for start, end, price in cursor.fetchall():
            print(f"  {start} — {end or 'действует'}  {price:.2f} ₽")

    print("\nPASS · DAG завершился, повторное чтение дня не добавило версий")
    print("  Grafana  http://127.0.0.1:3003/d/lab3-bookings")
    print("  Airflow  http://127.0.0.1:8082/dags/lab3_anchor_load/grid  (admin / admin)")


if __name__ == "__main__":
    main()
