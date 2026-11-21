from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from task3_mongo_cdc import airflow_run

with DAG(
    dag_id='task3_mongo_cdc',
    start_date=pendulum.datetime(2024, 1, 1, tz='UTC'),
    schedule='* * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['task3'],
) as dag:
    consume = PythonOperator(
        task_id='consume',
        python_callable=airflow_run,
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(seconds=120),
        do_xcom_push=False,
    )
