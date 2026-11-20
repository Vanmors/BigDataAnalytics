CREATE SCHEMA IF NOT EXISTS stg_airflow;

CREATE TABLE IF NOT EXISTS stg_airflow.dag (
    dag_id                VARCHAR(250) NOT NULL,
    is_paused             BOOLEAN,
    is_active             BOOLEAN,
    schedule_interval     VARCHAR(250),
    timetable_description VARCHAR(1000),
    next_dagrun           TIMESTAMP,
    last_parsed_time      TIMESTAMP,
    fileloc               VARCHAR(2000),
    load_dt               TIMESTAMP NOT NULL DEFAULT now()
)
ORDER BY dag_id;

CREATE TABLE IF NOT EXISTS stg_airflow.dag_run (
    id                  INT NOT NULL,
    dag_id              VARCHAR(250) NOT NULL,
    run_id              VARCHAR(250) NOT NULL,
    run_type            VARCHAR(50),
    state               VARCHAR(50),
    execution_date      TIMESTAMP,
    data_interval_start TIMESTAMP,
    data_interval_end   TIMESTAMP,
    queued_at           TIMESTAMP,
    start_date          TIMESTAMP,
    end_date            TIMESTAMP,
    load_dt             TIMESTAMP NOT NULL DEFAULT now()
)
ORDER BY dag_id, id;

CREATE TABLE IF NOT EXISTS stg_airflow.task_instance (
    dag_id      VARCHAR(250) NOT NULL,
    task_id     VARCHAR(250) NOT NULL,
    run_id      VARCHAR(250) NOT NULL,
    map_index   INT,
    state       VARCHAR(50),
    try_number  INT,
    max_tries   INT,
    operator    VARCHAR(1000),
    queued_dttm TIMESTAMP,
    start_date  TIMESTAMP,
    end_date    TIMESTAMP,
    duration    FLOAT,
    load_dt     TIMESTAMP NOT NULL DEFAULT now()
)
ORDER BY dag_id, task_id, run_id;

-- error_message is the last line of the stack trace.
CREATE TABLE IF NOT EXISTS stg_airflow.import_error (
    id            INT NOT NULL,
    filename      VARCHAR(2000),
    error_time    TIMESTAMP,
    error_message VARCHAR(2000),
    load_dt       TIMESTAMP NOT NULL DEFAULT now()
)
ORDER BY id;

-- Reference list of DAGs that must be registered, from data/airflow/expected_schedules.csv.
CREATE TABLE IF NOT EXISTS stg_airflow.expected_schedules (
    dag_id            VARCHAR(250) NOT NULL,
    expected_schedule VARCHAR(250),
    file_name         VARCHAR(250),
    load_dt           TIMESTAMP NOT NULL DEFAULT now()
)
ORDER BY dag_id;

-- CRON checks computed in Python with croniter.
CREATE TABLE IF NOT EXISTS stg_airflow.dag_cron_check (
    dag_id      VARCHAR(250) NOT NULL,
    cron_valid  BOOLEAN,
    missed_runs INT,
    load_dt     TIMESTAMP NOT NULL DEFAULT now()
)
ORDER BY dag_id;
