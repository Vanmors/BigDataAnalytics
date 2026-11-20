CREATE SCHEMA IF NOT EXISTS cdm;

CREATE TABLE IF NOT EXISTS cdm.dm_sys_schedules (
    dag_id                  VARCHAR(250) NOT NULL,
    schedule_interval       VARCHAR(250),
    expected_schedule       VARCHAR(250),
    cron_valid              BOOLEAN,
    expected_not_registered BOOLEAN NOT NULL,
    schedule_mismatch       BOOLEAN NOT NULL,
    is_paused               BOOLEAN,
    import_error            VARCHAR(2000),
    missed_runs             INT,
    last_run_at             TIMESTAMP,
    last_run_state          VARCHAR(50),
    load_date               TIMESTAMP NOT NULL
)
ORDER BY dag_id;

CREATE TABLE IF NOT EXISTS cdm.dm_sys_task_runs (
    dag_id           VARCHAR(250) NOT NULL,
    task_id          VARCHAR(250) NOT NULL,
    last_state       VARCHAR(50),
    last_run_at      TIMESTAMP,
    runs_total       INT NOT NULL,
    success_count    INT NOT NULL,
    failed_count     INT NOT NULL,
    success_rate_pct NUMERIC(5, 1),
    avg_duration_sec NUMERIC(12, 1),
    retries_total    INT NOT NULL,
    is_stuck         BOOLEAN NOT NULL,
    load_date        TIMESTAMP NOT NULL
)
ORDER BY dag_id, task_id;
