DELETE FROM cdm.dm_sys_schedules;

INSERT INTO cdm.dm_sys_schedules (
    dag_id, schedule_interval, expected_schedule, cron_valid, expected_not_registered, schedule_mismatch,
    is_paused, import_error, missed_runs, last_run_at, last_run_state, load_date
)
WITH dag_keys AS (
    SELECT dag_id FROM stg_airflow.dag
    UNION
    SELECT dag_id FROM stg_airflow.expected_schedules
),
import_errors AS (
    SELECT regexp_substr(filename, '[^/]+$') AS file_name, max(error_message) AS error_message
    FROM stg_airflow.import_error
    GROUP BY regexp_substr(filename, '[^/]+$')
),
last_runs AS (
    SELECT dag_id, coalesce(start_date, queued_at) AS run_at, state
    FROM stg_airflow.dag_run
    WHERE NOT (dag_id = '{{ dag.dag_id }}' AND run_id = '{{ run_id }}')
    LIMIT 1 OVER (PARTITION BY dag_id ORDER BY coalesce(start_date, queued_at) DESC NULLS LAST)
),
dags AS (
    SELECT k.dag_id,
           coalesce(regexp_substr(d.fileloc, '[^/]+$'), e.file_name) AS file_name,
           d.dag_id IS NOT NULL AND d.is_active AS is_registered,
           e.dag_id IS NOT NULL AS is_expected,
           d.schedule_interval, d.is_paused, e.expected_schedule
    FROM dag_keys k
    LEFT JOIN stg_airflow.dag d ON d.dag_id = k.dag_id
    LEFT JOIN stg_airflow.expected_schedules e ON e.dag_id = k.dag_id
)
SELECT g.dag_id, g.schedule_interval, g.expected_schedule, c.cron_valid,
       g.is_expected AND NOT g.is_registered,
       g.is_expected AND g.is_registered AND coalesce(g.schedule_interval, '') <> coalesce(g.expected_schedule, ''),
       g.is_paused, ie.error_message, c.missed_runs, r.run_at, r.state,
       now()
FROM dags g
LEFT JOIN stg_airflow.dag_cron_check c ON c.dag_id = g.dag_id
LEFT JOIN import_errors ie ON ie.file_name = g.file_name
LEFT JOIN last_runs r ON r.dag_id = g.dag_id;
