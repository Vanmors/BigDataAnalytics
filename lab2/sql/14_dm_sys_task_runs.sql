DELETE FROM cdm.dm_sys_task_runs;

INSERT INTO cdm.dm_sys_task_runs (
    dag_id, task_id, last_state, last_run_at, runs_total, success_count, failed_count, success_rate_pct,
    avg_duration_sec, retries_total, is_stuck, load_date
)
WITH snapshot AS (
    SELECT max(load_dt) AS snapshot_time FROM stg_airflow.task_instance
),
instances AS (
    SELECT ti.dag_id, ti.task_id, ti.state, ti.try_number, ti.start_date, ti.duration,
           coalesce(ti.start_date, ti.queued_dttm, dr.start_date) AS run_at,
           ti.state = 'running' AND datediff('minute', ti.start_date, s.snapshot_time) >= {{ params.stuck_after_minutes }}
               AS is_stuck
    FROM stg_airflow.task_instance ti
    JOIN stg_airflow.dag_run dr ON dr.dag_id = ti.dag_id AND dr.run_id = ti.run_id
    CROSS JOIN snapshot s
),
last_states AS (
    SELECT dag_id, task_id, state, run_at
    FROM instances
    LIMIT 1 OVER (PARTITION BY dag_id, task_id ORDER BY start_date DESC NULLS LAST, run_at DESC NULLS LAST)
),
stats AS (
    SELECT dag_id, task_id,
           count(*) AS runs_total,
           sum(CASE WHEN state = 'success' THEN 1 ELSE 0 END) AS success_count,
           sum(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) AS failed_count,
           avg(CASE WHEN state IN ('success', 'failed') THEN duration END) AS avg_duration_sec,
           sum(CASE WHEN try_number > 1 THEN try_number - 1 ELSE 0 END) AS retries_total,
           max(CASE WHEN is_stuck THEN 1 ELSE 0 END) = 1 AS is_stuck
    FROM instances
    GROUP BY dag_id, task_id
)
SELECT s.dag_id, s.task_id, l.state, l.run_at,
       s.runs_total, s.success_count, s.failed_count,
       round(100 * s.success_count / nullif(s.success_count + s.failed_count, 0), 1),
       round(s.avg_duration_sec, 1), s.retries_total, s.is_stuck,
       now()
FROM stats s
JOIN last_states l ON l.dag_id = s.dag_id AND l.task_id = s.task_id;
