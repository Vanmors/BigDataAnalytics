DELETE FROM cdm.dm_group_activity;

INSERT INTO cdm.dm_group_activity (
    group_id, title, status, owner_id, owner_name, created_date, group_age_days, messages_total, distinct_authors,
    first_message_date, last_message_date, messages_per_day, days_silent, is_dead, report_date, load_date
)
WITH report AS (
    SELECT max(msg_time)::DATE AS report_date FROM cdm.v_messages
),
stats AS (
    SELECT group_id,
           count(*) AS messages_total,
           count(DISTINCT author_id) AS distinct_authors,
           min(msg_time)::DATE AS first_message_date,
           max(msg_time)::DATE AS last_message_date
    FROM cdm.v_messages
    WHERE group_id IS NOT NULL
    GROUP BY group_id
)
SELECT g.group_id, g.title, g.status, g.owner_id, o.name, g.created_date,
       report.report_date - g.created_date,
       coalesce(s.messages_total, 0),
       coalesce(s.distinct_authors, 0),
       s.first_message_date, s.last_message_date,
       round(s.messages_total / (report.report_date - s.first_message_date + 1), 2),
       report.report_date - coalesce(s.last_message_date, g.created_date),
       report.report_date - coalesce(s.last_message_date, g.created_date) >= {{ params.dead_group_days }},
       report.report_date, now()
FROM cdm.v_groups g
CROSS JOIN report
LEFT JOIN stats s ON s.group_id = g.group_id
LEFT JOIN cdm.v_people o ON o.person_id = g.owner_id;
