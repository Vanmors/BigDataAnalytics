DELETE FROM cdm.dm_user_activity;

INSERT INTO cdm.dm_user_activity (
    person_id, name, country, messages_sent, private_sent, group_sent, messages_received,
    first_message_date, last_message_date, active_days, groups_written_to, groups_owned, report_date, load_date
)
WITH report AS (
    SELECT max(msg_time)::DATE AS report_date FROM cdm.v_messages
),
sent AS (
    SELECT author_id AS person_id,
           count(*) AS messages_sent,
           sum(CASE WHEN group_id IS NULL THEN 1 ELSE 0 END) AS private_sent,
           count(group_id) AS group_sent,
           min(msg_time)::DATE AS first_message_date,
           max(msg_time)::DATE AS last_message_date,
           count(DISTINCT msg_time::DATE) AS active_days,
           count(DISTINCT group_id) AS groups_written_to
    FROM cdm.v_messages
    GROUP BY author_id
),
received AS (
    SELECT receiver_id AS person_id, count(*) AS messages_received
    FROM cdm.v_messages
    WHERE receiver_id IS NOT NULL
    GROUP BY receiver_id
),
owned AS (
    SELECT owner_id AS person_id, count(*) AS groups_owned
    FROM cdm.v_groups
    GROUP BY owner_id
)
SELECT p.person_id, p.name, p.country,
       coalesce(s.messages_sent, 0), coalesce(s.private_sent, 0), coalesce(s.group_sent, 0),
       coalesce(r.messages_received, 0),
       s.first_message_date, s.last_message_date,
       coalesce(s.active_days, 0), coalesce(s.groups_written_to, 0), coalesce(o.groups_owned, 0),
       report.report_date, now()
FROM cdm.v_people p
CROSS JOIN report
LEFT JOIN sent s ON s.person_id = p.person_id
LEFT JOIN received r ON r.person_id = p.person_id
LEFT JOIN owned o ON o.person_id = p.person_id;
