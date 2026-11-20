DELETE FROM cdm.dm_messages_daily;

INSERT INTO cdm.dm_messages_daily (
    msg_date, weekday_num, weekday_name, is_weekend, messages_total, private_messages, group_messages, dau,
    peak_hour, load_date
)
WITH daily AS (
    SELECT msg_time::DATE AS msg_date,
           count(*) AS messages_total,
           sum(CASE WHEN group_id IS NULL THEN 1 ELSE 0 END) AS private_messages,
           count(group_id) AS group_messages,
           count(DISTINCT author_id) AS dau
    FROM cdm.v_messages
    GROUP BY msg_time::DATE
),
hourly AS (
    SELECT msg_time::DATE AS msg_date, hour(msg_time) AS msg_hour, count(*) AS messages
    FROM cdm.v_messages
    GROUP BY msg_time::DATE, hour(msg_time)
),
peak AS (
    SELECT msg_date, msg_hour FROM hourly
    LIMIT 1 OVER (PARTITION BY msg_date ORDER BY messages DESC, msg_hour)
)
SELECT d.msg_date, dayofweek_iso(d.msg_date), to_char(d.msg_date, 'Dy'), dayofweek_iso(d.msg_date) >= 6,
       d.messages_total, d.private_messages, d.group_messages, d.dau, p.msg_hour, now()
FROM daily d
JOIN peak p ON p.msg_date = d.msg_date;
