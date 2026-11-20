DELETE FROM cdm.dm_demography;

INSERT INTO cdm.dm_demography (
    country, age_bucket, users_total, users_active, messages_total, avg_messages_per_user, report_date, load_date
)
WITH report AS (
    SELECT max(msg_time)::DATE AS report_date FROM cdm.v_messages
),
sent AS (
    SELECT author_id AS person_id, count(*) AS messages_sent
    FROM cdm.v_messages
    GROUP BY author_id
),
people AS (
    SELECT coalesce(p.country, 'unknown') AS country,
           floor(months_between(report.report_date, p.date_of_birthday) / 12) AS age,
           coalesce(s.messages_sent, 0) AS messages_sent,
           report.report_date
    FROM cdm.v_people p
    CROSS JOIN report
    LEFT JOIN sent s ON s.person_id = p.person_id
),
bucketed AS (
    SELECT country, messages_sent, report_date,
           CASE
               WHEN age IS NULL THEN 'unknown'
               WHEN age < 18 THEN '<18'
               WHEN age < 25 THEN '18-24'
               WHEN age < 35 THEN '25-34'
               WHEN age < 45 THEN '35-44'
               WHEN age < 55 THEN '45-54'
               ELSE '55+'
           END AS age_bucket
    FROM people
)
SELECT country, age_bucket,
       count(*),
       sum(CASE WHEN messages_sent > 0 THEN 1 ELSE 0 END),
       sum(messages_sent),
       round(avg(messages_sent), 2),
       max(report_date), now()
FROM bucketed
GROUP BY country, age_bucket;
