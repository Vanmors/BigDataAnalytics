CREATE SCHEMA IF NOT EXISTS cdm;

CREATE TABLE IF NOT EXISTS cdm.dm_user_activity (
    person_id          INT NOT NULL,
    name               VARCHAR(200),
    country            VARCHAR(100),
    messages_sent      INT NOT NULL,
    private_sent       INT NOT NULL,
    group_sent         INT NOT NULL,
    messages_received  INT NOT NULL,
    first_message_date DATE,
    last_message_date  DATE,
    active_days        INT NOT NULL,
    groups_written_to  INT NOT NULL,
    groups_owned       INT NOT NULL,
    report_date        DATE NOT NULL,
    load_date          TIMESTAMP NOT NULL
)
ORDER BY person_id;

CREATE TABLE IF NOT EXISTS cdm.dm_group_activity (
    group_id          INT NOT NULL,
    title             VARCHAR(200),
    status            VARCHAR(20),
    owner_id          INT,
    owner_name        VARCHAR(200),
    created_date      DATE,
    group_age_days    INT,
    messages_total    INT NOT NULL,
    distinct_authors  INT NOT NULL,
    first_message_date DATE,
    last_message_date DATE,
    messages_per_day  NUMERIC(10, 2),
    days_silent       INT,
    is_dead           BOOLEAN NOT NULL,
    report_date       DATE NOT NULL,
    load_date         TIMESTAMP NOT NULL
)
ORDER BY group_id;

CREATE TABLE IF NOT EXISTS cdm.dm_demography (
    country               VARCHAR(100) NOT NULL,
    age_bucket            VARCHAR(10) NOT NULL,
    users_total           INT NOT NULL,
    users_active          INT NOT NULL,
    messages_total        INT NOT NULL,
    avg_messages_per_user NUMERIC(10, 2) NOT NULL,
    report_date           DATE NOT NULL,
    load_date             TIMESTAMP NOT NULL
)
ORDER BY country, age_bucket;

CREATE TABLE IF NOT EXISTS cdm.dm_messages_daily (
    msg_date         DATE NOT NULL,
    weekday_num      INT NOT NULL,
    weekday_name     VARCHAR(3) NOT NULL,
    is_weekend       BOOLEAN NOT NULL,
    messages_total   INT NOT NULL,
    private_messages INT NOT NULL,
    group_messages   INT NOT NULL,
    dau              INT NOT NULL,
    peak_hour        INT NOT NULL,
    load_date        TIMESTAMP NOT NULL
)
ORDER BY msg_date;

CREATE OR REPLACE VIEW cdm.v_people AS
SELECT h.person_id, s.name, s.date_of_birthday, s.country
FROM dds.h_people h
LEFT JOIN (
    SELECT hk_person_id, name, date_of_birthday, country FROM dds.s_person_info
    LIMIT 1 OVER (PARTITION BY hk_person_id ORDER BY load_date DESC)
) s ON s.hk_person_id = h.hk_person_id;

CREATE OR REPLACE VIEW cdm.v_groups AS
SELECT h.group_id, s.title, s.status, h.created_date, o.person_id AS owner_id
FROM dds.h_groups h
LEFT JOIN (
    SELECT hk_group_id, title, status FROM dds.s_group_info
    LIMIT 1 OVER (PARTITION BY hk_group_id ORDER BY load_date DESC)
) s ON s.hk_group_id = h.hk_group_id
LEFT JOIN dds.l_chat_owners l ON l.hk_group_id = h.hk_group_id
LEFT JOIN dds.h_people o ON o.hk_person_id = l.hk_person_id;

CREATE OR REPLACE VIEW cdm.v_messages AS
SELECT h.msg_id, h.msg_time, a.person_id AS author_id, s.msg_to AS receiver_id, g.group_id
FROM dds.h_chats h
JOIN dds.l_person_chat lp ON lp.hk_msg_id = h.hk_msg_id
JOIN dds.h_people a ON a.hk_person_id = lp.hk_person_id
LEFT JOIN (
    SELECT hk_msg_id, msg_to FROM dds.s_chat_info
    LIMIT 1 OVER (PARTITION BY hk_msg_id ORDER BY load_date DESC)
) s ON s.hk_msg_id = h.hk_msg_id
LEFT JOIN dds.l_groups lg ON lg.hk_msg_id = h.hk_msg_id
LEFT JOIN dds.h_groups g ON g.hk_group_id = lg.hk_group_id;
