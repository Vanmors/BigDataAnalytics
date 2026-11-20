CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.people (
    person_id        INT NOT NULL,
    name             VARCHAR(200),
    date_of_birthday DATE,
    country          VARCHAR(100),
    phone            VARCHAR(30),
    email            VARCHAR(200),
    load_dt          TIMESTAMP NOT NULL DEFAULT now(),
    load_src         VARCHAR(50) NOT NULL DEFAULT 'people.csv'
)
ORDER BY person_id;

CREATE TABLE IF NOT EXISTS stg.groups (
    group_id     INT NOT NULL,
    title        VARCHAR(200),
    status       VARCHAR(20),
    created_date DATE,
    owner_id     INT,
    load_dt      TIMESTAMP NOT NULL DEFAULT now(),
    load_src     VARCHAR(50) NOT NULL DEFAULT 'groups.csv'
)
ORDER BY group_id;

CREATE TABLE IF NOT EXISTS stg.messages (
    msg_id   INT NOT NULL,
    msg_time TIMESTAMP NOT NULL,
    msg_from INT NOT NULL,
    msg_to   INT,
    group_id INT,
    text_msg VARCHAR(1000),
    status   VARCHAR(20),
    load_dt  TIMESTAMP NOT NULL DEFAULT now(),
    load_src VARCHAR(50) NOT NULL DEFAULT 'messages.csv'
)
ORDER BY msg_time, msg_id;
