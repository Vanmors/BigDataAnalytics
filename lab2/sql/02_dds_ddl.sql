CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.h_people (
    hk_person_id INT NOT NULL PRIMARY KEY,
    person_id    INT NOT NULL,
    source       VARCHAR(50) NOT NULL,
    load_date    TIMESTAMP NOT NULL
)
ORDER BY hk_person_id;

CREATE TABLE IF NOT EXISTS dds.h_groups (
    hk_group_id  INT NOT NULL PRIMARY KEY,
    group_id     INT NOT NULL,
    created_date DATE,
    source       VARCHAR(50) NOT NULL,
    load_date    TIMESTAMP NOT NULL
)
ORDER BY hk_group_id;

CREATE TABLE IF NOT EXISTS dds.h_chats (
    hk_msg_id INT NOT NULL PRIMARY KEY,
    msg_id    INT NOT NULL,
    msg_time  TIMESTAMP NOT NULL,
    source    VARCHAR(50) NOT NULL,
    load_date TIMESTAMP NOT NULL
)
ORDER BY hk_msg_id;

CREATE TABLE IF NOT EXISTS dds.l_chat_owners (
    hk_l_owner_id INT NOT NULL PRIMARY KEY,
    hk_person_id  INT NOT NULL REFERENCES dds.h_people (hk_person_id),
    hk_group_id   INT NOT NULL REFERENCES dds.h_groups (hk_group_id),
    source        VARCHAR(50) NOT NULL,
    load_date     TIMESTAMP NOT NULL
)
ORDER BY hk_l_owner_id;

CREATE TABLE IF NOT EXISTS dds.l_person_chat (
    hk_l_person_chat INT NOT NULL PRIMARY KEY,
    hk_person_id     INT NOT NULL REFERENCES dds.h_people (hk_person_id),
    hk_msg_id        INT NOT NULL REFERENCES dds.h_chats (hk_msg_id),
    source           VARCHAR(50) NOT NULL,
    load_date        TIMESTAMP NOT NULL
)
ORDER BY hk_l_person_chat;

CREATE TABLE IF NOT EXISTS dds.l_groups (
    hk_l_groups_id INT NOT NULL PRIMARY KEY,
    hk_msg_id      INT NOT NULL REFERENCES dds.h_chats (hk_msg_id),
    hk_group_id    INT NOT NULL REFERENCES dds.h_groups (hk_group_id),
    source         VARCHAR(50) NOT NULL,
    load_date      TIMESTAMP NOT NULL
)
ORDER BY hk_l_groups_id;

CREATE TABLE IF NOT EXISTS dds.s_person_info (
    hk_person_id     INT NOT NULL REFERENCES dds.h_people (hk_person_id),
    name             VARCHAR(200),
    date_of_birthday DATE,
    country          VARCHAR(100),
    phone            VARCHAR(30),
    email            VARCHAR(200),
    hashdiff         INT NOT NULL,
    source           VARCHAR(50) NOT NULL,
    load_date        TIMESTAMP NOT NULL
)
ORDER BY hk_person_id, load_date;

CREATE TABLE IF NOT EXISTS dds.s_group_info (
    hk_group_id INT NOT NULL REFERENCES dds.h_groups (hk_group_id),
    title       VARCHAR(200),
    status      VARCHAR(20),
    hashdiff    INT NOT NULL,
    source      VARCHAR(50) NOT NULL,
    load_date   TIMESTAMP NOT NULL
)
ORDER BY hk_group_id, load_date;

CREATE TABLE IF NOT EXISTS dds.s_chat_info (
    hk_msg_id INT NOT NULL REFERENCES dds.h_chats (hk_msg_id),
    text_msg  VARCHAR(1000),
    msg_from  INT,
    msg_to    INT,
    status    VARCHAR(20),
    hashdiff  INT NOT NULL,
    source    VARCHAR(50) NOT NULL,
    load_date TIMESTAMP NOT NULL
)
ORDER BY hk_msg_id, load_date;
