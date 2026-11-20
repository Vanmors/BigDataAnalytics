INSERT INTO dds.s_person_info (hk_person_id, name, date_of_birthday, country, phone, email, hashdiff, source, load_date)
SELECT src.hk_person_id, src.name, src.date_of_birthday, src.country, src.phone, src.email, src.hashdiff, src.load_src, now()
FROM (
    SELECT hash(person_id) AS hk_person_id, name, date_of_birthday, country, phone, email, load_src,
           hash(name, date_of_birthday, country, phone, email) AS hashdiff
    FROM stg.people
) src
LEFT JOIN (
    SELECT hk_person_id, hashdiff FROM dds.s_person_info
    LIMIT 1 OVER (PARTITION BY hk_person_id ORDER BY load_date DESC)
) cur ON cur.hk_person_id = src.hk_person_id
WHERE cur.hk_person_id IS NULL OR cur.hashdiff <> src.hashdiff;

INSERT INTO dds.s_group_info (hk_group_id, title, status, hashdiff, source, load_date)
SELECT src.hk_group_id, src.title, src.status, src.hashdiff, src.load_src, now()
FROM (
    SELECT hash(group_id) AS hk_group_id, title, status, load_src, hash(title, status) AS hashdiff
    FROM stg.groups
) src
LEFT JOIN (
    SELECT hk_group_id, hashdiff FROM dds.s_group_info
    LIMIT 1 OVER (PARTITION BY hk_group_id ORDER BY load_date DESC)
) cur ON cur.hk_group_id = src.hk_group_id
WHERE cur.hk_group_id IS NULL OR cur.hashdiff <> src.hashdiff;

INSERT INTO dds.s_chat_info (hk_msg_id, text_msg, msg_from, msg_to, status, hashdiff, source, load_date)
SELECT src.hk_msg_id, src.text_msg, src.msg_from, src.msg_to, src.status, src.hashdiff, src.load_src, now()
FROM (
    SELECT hash(msg_id) AS hk_msg_id, text_msg, msg_from, msg_to, status, load_src,
           hash(text_msg, msg_from, msg_to, status) AS hashdiff
    FROM stg.messages
) src
LEFT JOIN (
    SELECT hk_msg_id, hashdiff FROM dds.s_chat_info
    LIMIT 1 OVER (PARTITION BY hk_msg_id ORDER BY load_date DESC)
) cur ON cur.hk_msg_id = src.hk_msg_id
WHERE cur.hk_msg_id IS NULL OR cur.hashdiff <> src.hashdiff;
