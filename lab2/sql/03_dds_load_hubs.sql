INSERT INTO dds.h_people (hk_person_id, person_id, source, load_date)
SELECT hash(p.person_id), p.person_id, p.load_src, now()
FROM stg.people p
WHERE NOT EXISTS (SELECT 1 FROM dds.h_people h WHERE h.hk_person_id = hash(p.person_id));

INSERT INTO dds.h_groups (hk_group_id, group_id, created_date, source, load_date)
SELECT hash(g.group_id), g.group_id, g.created_date, g.load_src, now()
FROM stg.groups g
WHERE NOT EXISTS (SELECT 1 FROM dds.h_groups h WHERE h.hk_group_id = hash(g.group_id));

INSERT INTO dds.h_chats (hk_msg_id, msg_id, msg_time, source, load_date)
SELECT hash(m.msg_id), m.msg_id, m.msg_time, m.load_src, now()
FROM stg.messages m
WHERE NOT EXISTS (SELECT 1 FROM dds.h_chats h WHERE h.hk_msg_id = hash(m.msg_id));
