INSERT INTO dds.l_chat_owners (hk_l_owner_id, hk_person_id, hk_group_id, source, load_date)
SELECT hash(g.owner_id, g.group_id), hash(g.owner_id), hash(g.group_id), g.load_src, now()
FROM stg.groups g
WHERE NOT EXISTS (SELECT 1 FROM dds.l_chat_owners l WHERE l.hk_l_owner_id = hash(g.owner_id, g.group_id));

INSERT INTO dds.l_person_chat (hk_l_person_chat, hk_person_id, hk_msg_id, source, load_date)
SELECT hash(m.msg_from, m.msg_id), hash(m.msg_from), hash(m.msg_id), m.load_src, now()
FROM stg.messages m
WHERE NOT EXISTS (SELECT 1 FROM dds.l_person_chat l WHERE l.hk_l_person_chat = hash(m.msg_from, m.msg_id));

INSERT INTO dds.l_groups (hk_l_groups_id, hk_msg_id, hk_group_id, source, load_date)
SELECT hash(m.msg_id, m.group_id), hash(m.msg_id), hash(m.group_id), m.load_src, now()
FROM stg.messages m
WHERE m.group_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM dds.l_groups l WHERE l.hk_l_groups_id = hash(m.msg_id, m.group_id));
