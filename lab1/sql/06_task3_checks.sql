-- Проверки/запросы только для чтения. Счётчики не заменяют byte-fidelity тесты.
SELECT table_schema,table_name,operation,count(*) FROM task3.logs
GROUP BY table_schema,table_name,operation ORDER BY 1,2,3;
SELECT status,epoch,reader_version,projection_version,last_commit_at,last_empty_at,failure
FROM task3.mongo_state;
SELECT kind,projection_status,count(*) FROM task3.mongo_events GROUP BY 1,2 ORDER BY 1,2;
SELECT collection,count(*) FILTER (WHERE projection_status='unavailable') AS unavailable
FROM task3.mongo_latest GROUP BY 1;

SELECT document_id,document->>'name' AS name
FROM task3.mongo_latest WHERE collection='clients';
SELECT document_id,document #>> '{client,id,$oid}' AS client_id
FROM task3.mongo_latest WHERE collection='orders';
SELECT o.document_id AS order_id,c.document->>'name' AS client_name,
       o.document->>'cost' AS source_cost
FROM task3.mongo_latest o JOIN task3.mongo_latest c
ON c.collection='clients' AND c.document_id=o.document #>> '{client,id,$oid}'
WHERE o.collection='orders';
-- Удаление хранит before-image в document; обе full images остальных операций — в raw_bson.
SELECT epoch,collection,document_id,document,projection_status,projection_reason
FROM task3.mongo_events WHERE kind='change' AND operation='delete' ORDER BY id;
