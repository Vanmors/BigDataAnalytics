BEGIN;
SELECT pg_advisory_xact_lock(3141592653589793);
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname='task3')
       AND obj_description(to_regnamespace('task3'), 'pg_namespace') IS DISTINCT FROM 'task3-v1' THEN
        RAISE EXCEPTION 'Incompatible task3 schema; explicit migration required';
    END IF;
END $$;
CREATE SCHEMA IF NOT EXISTS task3;
DO $$
DECLARE relation text;
BEGIN
    IF obj_description(to_regnamespace('task3'), 'pg_namespace') IS NOT NULL THEN
        FOREACH relation IN ARRAY ARRAY['logs','mongo_events','mongo_latest','mongo_state','mongo_baseline_key','mongo_epoch_marker'] LOOP
            IF to_regclass('task3.'||relation) IS NULL THEN RAISE EXCEPTION 'Missing task3.%', relation; END IF;
        END LOOP;
    END IF;
END $$;
CREATE TABLE IF NOT EXISTS task3.logs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    changed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    table_schema text NOT NULL,
    table_name text NOT NULL,
    operation text NOT NULL CHECK (operation IN ('INSERT','UPDATE','DELETE','TRUNCATE')),
    old_data jsonb,
    new_data jsonb
);
CREATE TABLE IF NOT EXISTS task3.mongo_events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    epoch bigint NOT NULL,
    kind text NOT NULL CHECK (kind IN ('epoch','baseline','change','gap')),
    collection text CHECK (collection IN ('clients','orders','restaurant')),
    document_id text CHECK (document_id ~ '^[0-9a-f]{24}$'),
    operation text,
    token bytea UNIQUE,
    cluster_seconds bigint,
    cluster_increment bigint,
    raw_bson bytea,
    document jsonb,
    projection_status text NOT NULL CHECK (projection_status IN ('available','unavailable','metadata')),
    projection_reason text CHECK (length(projection_reason) <= 200),
    metadata jsonb,
    ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((projection_status='available' AND document IS NOT NULL AND projection_reason IS NULL)
        OR (projection_status='unavailable' AND document IS NULL AND projection_reason IS NOT NULL)
        OR (projection_status='metadata' AND document IS NULL AND projection_reason IS NULL)),
    CHECK ((kind IN ('baseline','change') AND raw_bson IS NOT NULL AND collection IS NOT NULL AND document_id IS NOT NULL AND projection_status <> 'metadata')
        OR (kind IN ('epoch','gap') AND raw_bson IS NULL AND token IS NULL AND projection_status='metadata')),
    CHECK (kind <> 'change' OR (token IS NOT NULL AND cluster_seconds IS NOT NULL AND cluster_increment IS NOT NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS mongo_baseline_key ON task3.mongo_events(epoch,collection,document_id) WHERE kind='baseline';
CREATE UNIQUE INDEX IF NOT EXISTS mongo_epoch_marker ON task3.mongo_events(epoch,kind) WHERE kind IN ('epoch','gap');
CREATE TABLE IF NOT EXISTS task3.mongo_latest (
    collection text NOT NULL CHECK (collection IN ('clients','orders','restaurant')),
    document_id text NOT NULL CHECK (document_id ~ '^[0-9a-f]{24}$'),
    epoch bigint NOT NULL,
    raw_bson bytea NOT NULL,
    document jsonb,
    projection_status text NOT NULL CHECK (projection_status IN ('available','unavailable')),
    projection_reason text CHECK (length(projection_reason) <= 200),
    PRIMARY KEY (collection,document_id),
    CHECK ((projection_status='available' AND document IS NOT NULL AND projection_reason IS NULL)
        OR (projection_status='unavailable' AND document IS NULL AND projection_reason IS NOT NULL))
);
CREATE TABLE IF NOT EXISTS task3.mongo_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    status text NOT NULL CHECK (status IN ('uninitialized','active','needs_manual_rebaseline')),
    identity jsonb,
    reader_version text NOT NULL DEFAULT 'raw-required-v1',
    projection_version text NOT NULL DEFAULT 'relaxed-ejson-v1',
    epoch bigint NOT NULL DEFAULT 0,
    resume_token bytea,
    last_commit_at timestamptz,
    last_empty_at timestamptz,
    failure jsonb,
    CHECK ((status='uninitialized' AND identity IS NULL AND epoch=0 AND resume_token IS NULL)
        OR (status<>'uninitialized' AND identity IS NOT NULL AND epoch>0 AND resume_token IS NOT NULL))
);
-- Refuse an altered table/index/constraint on rerun instead of silently adopting it.
DO $$
DECLARE t text; signature text; saved text;
BEGIN
    FOREACH t IN ARRAY ARRAY['logs','mongo_events','mongo_latest','mongo_state'] LOOP
        SELECT md5(concat(
            (SELECT string_agg(a.attname || ':' || format_type(a.atttypid,a.atttypmod) || ':' || a.attnotnull::text || ':' || a.attidentity::text || ':' || a.attgenerated::text || ':' || COALESCE(pg_get_expr(d.adbin,d.adrelid),''), ',' ORDER BY a.attnum) FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid=('task3.'||t)::regclass AND a.attnum>0 AND NOT a.attisdropped),
            (SELECT string_agg(pg_get_constraintdef(oid), ',' ORDER BY conname) FROM pg_constraint WHERE conrelid=('task3.'||t)::regclass),
            (SELECT string_agg(pg_get_indexdef(indexrelid), ',' ORDER BY indexrelid::regclass::text) FROM pg_index WHERE indrelid=('task3.'||t)::regclass)
        )) INTO signature;
        SELECT obj_description(('task3.'||t)::regclass, 'pg_class') INTO saved;
        IF saved IS NOT NULL AND saved <> signature THEN RAISE EXCEPTION 'Incompatible task3.%', t; END IF;
        IF saved IS NULL AND obj_description('task3'::regnamespace, 'pg_namespace') IS NOT NULL THEN RAISE EXCEPTION 'Unrecognized task3.%', t; END IF;
        EXECUTE format('COMMENT ON TABLE task3.%I IS %L',t,signature);
    END LOOP;
END $$;
INSERT INTO task3.mongo_state(singleton,status) VALUES (true,'uninitialized') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION task3.audit_source() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO task3.logs(table_schema,table_name,operation,old_data,new_data)
    VALUES (TG_TABLE_SCHEMA,TG_TABLE_NAME,TG_OP,
        CASE WHEN TG_OP IN ('UPDATE','DELETE') THEN to_jsonb(OLD) END,
        CASE WHEN TG_OP IN ('INSERT','UPDATE') THEN to_jsonb(NEW) END);
    RETURN NULL;
END $$;
DO $$
DECLARE target text;
BEGIN
    FOREACH target IN ARRAY ARRAY[
        'source1.craft_market_wide','source2.craft_market_masters_products',
        'source2.craft_market_orders_customers','source3.craft_market_craftsmans',
        'source3.craft_market_customers','source3.craft_market_orders'
    ] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS task3_row_audit ON %s', target);
        EXECUTE format('CREATE TRIGGER task3_row_audit AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION task3.audit_source()', target);
        EXECUTE format('DROP TRIGGER IF EXISTS task3_truncate_audit ON %s', target);
        EXECUTE format('CREATE TRIGGER task3_truncate_audit AFTER TRUNCATE ON %s FOR EACH STATEMENT EXECUTE FUNCTION task3.audit_source()', target);
    END LOOP;
END $$;
COMMENT ON SCHEMA task3 IS 'task3-v1';
COMMIT;
