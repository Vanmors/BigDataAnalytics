BEGIN;

CREATE FUNCTION anchor.load_day(source_day date) RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE
    entity_name text;
    spec record;
    anchor_count integer := 0;
    history_count integer := 0;
    row_count integer;
    old_values text;
    new_values text;
BEGIN
    PERFORM pg_advisory_xact_lock(303, 1);
    IF EXISTS (SELECT FROM anchor.load_batch WHERE load_date = source_day) THEN
        RETURN jsonb_build_object('state', 'already_applied', 'anchors', 0, 'versions', 0);
    END IF;
    IF NOT EXISTS (SELECT FROM stg.load_dates WHERE load_date = source_day) THEN
        RETURN jsonb_build_object('state', 'no_source_rows', 'anchors', 0, 'versions', 0);
    END IF;
    IF EXISTS (SELECT FROM anchor.load_batch WHERE load_date > source_day) THEN
        RAISE EXCEPTION 'Source day % precedes committed history', source_day;
    END IF;
    IF EXISTS (
        SELECT FROM stg.load_dates s WHERE s.load_date < source_day
        AND NOT EXISTS (SELECT FROM anchor.load_batch b WHERE b.load_date = s.load_date)
    ) THEN
        RAISE EXCEPTION 'An earlier source day must be loaded before %', source_day;
    END IF;

    FOR entity_name IN SELECT DISTINCT entity FROM anchor.history_model ORDER BY entity LOOP
        EXECUTE format(
            'INSERT INTO anchor.%1$I (id) SELECT id FROM stg.%1$I WHERE load_date = $1 ON CONFLICT DO NOTHING',
            entity_name
        ) USING source_day;
        GET DIAGNOSTICS row_count = ROW_COUNT;
        anchor_count := anchor_count + row_count;
    END LOOP;

    FOR spec IN SELECT entity, relation, fields FROM anchor.history_model ORDER BY relation LOOP
        SELECT string_agg(format('a.%I', col), ', ' ORDER BY ord),
               string_agg(format('s.%I', col), ', ' ORDER BY ord)
        INTO old_values, new_values
        FROM unnest(string_to_array(spec.fields, ',')) WITH ORDINALITY AS f(col, ord);

        EXECUTE format(
            'UPDATE anchor.%1$I a SET valid_to = $1::date - 1 FROM stg.%2$I s
             WHERE s.load_date = $1 AND s.id = a.id AND a.valid_to IS NULL
             AND ROW(%3$s) IS DISTINCT FROM ROW(%4$s)',
            spec.relation, spec.entity, old_values, new_values
        ) USING source_day;

        EXECUTE format(
            'INSERT INTO anchor.%1$I (id, valid_from, %2$s)
             SELECT s.id, $1, %3$s FROM stg.%4$I s
             WHERE s.load_date = $1 AND NOT EXISTS (
                 SELECT FROM anchor.%1$I a WHERE a.id = s.id AND a.valid_to IS NULL
             )',
            spec.relation, spec.fields, new_values, spec.entity
        ) USING source_day;
        GET DIAGNOSTICS row_count = ROW_COUNT;
        history_count := history_count + row_count;
        IF row_count > 0 THEN
            RAISE INFO '%: % new versions', spec.relation, row_count;
        END IF;
    END LOOP;

    INSERT INTO anchor.load_batch VALUES (source_day, anchor_count, history_count);
    RETURN jsonb_build_object('state', 'applied', 'anchors', anchor_count, 'versions', history_count);
END;
$$;

COMMIT;
