BEGIN;

DO $$
DECLARE
    first_day date;
    last_day date;
    next_day date;
    month_end date;
    visa_check_day date;
    relation_name text;
    has_changes boolean;
    result jsonb;
BEGIN
    SELECT min(load_date), max(load_date) INTO first_day, last_day FROM anchor.load_batch;
    IF (SELECT count(*) FROM anchor.load_batch) <> 3 OR
       (SELECT count(*) FROM report.booking_cost) <> 16 THEN
        RAISE EXCEPTION 'Expected three demo batches and 16 bookable itineraries. Run make demo first.';
    END IF;

    FOR relation_name IN SELECT relation FROM anchor.history_model LOOP
        EXECUTE format(
            'SELECT EXISTS (SELECT FROM anchor.%I GROUP BY id HAVING count(*) BETWEEN 2 AND 3)', relation_name
        ) INTO has_changes;
        IF NOT has_changes THEN
            RAISE EXCEPTION 'Expected 2–3 historical versions for a sample key in %', relation_name;
        END IF;
    END LOOP;

    IF (SELECT tour_id FROM report.booking_cost WHERE booking_id = 3) IS DISTINCT FROM 3 OR
       (SELECT tour_id FROM anchor.booking_tour WHERE id = 3 AND valid_to IS NULL) IS DISTINCT FROM 4 OR
       (SELECT package_cost FROM report.booking_cost WHERE booking_id = 1) IS DISTINCT FROM 33100.00 THEN
        RAISE EXCEPTION 'Booking-day reports must retain the original route and rates';
    END IF;

    IF (SELECT count(*) FROM report.tour_cost_history(1)) <> 3 OR EXISTS (
        SELECT FROM (
            SELECT valid_from, valid_to, lead(valid_from) OVER (ORDER BY valid_from) AS next_from
            FROM report.tour_cost_history(1)
        ) h WHERE valid_to IS DISTINCT FROM next_from - 1
    ) THEN
        RAISE EXCEPTION 'Expected three adjacent periods and one open end for tour 1';
    END IF;

    IF NOT EXISTS (SELECT FROM report.upcoming_visa WHERE booking_id = 9 AND problem = 'Истекла')
       OR NOT EXISTS (SELECT FROM report.upcoming_visa WHERE booking_id = 10 AND problem IS NULL) THEN
        RAISE EXCEPTION 'Expected expired visa 9 and extended, valid visa 10';
    END IF;

    IF (SELECT array_agg(tour_id ORDER BY tour_id) FROM report.visa_assessment WHERE booking_id = 3)
       IS DISTINCT FROM ARRAY[3] THEN
        RAISE EXCEPTION 'The visa report must use the tour known at departure for a past flight';
    END IF;

    IF EXISTS (
        SELECT v.visa_id FROM anchor.booking_visa v
        JOIN anchor.booking_customer c ON c.id = v.id AND c.valid_to IS NULL
        WHERE v.valid_to IS NULL GROUP BY v.visa_id HAVING count(DISTINCT c.customer_id) > 1
    ) THEN
        RAISE EXCEPTION 'A personal visa is assigned to more than one customer';
    END IF;

    result := anchor.load_day(last_day);
    IF result <> '{"state":"already_applied","anchors":0,"versions":0}'::jsonb THEN
        RAISE EXCEPTION 'Replay changed data: %', result;
    END IF;
    IF anchor.load_day(last_day + 1)->>'state' <> 'no_source_rows' THEN
        RAISE EXCEPTION 'An empty day was marked as applied';
    END IF;

    INSERT INTO stg.hotel SELECT first_day - 1, id, hotel_name, address, stars, nightly_rate
    FROM stg.hotel WHERE load_date = first_day AND id = 1;
    BEGIN
        PERFORM anchor.load_day(first_day - 1);
        RAISE EXCEPTION 'An older and unapplied day was accepted';
    EXCEPTION WHEN SQLSTATE 'P0001' THEN
        IF SQLERRM NOT LIKE '%precedes committed history' THEN RAISE; END IF;
    END;
    DELETE FROM stg.hotel WHERE load_date = first_day - 1;

    next_day := last_day + 1;
    INSERT INTO stg.hotel SELECT next_day, id, hotel_name, address, stars, nightly_rate
    FROM stg.hotel WHERE load_date = last_day AND id = 1;

    INSERT INTO stg.hotel SELECT next_day + 1, id, hotel_name, address, stars, nightly_rate + 10
    FROM stg.hotel WHERE load_date = last_day AND id = 1;
    INSERT INTO stg.hotel VALUES (next_day + 1, 97, 'Тестовый', 'Тестовая, 1', 4, 5000);
    INSERT INTO stg.tour SELECT next_day + 1, id, start_date, end_date, country_code, 999, check_in, check_out, flight_id
    FROM stg.tour WHERE load_date = last_day AND id = 1;

    BEGIN
        PERFORM anchor.load_day(next_day + 1);
        RAISE EXCEPTION 'The loader jumped over a staged source day';
    EXCEPTION WHEN SQLSTATE 'P0001' THEN
        IF SQLERRM NOT LIKE 'An earlier source day must be loaded before %' THEN RAISE; END IF;
    END;

    result := anchor.load_day(next_day);
    IF result <> '{"state":"applied","anchors":0,"versions":0}'::jsonb THEN
        RAISE EXCEPTION 'An unchanged row generated a version: %', result;
    END IF;

    BEGIN
        PERFORM anchor.load_day(next_day + 1);
        RAISE EXCEPTION 'Invalid foreign key was accepted';
    EXCEPTION WHEN foreign_key_violation THEN
        NULL;
    END;

    IF EXISTS (SELECT FROM anchor.hotel WHERE id = 97)
       OR EXISTS (SELECT FROM anchor.hotel_rate WHERE valid_from = next_day + 1 OR valid_to = next_day)
       OR EXISTS (SELECT FROM anchor.load_batch WHERE load_date = next_day + 1) THEN
        RAISE EXCEPTION 'A failed batch committed a partial change';
    END IF;

    month_end := date_trunc('month', next_day + interval '1 month')::date - 1;
    UPDATE stg.tour SET hotel_id = 97, check_in = month_end, check_out = month_end + 3,
                       start_date = month_end, end_date = month_end + 3
    WHERE load_date = next_day + 1;
    PERFORM anchor.load_day(next_day + 1);
    IF (SELECT array_agg(nights ORDER BY month) FROM report.hotel_month WHERE hotel_id = 97)
       IS DISTINCT FROM ARRAY[1, 2]::bigint[] THEN
        RAISE EXCEPTION 'A cross-month stay must split into one and two nights, excluding checkout';
    END IF;

    visa_check_day := next_day + 2;
    INSERT INTO stg.flight
    SELECT visa_check_day, id, flight_number, visa_check_day + CASE id WHEN 9 THEN 29 ELSE 30 END,
           fare, class_code, seat, airline_code
    FROM stg.flight WHERE load_date = last_day AND id IN (9, 11);
    PERFORM anchor.load_day(visa_check_day);
    IF NOT EXISTS (SELECT FROM report.upcoming_visa WHERE booking_id = 9)
       OR EXISTS (SELECT FROM report.upcoming_visa WHERE booking_id = 11) THEN
        RAISE EXCEPTION 'The 30-day visa window must include day 29 and exclude day 30';
    END IF;
END;
$$;

ROLLBACK;
\echo 'PASS · история, отчёты, идемпотентность, порядок и откат'
