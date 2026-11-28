BEGIN;

CREATE VIEW report.clock AS SELECT max(load_date) AS as_of FROM anchor.load_batch;

CREATE FUNCTION report.tour_services(on_day date)
RETURNS TABLE (
    tour_id integer, destination text, country_code text, hotel_id integer,
    check_in date, check_out date, flight_date date, fare numeric, nightly_rate numeric, package_cost numeric
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
    SELECT t.id, c.label, d.country_code, h.hotel_id, h.check_in, h.check_out,
           f.departure_date, fp.fare, hp.nightly_rate, fp.fare + hp.nightly_rate * (h.check_out - h.check_in)
    FROM anchor.tour t
    JOIN anchor.tour_country d ON d.id = t.id AND daterange(d.valid_from, d.valid_to, '[]') @> on_day
    JOIN anchor.country c ON c.code = d.country_code
    JOIN anchor.tour_hotel h ON h.id = t.id AND daterange(h.valid_from, h.valid_to, '[]') @> on_day
    JOIN anchor.hotel_rate hp ON hp.id = h.hotel_id AND daterange(hp.valid_from, hp.valid_to, '[]') @> on_day
    JOIN anchor.tour_flight tf ON tf.id = t.id AND daterange(tf.valid_from, tf.valid_to, '[]') @> on_day
    JOIN anchor.flight_departure f ON f.id = tf.flight_id AND daterange(f.valid_from, f.valid_to, '[]') @> on_day
    JOIN anchor.flight_fare fp ON fp.id = tf.flight_id AND daterange(fp.valid_from, fp.valid_to, '[]') @> on_day;
$$;

CREATE VIEW report.booking_cost AS
SELECT first_tour.id AS booking_id, first_tour.valid_from AS created_on, first_tour.tour_id,
       t.destination, t.flight_date, t.fare, t.nightly_rate, t.check_out - t.check_in AS nights, t.package_cost,
       money.total AS quoted_total
FROM (
    SELECT DISTINCT ON (id) id, tour_id, valid_from FROM anchor.booking_tour ORDER BY id, valid_from
) first_tour
JOIN anchor.booking_total money
  ON money.id = first_tour.id AND daterange(money.valid_from, money.valid_to, '[]') @> first_tour.valid_from
CROSS JOIN LATERAL report.tour_services(first_tour.valid_from) t
WHERE t.tour_id = first_tour.tour_id;

CREATE VIEW report.customer_profile AS
SELECT n.id AS customer_id, n.full_name, p.passport,
       count(DISTINCT t.tour_id) FILTER (WHERE s.status_code = 'completed') AS completed_tours
FROM anchor.customer_name n
JOIN anchor.customer_passport p ON p.id = n.id AND p.valid_to IS NULL
LEFT JOIN anchor.booking_customer b ON b.customer_id = n.id AND b.valid_to IS NULL
LEFT JOIN anchor.booking_state s ON s.id = b.id AND s.valid_to IS NULL
LEFT JOIN anchor.booking_tour t ON t.id = b.id AND t.valid_to IS NULL
WHERE n.valid_to IS NULL
GROUP BY n.id, n.full_name, p.passport;

CREATE VIEW report.hotel_price_history AS
SELECT p.id AS hotel_id, n.hotel_name, p.valid_from, p.valid_to, p.nightly_rate
FROM anchor.hotel_rate p
JOIN anchor.hotel_name n ON n.id = p.id AND daterange(n.valid_from, n.valid_to, '[]') @> p.valid_from;

CREATE VIEW report.hotel_stay AS
SELECT t.tour_id, t.hotel_id, n.hotel_name, t.check_in, t.check_out, t.check_out - t.check_in AS nights
FROM report.clock
CROSS JOIN LATERAL report.tour_services(as_of) t
JOIN anchor.hotel_name n ON n.id = t.hotel_id AND n.valid_to IS NULL;

CREATE VIEW report.hotel_month AS
SELECT hotel_id, hotel_name, month::date AS month,
       sum(least(check_out, (month + interval '1 month')::date) - greatest(check_in, month::date)) AS nights
FROM report.hotel_stay
CROSS JOIN LATERAL generate_series(
    date_trunc('month', check_in)::timestamp, date_trunc('month', check_out - 1)::timestamp, interval '1 month'
) month
GROUP BY hotel_id, hotel_name, month;

CREATE VIEW report.visa_assessment AS
SELECT b.id AS booking_id, b.tour_id, bc.customer_id, n.full_name, c.label AS destination, f.departure_date AS flight_date,
       e.expires_on, s.status_code AS booking_status,
       CASE WHEN e.expires_on IS NULL THEN 'Нет данных о визе'
            WHEN e.expires_on < f.departure_date THEN 'Истекла'
            WHEN vc.country_code <> d.country_code THEN 'Другая страна'
            WHEN v.status_code <> 'issued' THEN 'Виза не выдана'
            WHEN i.issued_on > f.departure_date THEN 'Выдана после вылета'
       END AS problem
FROM report.clock
CROSS JOIN anchor.booking_tour b
JOIN anchor.tour_flight tf ON tf.id = b.tour_id
JOIN anchor.flight_departure f ON f.id = tf.flight_id
CROSS JOIN LATERAL (SELECT least(as_of, f.departure_date) AS known_on) cutoff
JOIN anchor.booking_customer bc ON bc.id = b.id AND daterange(bc.valid_from, bc.valid_to, '[]') @> known_on
JOIN anchor.booking_state s ON s.id = b.id AND daterange(s.valid_from, s.valid_to, '[]') @> known_on
JOIN anchor.customer_name n ON n.id = bc.customer_id AND daterange(n.valid_from, n.valid_to, '[]') @> known_on
JOIN anchor.tour_country d ON d.id = b.tour_id AND daterange(d.valid_from, d.valid_to, '[]') @> known_on
JOIN anchor.country c ON c.code = d.country_code
LEFT JOIN anchor.booking_visa bv ON bv.id = b.id AND daterange(bv.valid_from, bv.valid_to, '[]') @> known_on
LEFT JOIN anchor.visa_expiry e ON e.id = bv.visa_id AND daterange(e.valid_from, e.valid_to, '[]') @> known_on
LEFT JOIN anchor.visa_country vc ON vc.id = bv.visa_id AND daterange(vc.valid_from, vc.valid_to, '[]') @> known_on
LEFT JOIN anchor.visa_state v ON v.id = bv.visa_id AND daterange(v.valid_from, v.valid_to, '[]') @> known_on
LEFT JOIN anchor.visa_issue i ON i.id = bv.visa_id AND daterange(i.valid_from, i.valid_to, '[]') @> known_on
WHERE daterange(b.valid_from, b.valid_to, '[]') @> known_on
  AND daterange(tf.valid_from, tf.valid_to, '[]') @> known_on
  AND daterange(f.valid_from, f.valid_to, '[]') @> known_on
  AND s.status_code <> 'cancelled';

CREATE VIEW report.upcoming_visa AS
SELECT v.*
FROM report.visa_assessment v CROSS JOIN report.clock
WHERE v.flight_date >= as_of AND v.flight_date < as_of + 30
  AND v.booking_status IN ('booked', 'confirmed');

CREATE FUNCTION report.tour_cost_history(selected_tour integer)
RETURNS TABLE (valid_from date, valid_to date, package_cost numeric)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
    WITH points AS (
        SELECT h.valid_from FROM anchor.tour_hotel h WHERE h.id = selected_tour
        UNION SELECT f.valid_from FROM anchor.tour_flight f WHERE f.id = selected_tour
        UNION SELECT p.valid_from FROM anchor.tour_hotel h JOIN anchor.hotel_rate p ON p.id = h.hotel_id
              WHERE h.id = selected_tour
        UNION SELECT p.valid_from FROM anchor.tour_flight f JOIN anchor.flight_fare p ON p.id = f.flight_id
              WHERE f.id = selected_tour
    ), compared AS (
        SELECT p.valid_from, t.package_cost,
               lag(t.package_cost) OVER (ORDER BY p.valid_from) AS previous
        FROM points p CROSS JOIN LATERAL report.tour_services(p.valid_from) t
        WHERE t.tour_id = selected_tour
    )
    SELECT c.valid_from, lead(c.valid_from) OVER (ORDER BY c.valid_from) - 1, c.package_cost
    FROM compared c WHERE c.package_cost IS DISTINCT FROM c.previous
    ORDER BY c.valid_from;
$$;

REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA report FROM PUBLIC;
GRANT USAGE ON SCHEMA report TO monitoring;
GRANT SELECT ON ALL TABLES IN SCHEMA report TO monitoring;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA report TO monitoring;

COMMIT;
