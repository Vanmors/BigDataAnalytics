CREATE DATABASE airflow;

BEGIN;
CREATE ROLE monitoring LOGIN PASSWORD 'monitoring';
CREATE SCHEMA stg;
CREATE SCHEMA anchor;
CREATE SCHEMA report;

CREATE TABLE anchor.country (code text PRIMARY KEY, label text NOT NULL);
CREATE TABLE anchor.airline (code text PRIMARY KEY, label text NOT NULL);
CREATE TABLE anchor.flight_class (code text PRIMARY KEY, label text NOT NULL);
CREATE TABLE anchor.visa_type (code text PRIMARY KEY, label text NOT NULL);
CREATE TABLE anchor.visa_status (code text PRIMARY KEY, label text NOT NULL);
CREATE TABLE anchor.booking_status (code text PRIMARY KEY, label text NOT NULL);

CREATE TABLE anchor.customer (id integer PRIMARY KEY);
CREATE TABLE anchor.tour (id integer PRIMARY KEY);
CREATE TABLE anchor.hotel (id integer PRIMARY KEY);
CREATE TABLE anchor.flight (id integer PRIMARY KEY);
CREATE TABLE anchor.visa (id integer PRIMARY KEY);
CREATE TABLE anchor.booking (id integer PRIMARY KEY);

CREATE TABLE stg.customer (
    load_date date NOT NULL, id integer NOT NULL, full_name text NOT NULL, passport text NOT NULL,
    PRIMARY KEY (load_date, id)
);
CREATE TABLE stg.tour (
    load_date date NOT NULL, id integer NOT NULL, start_date date NOT NULL, end_date date NOT NULL,
    country_code text NOT NULL, hotel_id integer NOT NULL, check_in date NOT NULL, check_out date NOT NULL,
    flight_id integer NOT NULL, PRIMARY KEY (load_date, id),
    CHECK (end_date >= start_date), CHECK (check_out > check_in)
);
CREATE TABLE stg.hotel (
    load_date date NOT NULL, id integer NOT NULL, hotel_name text NOT NULL, address text NOT NULL,
    stars integer NOT NULL, nightly_rate numeric(12,2) NOT NULL, PRIMARY KEY (load_date, id)
);
CREATE TABLE stg.flight (
    load_date date NOT NULL, id integer NOT NULL, flight_number text NOT NULL, departure_date date NOT NULL,
    fare numeric(12,2) NOT NULL, class_code text NOT NULL, seat text NOT NULL, airline_code text NOT NULL,
    PRIMARY KEY (load_date, id)
);
CREATE TABLE stg.visa (
    load_date date NOT NULL, id integer NOT NULL, country_code text NOT NULL, type_code text NOT NULL,
    issued_on date NOT NULL, expires_on date NOT NULL, status_code text NOT NULL,
    PRIMARY KEY (load_date, id), CHECK (expires_on >= issued_on)
);
CREATE TABLE stg.booking (
    load_date date NOT NULL, id integer NOT NULL, status_code text NOT NULL, total numeric(12,2) NOT NULL,
    customer_id integer NOT NULL, tour_id integer NOT NULL, visa_id integer NOT NULL,
    PRIMARY KEY (load_date, id)
);

CREATE VIEW stg.load_dates AS
    SELECT load_date FROM stg.customer UNION SELECT load_date FROM stg.tour
    UNION SELECT load_date FROM stg.hotel UNION SELECT load_date FROM stg.flight
    UNION SELECT load_date FROM stg.visa UNION SELECT load_date FROM stg.booking;

CREATE TABLE anchor.load_batch (
    load_date date PRIMARY KEY,
    anchor_rows integer NOT NULL,
    history_rows integer NOT NULL
);

-- Every declaration creates a separate typed table with the same versioning rules.
CREATE VIEW anchor.history_model (entity, relation, fields, definition) AS VALUES
    ('customer', 'customer_name',     'full_name',      'full_name text NOT NULL'),
    ('customer', 'customer_passport', 'passport',       'passport text NOT NULL'),
    ('tour',     'tour_start',        'start_date',     'start_date date NOT NULL'),
    ('tour',     'tour_end',          'end_date',       'end_date date NOT NULL'),
    ('tour',     'tour_country',      'country_code',   'country_code text NOT NULL REFERENCES anchor.country'),
    ('hotel',    'hotel_name',        'hotel_name',     'hotel_name text NOT NULL'),
    ('hotel',    'hotel_address',     'address',        'address text NOT NULL'),
    ('hotel',    'hotel_stars',       'stars',          'stars integer NOT NULL CHECK (stars BETWEEN 1 AND 5)'),
    ('hotel',    'hotel_rate',        'nightly_rate',   'nightly_rate numeric(12,2) NOT NULL CHECK (nightly_rate >= 0)'),
    ('flight',   'flight_number',     'flight_number',  'flight_number text NOT NULL'),
    ('flight',   'flight_departure',  'departure_date', 'departure_date date NOT NULL'),
    ('flight',   'flight_fare',       'fare',           'fare numeric(12,2) NOT NULL CHECK (fare >= 0)'),
    ('flight',   'flight_cabin',      'class_code',     'class_code text NOT NULL REFERENCES anchor.flight_class'),
    ('flight',   'flight_seat',       'seat',           'seat text NOT NULL'),
    ('flight',   'flight_airline',    'airline_code',   'airline_code text NOT NULL REFERENCES anchor.airline'),
    ('visa',     'visa_country',      'country_code',   'country_code text NOT NULL REFERENCES anchor.country'),
    ('visa',     'visa_type_code',    'type_code',      'type_code text NOT NULL REFERENCES anchor.visa_type'),
    ('visa',     'visa_issue',        'issued_on',      'issued_on date NOT NULL'),
    ('visa',     'visa_expiry',       'expires_on',     'expires_on date NOT NULL'),
    ('visa',     'visa_state',        'status_code',    'status_code text NOT NULL REFERENCES anchor.visa_status'),
    ('booking',  'booking_state',     'status_code',    'status_code text NOT NULL REFERENCES anchor.booking_status'),
    ('booking',  'booking_total',     'total',          'total numeric(12,2) NOT NULL CHECK (total >= 0)'),
    ('tour',     'tour_hotel',        'hotel_id,check_in,check_out',
        'hotel_id integer NOT NULL REFERENCES anchor.hotel, check_in date NOT NULL, check_out date NOT NULL CHECK (check_out > check_in)'),
    ('tour',     'tour_flight',       'flight_id',      'flight_id integer NOT NULL REFERENCES anchor.flight'),
    ('booking',  'booking_customer',  'customer_id',    'customer_id integer NOT NULL REFERENCES anchor.customer'),
    ('booking',  'booking_tour',      'tour_id',        'tour_id integer NOT NULL REFERENCES anchor.tour'),
    ('booking',  'booking_visa',      'visa_id',        'visa_id integer NOT NULL REFERENCES anchor.visa');

DO $$
DECLARE
    spec record;
BEGIN
    FOR spec IN SELECT * FROM anchor.history_model LOOP
        EXECUTE format(
            'CREATE TABLE anchor.%I (
                id integer NOT NULL REFERENCES anchor.%I,
                valid_from date NOT NULL,
                valid_to date CHECK (valid_to >= valid_from),
                %s,
                PRIMARY KEY (id, valid_from)
            )',
            spec.relation, spec.entity, spec.definition
        );
        EXECUTE format('CREATE UNIQUE INDEX ON anchor.%I (id) WHERE valid_to IS NULL', spec.relation);
    END LOOP;
END;
$$;

COMMIT;
