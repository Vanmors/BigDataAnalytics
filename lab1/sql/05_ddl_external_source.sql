CREATE SCHEMA IF NOT EXISTS external_source;

CREATE TABLE IF NOT EXISTS external_source.user_order_log (
    date_time      timestamp      NOT NULL,
    customer_id    bigint         NOT NULL,
    quantity       integer        NOT NULL CHECK (quantity > 0),
    payment_amount numeric(14, 2) NOT NULL CHECK (payment_amount >= 0)
);

CREATE TABLE IF NOT EXISTS external_source.user_activity_log (
    date_time      timestamp      NOT NULL,
    customer_id    bigint         NOT NULL
);

CREATE TABLE IF NOT EXISTS external_source.customer_research (
    date_id        date           NOT NULL,
    geo_id         integer        NOT NULL,
    sales_qty      integer        NOT NULL CHECK (sales_qty >= 0),
    sales_amt      numeric(14, 2) NOT NULL CHECK (sales_amt >= 0)
);