CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.craft_market_customer_report (
    id                          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id                 bigint       NOT NULL,
    customer_name               varchar(255),
    customer_address            varchar(255),
    customer_birthday           date,
    customer_email              varchar(255),
    customer_spent              numeric(14, 2) NOT NULL,
    platform_money              numeric(14, 2) NOT NULL,
    count_order                 bigint       NOT NULL,
    avg_price_order             numeric(14, 2),
    median_time_order_completed numeric(10, 2),
    top_product_category        varchar(100),
    top_craftsman_id            bigint,
    count_order_created         bigint       NOT NULL,
    count_order_in_progress     bigint       NOT NULL,
    count_order_delivery        bigint       NOT NULL,
    count_order_done            bigint       NOT NULL,
    count_order_not_done        bigint       NOT NULL,
    report_period               varchar(7)   NOT NULL,
    UNIQUE (customer_id, report_period)
);
