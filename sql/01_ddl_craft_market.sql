CREATE SCHEMA IF NOT EXISTS craft_market;

CREATE TABLE IF NOT EXISTS craft_market.craft_market_craftsmans (
    craftsman_id          bigint PRIMARY KEY,
    craftsman_name        varchar(255) NOT NULL,
    craftsman_address     varchar(255),
    craftsman_birthday    date,
    craftsman_email       varchar(255)
);

CREATE TABLE IF NOT EXISTS craft_market.craft_market_customers (
    customer_id           bigint PRIMARY KEY,
    customer_name         varchar(255),
    customer_address      varchar(255),
    customer_birthday     date,
    customer_email        varchar(255)
);

CREATE TABLE IF NOT EXISTS craft_market.craft_market_masters_products (
    craftsman_id          bigint       NOT NULL,
    craftsman_name        varchar(255) NOT NULL,
    craftsman_address     varchar(255),
    craftsman_birthday    date,
    craftsman_email       varchar(255),
    product_id            bigint       NOT NULL,
    product_name          varchar(255) NOT NULL,
    product_description   text,
    product_type          varchar(100) NOT NULL,
    product_price         bigint       NOT NULL CHECK (product_price >= 0),
    PRIMARY KEY (craftsman_id, product_id)
);

CREATE TABLE IF NOT EXISTS craft_market.craft_market_orders_customers (
    order_id              bigint PRIMARY KEY,
    craftsman_id          bigint       NOT NULL,
    product_id            bigint       NOT NULL,
    order_created_date    date         NOT NULL,
    order_completion_date date,
    order_status          varchar(50)  NOT NULL,
    customer_id           bigint       NOT NULL,
    customer_name         varchar(255),
    customer_address      varchar(255),
    customer_birthday     date,
    customer_email        varchar(255),
    FOREIGN KEY (craftsman_id, product_id)
        REFERENCES craft_market.craft_market_masters_products (craftsman_id, product_id)
);

CREATE TABLE IF NOT EXISTS craft_market.craft_market_orders (
    order_id              bigint PRIMARY KEY,
    product_id            bigint       NOT NULL,
    craftsman_id          bigint       NOT NULL
        REFERENCES craft_market.craft_market_craftsmans (craftsman_id),
    customer_id           bigint       NOT NULL
        REFERENCES craft_market.craft_market_customers (customer_id),
    order_created_date    date         NOT NULL,
    order_completion_date date,
    order_status          varchar(50)  NOT NULL,
    product_name          varchar(255) NOT NULL,
    product_description   text,
    product_type          varchar(100) NOT NULL,
    product_price         bigint       NOT NULL CHECK (product_price >= 0)
);
