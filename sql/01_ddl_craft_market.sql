-- source1: широкая таблица
CREATE SCHEMA IF NOT EXISTS source1;

CREATE TABLE IF NOT EXISTS source1.craft_market_wide (
    id                    bigint PRIMARY KEY,
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
    order_id              bigint       NOT NULL,
    order_created_date    date         NOT NULL,
    order_completion_date date,
    order_status          varchar(50)  NOT NULL,
    customer_id           bigint       NOT NULL,
    customer_name         varchar(255),
    customer_address      varchar(255),
    customer_birthday     date,
    customer_email        varchar(255)
);

-- source2: мастера+товары и заказы+заказчики
CREATE SCHEMA IF NOT EXISTS source2;

-- Таблица мастеров и товаров. PK - craftsman_id, как на диаграмме.
CREATE TABLE IF NOT EXISTS source2.craft_market_masters_products (
    craftsman_id          bigint PRIMARY KEY,
    craftsman_name        varchar(255) NOT NULL,
    craftsman_address     varchar(255),
    craftsman_birthday    date,
    craftsman_email       varchar(255),
    product_id            bigint       NOT NULL,
    product_name          varchar(255) NOT NULL,
    product_description   text,
    product_type          varchar(100) NOT NULL,
    product_price         bigint       NOT NULL CHECK (product_price >= 0)
);

-- Таблица заказов и заказчиков.
CREATE TABLE IF NOT EXISTS source2.craft_market_orders_customers (
    order_id              bigint PRIMARY KEY,
    craftsman_id          bigint       NOT NULL
        REFERENCES source2.craft_market_masters_products (craftsman_id),
    product_id            bigint       NOT NULL,
    order_created_date    date         NOT NULL,
    order_completion_date date,
    order_status          varchar(50)  NOT NULL,
    customer_id           bigint       NOT NULL,
    customer_name         varchar(255),
    customer_address      varchar(255),
    customer_birthday     date,
    customer_email        varchar(255)
);

-- source3: заказы (с товарами), мастера, заказчики
CREATE SCHEMA IF NOT EXISTS source3;

-- Таблица мастеров.
CREATE TABLE IF NOT EXISTS source3.craft_market_craftsmans (
    craftsman_id          bigint PRIMARY KEY,
    craftsman_name        varchar(255) NOT NULL,
    craftsman_address     varchar(255),
    craftsman_birthday    date,
    craftsman_email       varchar(255)
);

-- Таблица заказчиков.
CREATE TABLE IF NOT EXISTS source3.craft_market_customers (
    customer_id           bigint PRIMARY KEY,
    customer_name         varchar(255),
    customer_address      varchar(255),
    customer_birthday     date,
    customer_email        varchar(255)
);

-- Таблица заказов с товарами.
CREATE TABLE IF NOT EXISTS source3.craft_market_orders (
    order_id              bigint PRIMARY KEY,
    product_id            bigint       NOT NULL,
    craftsman_id          bigint       NOT NULL
        REFERENCES source3.craft_market_craftsmans (craftsman_id),
    customer_id           bigint       NOT NULL
        REFERENCES source3.craft_market_customers (customer_id),
    order_created_date    date         NOT NULL,
    order_completion_date date,
    order_status          varchar(50)  NOT NULL,
    product_name          varchar(255) NOT NULL,
    product_description   text,
    product_type          varchar(100) NOT NULL,
    product_price         bigint       NOT NULL CHECK (product_price >= 0)
);

CREATE INDEX IF NOT EXISTS ix_cm_orders_craftsman ON source3.craft_market_orders (craftsman_id);
CREATE INDEX IF NOT EXISTS ix_cm_orders_customer  ON source3.craft_market_orders (customer_id);
