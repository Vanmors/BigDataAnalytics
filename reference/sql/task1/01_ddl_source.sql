-- Task 1. OLTP model of the handmade goods shop (schema `source`).
-- Mirrors the ER diagram from Task1_BDA.docx: master, product, master_product, order, client.
--
-- Deviations from the document (explained in README.md):
--   * `order` is a reserved word in PostgreSQL, so the table is named `orders`;
--     a view source."order" is provided for compatibility with the wording of the task.
--   * `client.email` is added: the report requires "электронная почта заказчика",
--     but the diagram does not have this column.

CREATE SCHEMA IF NOT EXISTS source;

DROP VIEW IF EXISTS source."order";

CREATE TABLE IF NOT EXISTS source.master (
    master_id   bigint PRIMARY KEY,
    name        varchar(255) NOT NULL,
    phone       varchar(50),
    address     varchar(255),
    birthday    date
);

CREATE TABLE IF NOT EXISTS source.product (
    product_id  bigint PRIMARY KEY,
    name        varchar(255) NOT NULL,
    description text,
    type        varchar(100) NOT NULL   -- product category, used for "самая популярная категория"
);

-- Which master makes which product and at what price.
CREATE TABLE IF NOT EXISTS source.master_product (
    id          bigint PRIMARY KEY,
    master_id   bigint NOT NULL REFERENCES source.master (master_id),
    product_id  bigint NOT NULL REFERENCES source.product (product_id),
    price       numeric(12, 2) NOT NULL CHECK (price >= 0),
    UNIQUE (master_id, product_id)
);

CREATE TABLE IF NOT EXISTS source.client (
    client_id   bigint PRIMARY KEY,
    name        varchar(255) NOT NULL,
    phone       varchar(50),
    address     varchar(255),
    birthday    date,
    email       varchar(255)
);

-- Order statuses used by the lab (the document does not define the domain explicitly):
--   created     - order is created, work has not started
--   in_progress - the master is making the item
--   delivery    - the item is made and handed over to delivery
--   done        - the order is completed (paid and received)
-- "незавершённый заказ" is everything that is not `done` - it is derived, not a status.
CREATE TABLE IF NOT EXISTS source.orders (
    order_id      bigint PRIMARY KEY,
    master_id     bigint NOT NULL REFERENCES source.master (master_id),
    product_id    bigint NOT NULL REFERENCES source.product (product_id),
    client_id     bigint NOT NULL REFERENCES source.client (client_id),
    order_date    date NOT NULL,
    delivery_date date,                       -- filled in when the order reaches `done`
    status        varchar(20) NOT NULL
        CHECK (status IN ('created', 'in_progress', 'delivery', 'done'))
);

CREATE INDEX IF NOT EXISTS ix_orders_client_date ON source.orders (client_id, order_date);
CREATE INDEX IF NOT EXISTS ix_orders_status ON source.orders (status);

-- Compatibility view: the task calls this entity `order`.
CREATE VIEW source."order" AS SELECT * FROM source.orders;
