-- Task 2. External source schema attached to the warehouse.
--
-- `staging` holds the CSV files as they are (all columns from the file),
-- `external_source` holds only the columns listed in the task.
-- Splitting them is what makes this an ETL and not a plain file copy.

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS external_source;

-- ---------------------------------------------------------------- order_statuses
-- File columns: id, date_time, city_id, city_name, client_id, first_name, last_name,
--               item_id, item_name, quantity, payment_amount
CREATE TABLE IF NOT EXISTS staging.order_statuses_raw (
    id             bigint,
    date_time      timestamp,
    city_id        integer,
    city_name      varchar(100),
    client_id      bigint,
    first_name     varchar(100),
    last_name      varchar(100),
    item_id        bigint,
    item_name      varchar(255),
    quantity       integer,
    payment_amount numeric(12, 2)
);

CREATE TABLE IF NOT EXISTS external_source.order_statuses (
    date_time      timestamp NOT NULL,
    client_id      bigint NOT NULL,
    quantity       integer,
    payment_amount numeric(12, 2)
);

-- ---------------------------------------------------------------- user_behavior
-- File columns: id, date_time, behavior_id, user_id, quantity
CREATE TABLE IF NOT EXISTS staging.user_behavior_raw (
    id          bigint,
    date_time   timestamp,
    behavior_id integer,
    user_id     bigint,
    quantity    integer
);

-- The task asks for `client_id` in the target table while the file carries `user_id`:
-- this rename is the whole point of the transform step.
CREATE TABLE IF NOT EXISTS external_source.user_behavior (
    date_time timestamp NOT NULL,
    client_id bigint NOT NULL
);

-- ---------------------------------------------------------------- marketing_research
-- File columns: date_id, category_id, sales_qty, sales_amt
CREATE TABLE IF NOT EXISTS staging.marketing_research_raw (
    date_id     date,
    category_id integer,
    sales_qty   integer,
    sales_amt   numeric(12, 2)
);

CREATE TABLE IF NOT EXISTS external_source.marketing_research (
    date_id   date NOT NULL,
    sales_qty integer,
    sales_amt numeric(12, 2)
);
