-- Task 1. Target data mart: one row per client per reporting month.
-- Every column corresponds to a bullet from the task description (see README.md).

CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.customer_report (
    id                       bigserial PRIMARY KEY,        -- идентификатор записи
    client_id                bigint       NOT NULL,        -- идентификатор заказчика
    client_name              varchar(255) NOT NULL,        -- Ф. И. О. заказчика
    client_address           varchar(255),                 -- адрес заказчика
    client_birthday          date,                         -- дата рождения заказчика
    client_email             varchar(255),                 -- электронная почта заказчика
    total_paid_amount        numeric(14, 2) NOT NULL,      -- сумма всех оплаченных заказов клиента
    orders_count             integer      NOT NULL,        -- количество заказов за месяц
    avg_order_value          numeric(14, 2),               -- средняя стоимость заказа за месяц
    median_completion_days   numeric(10, 2),               -- медианное время выполнения, дней
    top_product_type         varchar(100),                 -- самая популярная категория товаров
    top_master_id            bigint,                       -- самый популярный мастер
    orders_created_count     integer      NOT NULL,        -- заказов в статусе created
    orders_in_progress_count integer      NOT NULL,        -- заказов в процессе изготовления
    orders_delivery_count    integer      NOT NULL,        -- заказов в доставке (доп. метрика)
    orders_done_count        integer      NOT NULL,        -- завершённых заказов
    orders_not_done_count    integer      NOT NULL,        -- незавершённых заказов
    report_year              integer      NOT NULL,        -- отчётный период: год
    report_month             integer      NOT NULL,        -- отчётный период: месяц
    CONSTRAINT uq_customer_report_period UNIQUE (client_id, report_year, report_month)
);

COMMENT ON TABLE mart.customer_report IS
    'Task 1 data mart: client metrics per reporting month. Reloadable per period (delete + insert).';
