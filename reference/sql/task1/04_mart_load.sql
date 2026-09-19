-- Task 1. Rebuild mart.customer_report for one reporting month.
--
-- Parameters (passed by the DAG / by psql -v):
--   :report_year, :report_month
--
-- The load is idempotent: rows of the period are deleted and inserted again,
-- so any Airflow re-run produces the same result.

DELETE FROM mart.customer_report
WHERE report_year = :report_year
  AND report_month = :report_month;

WITH
-- 1. All orders of the period enriched with the price of the ordered item.
--    price lives in master_product (master_id + product_id), not in orders.
period_orders AS (
    SELECT o.order_id,
           o.client_id,
           o.master_id,
           o.product_id,
           o.order_date,
           o.delivery_date,
           o.status,
           p.type AS product_type,
           mp.price
    FROM source.orders o
             JOIN source.product p ON p.product_id = o.product_id
             JOIN source.master_product mp
                  ON mp.master_id = o.master_id AND mp.product_id = o.product_id
    WHERE date_part('year', o.order_date) = :report_year
      AND date_part('month', o.order_date) = :report_month
),
-- 2. "Сумма всех оплаченных заказов клиента" - cumulative, NOT limited to the month
--    (the task says "всех", unlike every other metric which says "за месяц").
paid_total AS (
    SELECT o.client_id,
           sum(mp.price) AS total_paid_amount
    FROM source.orders o
             JOIN source.master_product mp
                  ON mp.master_id = o.master_id AND mp.product_id = o.product_id
    WHERE o.status = 'done'
    GROUP BY o.client_id
),
-- 3. Monthly counters and averages.
monthly AS (
    SELECT client_id,
           count(*)                                                   AS orders_count,
           round(avg(price), 2)                                       AS avg_order_value,
           count(*) FILTER (WHERE status = 'created')                 AS orders_created_count,
           count(*) FILTER (WHERE status = 'in_progress')             AS orders_in_progress_count,
           count(*) FILTER (WHERE status = 'delivery')                AS orders_delivery_count,
           count(*) FILTER (WHERE status = 'done')                    AS orders_done_count,
           count(*) FILTER (WHERE status <> 'done')                   AS orders_not_done_count,
           -- Median completion time in days. `delivery_date - order_date` on two date columns
           -- yields integer; percentile_cont works on double precision and returns double
           -- precision, so the result is cast back to numeric for round().
           round((percentile_cont(0.5) WITHIN GROUP (ORDER BY (delivery_date - order_date))
                      FILTER (WHERE status = 'done' AND delivery_date IS NOT NULL)
                  )::numeric, 2)                                      AS median_completion_days,
           -- Most popular product category. mode() breaks ties arbitrarily - exactly what
           -- the task allows ("возьмите любого").
           mode() WITHIN GROUP (ORDER BY product_type)                AS top_product_type,
           -- Most popular master of this client in the period.
           mode() WITHIN GROUP (ORDER BY master_id)                   AS top_master_id
    FROM period_orders
    GROUP BY client_id
)
INSERT INTO mart.customer_report (client_id, client_name, client_address, client_birthday, client_email,
                                  total_paid_amount, orders_count, avg_order_value, median_completion_days,
                                  top_product_type, top_master_id, orders_created_count, orders_in_progress_count,
                                  orders_delivery_count, orders_done_count, orders_not_done_count,
                                  report_year, report_month)
SELECT c.client_id,
       c.name,
       c.address,
       c.birthday,
       c.email,
       coalesce(pt.total_paid_amount, 0),
       m.orders_count,
       m.avg_order_value,
       m.median_completion_days,
       m.top_product_type,
       m.top_master_id,
       m.orders_created_count,
       m.orders_in_progress_count,
       m.orders_delivery_count,
       m.orders_done_count,
       m.orders_not_done_count,
       :report_year,
       :report_month
FROM monthly m
         JOIN source.client c ON c.client_id = m.client_id
         LEFT JOIN paid_total pt ON pt.client_id = m.client_id;
