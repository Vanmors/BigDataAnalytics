SELECT
    row_number() OVER ()                         AS id,                    -- идентификатор записи
    c.customer_id,
    c.customer_name,
    c.customer_address,
    c.customer_birthday,
    c.customer_email,
    sum(o.product_price)                         AS customer_spent,        -- потратил заказчик
    sum(o.product_price) * 0.1                   AS platform_money,        -- 10% платформе
    count(*)                                     AS count_order,
    avg(o.product_price)                         AS avg_price_order,
    percentile_cont(0.5) WITHIN GROUP (
        ORDER BY o.order_completion_date - o.order_created_date
    ) FILTER (WHERE o.order_completion_date IS NOT NULL)
                                                 AS median_time_order_completed,
    mode() WITHIN GROUP (ORDER BY o.product_type)  AS top_product_category,
    mode() WITHIN GROUP (ORDER BY o.craftsman_id)  AS top_craftsman_id,
    count(*) FILTER (WHERE o.order_status = 'created')     AS count_order_created,
    count(*) FILTER (WHERE o.order_status = 'in progress') AS count_order_in_progress,
    count(*) FILTER (WHERE o.order_status = 'delivery')    AS count_order_delivery,
    count(*) FILTER (WHERE o.order_status = 'done')        AS count_order_done,
    count(*) FILTER (WHERE o.order_status <> 'done')       AS count_order_not_done,
    to_char(o.order_created_date, 'YYYY-MM')     AS report_period
FROM source3.craft_market_orders o
JOIN source3.craft_market_customers c USING (customer_id)
-- отчётный месяц: параметры подставляет DAG
WHERE o.order_created_date >= make_date(:report_year, :report_month, 1)
  AND o.order_created_date <  make_date(:report_year, :report_month, 1) + interval '1 month'
GROUP BY c.customer_id, c.customer_name, c.customer_address,
         c.customer_birthday, c.customer_email,
         to_char(o.order_created_date, 'YYYY-MM');