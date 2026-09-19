-- Task 2. Transform: move the columns required by the task from staging to external_source.
-- Full reload, so the DAG can be re-run any number of times.

TRUNCATE TABLE external_source.order_statuses;
INSERT INTO external_source.order_statuses (date_time, client_id, quantity, payment_amount)
SELECT date_time, client_id, quantity, payment_amount
FROM staging.order_statuses_raw
WHERE date_time IS NOT NULL
  AND client_id IS NOT NULL;

TRUNCATE TABLE external_source.user_behavior;
INSERT INTO external_source.user_behavior (date_time, client_id)
SELECT date_time, user_id
FROM staging.user_behavior_raw
WHERE date_time IS NOT NULL
  AND user_id IS NOT NULL;

TRUNCATE TABLE external_source.marketing_research;
INSERT INTO external_source.marketing_research (date_id, sales_qty, sales_amt)
SELECT date_id, sales_qty, sales_amt
FROM staging.marketing_research_raw
WHERE date_id IS NOT NULL;
