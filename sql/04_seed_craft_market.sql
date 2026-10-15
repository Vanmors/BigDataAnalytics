TRUNCATE TABLE source3.craft_market_orders, source3.craft_market_customers, source3.craft_market_craftsmans;

COPY source3.craft_market_craftsmans (craftsman_id, craftsman_name, craftsman_address, craftsman_birthday,
                                      craftsman_email)
    FROM '/data/craft_market/craft_market_craftsmans.csv' WITH (FORMAT csv, HEADER true);

COPY source3.craft_market_customers (customer_id, customer_name, customer_address, customer_birthday,
                                     customer_email)
    FROM '/data/craft_market/craft_market_customers.csv' WITH (FORMAT csv, HEADER true);

COPY source3.craft_market_orders (order_id, product_id, craftsman_id, customer_id, order_created_date,
                                  order_completion_date, order_status, product_name, product_description,
                                  product_type, product_price)
    FROM '/data/craft_market/craft_market_orders.csv' WITH (FORMAT csv, HEADER true);
