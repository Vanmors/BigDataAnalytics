-- Task 1. Load generated test data into the `source` schema.
-- Run `make gen` first - it creates the CSV files in ./data/source (mounted as /data inside the container).
-- Idempotent: the tables are truncated before loading.

TRUNCATE TABLE source.orders, source.master_product, source.product, source.client, source.master RESTART IDENTITY;

COPY source.master (master_id, name, phone, address, birthday)
    FROM '/data/source/master.csv' WITH (FORMAT csv, HEADER true);

COPY source.product (product_id, name, description, type)
    FROM '/data/source/product.csv' WITH (FORMAT csv, HEADER true);

COPY source.master_product (id, master_id, product_id, price)
    FROM '/data/source/master_product.csv' WITH (FORMAT csv, HEADER true);

COPY source.client (client_id, name, phone, address, birthday, email)
    FROM '/data/source/client.csv' WITH (FORMAT csv, HEADER true);

COPY source.orders (order_id, master_id, product_id, client_id, order_date, delivery_date, status)
    FROM '/data/source/orders.csv' WITH (FORMAT csv, HEADER true);

ANALYZE source.master;
ANALYZE source.product;
ANALYZE source.master_product;
ANALYZE source.client;
ANALYZE source.orders;
