-- Загрузка d_customers
WITH last_load AS (
    SELECT COALESCE(MAX(load_dttm)::date, DATE '1900-01-01') AS last_load_date
    FROM dwh.load_dates_craftsman_report_datamart
)
INSERT INTO dwh.d_customers (
    customer_name,
    customer_address,
    customer_birthday,
    customer_email,
    load_dttm
)
SELECT DISTINCT
    s.customer_name,
    s.customer_address,
    s.customer_birthday,
    s.customer_email,
    CURRENT_TIMESTAMP
FROM source1.craft_market_wide s
JOIN last_load ll ON 1=1
WHERE
    (s.order_created_date >= ll.last_load_date OR s.order_completion_date >= ll.last_load_date)
    AND NOT EXISTS (
        SELECT 1 
        FROM dwh.d_customers d 
        WHERE d.customer_email = s.customer_email
    )
;

-- Данные из source2
WITH last_load AS (
    SELECT COALESCE(MAX(load_dttm)::date, DATE '1900-01-01') AS last_load_date
    FROM dwh.load_dates_craftsman_report_datamart
)
INSERT INTO dwh.d_customers (
    customer_name,
    customer_address,
    customer_birthday,
    customer_email,
    load_dttm
)
SELECT DISTINCT
    s.customer_name,
    s.customer_address,
    s.customer_birthday,
    s.customer_email,
    CURRENT_TIMESTAMP
FROM source2.craft_market_orders_customers s
JOIN last_load ll ON 1=1
WHERE
    (s.order_created_date >= ll.last_load_date OR s.order_completion_date >= ll.last_load_date)
    AND NOT EXISTS (
        SELECT 1 
        FROM dwh.d_customers d 
        WHERE d.customer_email = s.customer_email
    )
;

-- Данные из source3
WITH last_load AS (
    SELECT COALESCE(MAX(load_dttm)::date, DATE '1900-01-01') AS last_load_date
    FROM dwh.load_dates_craftsman_report_datamart
)
INSERT INTO dwh.d_customers (
    customer_name,
    customer_address,
    customer_birthday,
    customer_email,
    load_dttm
)
SELECT DISTINCT
    s.customer_name,
    s.customer_address,
    s.customer_birthday,
    s.customer_email,
    CURRENT_TIMESTAMP
FROM source3.craft_market_customers s
JOIN last_load ll ON 1=1

WHERE NOT EXISTS (
        SELECT 1 
        FROM dwh.d_customers d 
        WHERE d.customer_email = s.customer_email
    )
;


-- Загрузка d_products
WITH last_load AS (
    SELECT COALESCE(MAX(load_dttm)::date, DATE '1900-01-01') AS last_load_date
    FROM dwh.load_dates_craftsman_report_datamart
)
INSERT INTO dwh.d_products (
    product_name,
    product_description,
    product_type,
    product_price,
    load_dttm
)
SELECT DISTINCT
    s.product_name,
    s.product_description,
    s.product_type,
    s.product_price,
    CURRENT_TIMESTAMP
FROM source1.craft_market_wide s
JOIN last_load ll ON 1=1
WHERE
    (s.order_created_date >= ll.last_load_date OR s.order_completion_date >= ll.last_load_date)
    AND NOT EXISTS (
        SELECT 1 
        FROM dwh.d_products p
        WHERE p.product_name = s.product_name
          AND p.product_description = s.product_description
    )
;

-- source2:
WITH last_load AS (
    SELECT COALESCE(MAX(load_dttm)::date, DATE '1900-01-01') AS last_load_date
    FROM dwh.load_dates_craftsman_report_datamart
)
INSERT INTO dwh.d_products (
    product_name,
    product_description,
    product_type,
    product_price,
    load_dttm
)
SELECT DISTINCT
    s.product_name,
    s.product_description,
    s.product_type,
    s.product_price,
    CURRENT_TIMESTAMP
FROM source2.craft_market_masters_products s
JOIN last_load ll ON 1=1
-- Проверка на обновления/новые записи
WHERE NOT EXISTS (
    SELECT 1 
    FROM dwh.d_products p
    WHERE p.product_name = s.product_name
      AND p.product_description = s.product_description
);


-- Загрузка d_craftsmans
WITH last_load AS (
    SELECT COALESCE(MAX(load_dttm)::date, DATE '1900-01-01') AS last_load_date
    FROM dwh.load_dates_craftsman_report_datamart
)
INSERT INTO dwh.d_craftsmans (
    craftsman_name,
    craftsman_address,
    craftsman_birthday,
    craftsman_email,
    load_dttm
)
SELECT DISTINCT
    s.craftsman_name,
    s.craftsman_address,
    s.craftsman_birthday,
    s.craftsman_email,
    CURRENT_TIMESTAMP
FROM source1.craft_market_wide s
JOIN last_load ll ON 1=1
WHERE
    (s.order_created_date >= ll.last_load_date OR s.order_completion_date >= ll.last_load_date)
    AND NOT EXISTS (
        SELECT 1 
        FROM dwh.d_craftsmans c
        WHERE c.craftsman_email = s.craftsman_email
    )
;

-- source2
WITH last_load AS (
    SELECT COALESCE(MAX(load_dttm)::date, DATE '1900-01-01') AS last_load_date
    FROM dwh.load_dates_craftsman_report_datamart
)
INSERT INTO dwh.d_craftsmans (
    craftsman_name,
    craftsman_address,
    craftsman_birthday,
    craftsman_email,
    load_dttm
)
SELECT DISTINCT
    s.craftsman_name,
    s.craftsman_address,
    s.craftsman_birthday,
    s.craftsman_email,
    CURRENT_TIMESTAMP
FROM source2.craft_market_masters_products s
JOIN last_load ll ON 1=1
WHERE NOT EXISTS (
    SELECT 1 
    FROM dwh.d_craftsmans c
    WHERE c.craftsman_email = s.craftsman_email
)
;

-- source3
WITH last_load AS (
    SELECT COALESCE(MAX(load_dttm)::date, DATE '1900-01-01') AS last_load_date
    FROM dwh.load_dates_craftsman_report_datamart
)
INSERT INTO dwh.d_craftsmans (
    craftsman_name,
    craftsman_address,
    craftsman_birthday,
    craftsman_email,
    load_dttm
)
SELECT DISTINCT
    s.craftsman_name,
    s.craftsman_address,
    s.craftsman_birthday,
    s.craftsman_email,
    CURRENT_TIMESTAMP
FROM source3.craft_market_craftsmans s
JOIN last_load ll ON 1=1
WHERE NOT EXISTS (
    SELECT 1 
    FROM dwh.d_craftsmans c
    WHERE c.craftsman_email = s.craftsman_email
)
;


-- f_orders
WITH last_load AS (
    SELECT COALESCE(MAX(load_dttm)::date, DATE '1900-01-01') AS last_load_date
    FROM dwh.load_dates_craftsman_report_datamart
)
INSERT INTO dwh.f_orders (
    -- order_id генерируется автоматически
    product_id,
    craftsman_id,
    customer_id,
    order_created_date,
    order_completion_date,
    order_status,
    load_dttm
)
OVERRIDING SYSTEM VALUE
SELECT
    dp.product_id,
    dcft.craftsman_id,
    dcu.customer_id,
    s.order_created_date,
    s.order_completion_date,
    s.order_status,
    CURRENT_TIMESTAMP
FROM source1.craft_market_wide s
JOIN last_load ll ON 1=1
-- JOIN с d_products
JOIN dwh.d_products dp
  ON dp.product_name = s.product_name
  AND dp.product_description = s.product_description
-- JOIN с d_craftsmans
JOIN dwh.d_craftsmans dcft
  ON dcft.craftsman_email = s.craftsman_email
-- JOIN с d_customers
JOIN dwh.d_customers dcu
  ON dcu.customer_email = s.customer_email
WHERE
    (s.order_created_date >= ll.last_load_date OR s.order_completion_date >= ll.last_load_date)
    AND NOT EXISTS (
        SELECT 1 
        FROM dwh.f_orders fo
        WHERE fo.order_created_date = s.order_created_date
          AND fo.craftsman_id       = dcft.craftsman_id
          AND fo.customer_id        = dcu.customer_id
          AND fo.product_id         = dp.product_id
    )
;

-- source2
WITH last_load AS (
    SELECT COALESCE(MAX(load_dttm)::date, DATE '1900-01-01') AS last_load_date
    FROM dwh.load_dates_craftsman_report_datamart
)
INSERT INTO dwh.f_orders (
    product_id,
    craftsman_id,
    customer_id,
    order_created_date,
    order_completion_date,
    order_status,
    load_dttm
)
OVERRIDING SYSTEM VALUE
SELECT
    dp.product_id,
    dcft.craftsman_id,
    dcu.customer_id,
    s.order_created_date,
    s.order_completion_date,
    s.order_status,
    CURRENT_TIMESTAMP
FROM source2.craft_market_orders_customers s
JOIN last_load ll ON 1=1
JOIN dwh.d_products dp
  ON dp.product_id = s.product_id
JOIN dwh.d_craftsmans dcft
  ON dcft.craftsman_id = s.craftsman_id
JOIN dwh.d_customers dcu
  ON dcu.customer_email = s.customer_email
WHERE
    (s.order_created_date >= ll.last_load_date OR s.order_completion_date >= ll.last_load_date)
    AND NOT EXISTS (
        SELECT 1 
        FROM dwh.f_orders fo
        WHERE fo.order_created_date = s.order_created_date
          AND fo.craftsman_id      = dcft.craftsman_id
          AND fo.customer_id       = dcu.customer_id
          AND fo.product_id        = dp.product_id
    )
;

WITH last_load AS (
    SELECT COALESCE(MAX(load_dttm)::date, DATE '1900-01-01') AS last_load_date
    FROM dwh.load_dates_craftsman_report_datamart
)
INSERT INTO dwh.f_orders (
    product_id,
    craftsman_id,
    customer_id,
    order_created_date,
    order_completion_date,
    order_status,
    load_dttm
)
OVERRIDING SYSTEM VALUE
SELECT
    dp.product_id,
    dcft.craftsman_id,
    dcu.customer_id,
    s.order_created_date,
    s.order_completion_date,
    s.order_status,
    CURRENT_TIMESTAMP
FROM source3.craft_market_orders s
JOIN last_load ll ON 1=1
JOIN dwh.d_products dp
  ON dp.product_name = s.product_name 
  AND dp.product_description = s.product_description
JOIN dwh.d_craftsmans dcft
  ON dcft.craftsman_id = s.craftsman_id
JOIN dwh.d_customers dcu
  ON dcu.customer_id = s.customer_id
WHERE
    (s.order_created_date >= ll.last_load_date OR s.order_completion_date >= ll.last_load_date)
    AND NOT EXISTS (
        SELECT 1 
        FROM dwh.f_orders fo
        WHERE fo.order_created_date = s.order_created_date
          AND fo.craftsman_id      = dcft.craftsman_id
          AND fo.customer_id       = dcu.customer_id
          AND fo.product_id        = dp.product_id
    )
;


INSERT INTO dwh.load_dates_craftsman_report_datamart (load_dttm)
VALUES (CURRENT_DATE);
