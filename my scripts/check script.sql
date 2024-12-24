-- Количество строк в каждом справочнике
SELECT COUNT(*) AS cnt_customers FROM dwh.d_customers;
SELECT COUNT(*) AS cnt_products  FROM dwh.d_products;
SELECT COUNT(*) AS cnt_craftsmans FROM dwh.d_craftsmans;

SELECT * FROM dwh.d_customers ORDER BY customer_id DESC LIMIT 5;
SELECT * FROM dwh.d_products ORDER BY product_id DESC LIMIT 5;
SELECT * FROM dwh.d_craftsmans ORDER BY craftsman_id DESC LIMIT 5;

-- Общее число записей в таблице фактов
SELECT COUNT(*) AS cnt_orders FROM dwh.f_orders;

SELECT * 
FROM dwh.f_orders
ORDER BY order_id DESC
LIMIT 5;

-- Таблица с датами загрузок
SELECT id, load_dttm
FROM dwh.load_dates_craftsman_report_datamart
ORDER BY id DESC
LIMIT 5;

-- Общее число записей в витрине
SELECT COUNT(*) AS cnt_datamart 
FROM dwh.craftsman_report_datamart;

SELECT COUNT(*) 
FROM dwh.f_orders
WHERE fo.order_created_date >= last_load.last_load_date;

SELECT 
    craftsman_id,
    report_period,
    craftsman_money,
    platform_money,
    count_order,
    top_product_category
FROM dwh.craftsman_report_datamart
ORDER BY id DESC
LIMIT 10;
