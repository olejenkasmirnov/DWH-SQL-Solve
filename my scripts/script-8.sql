WITH last_load AS (
    SELECT COALESCE(MAX(load_dttm)::date, DATE '1900-01-01') AS last_load_date
    FROM dwh.load_dates_craftsman_report_datamart
),
calc AS (
    SELECT
        c.craftsman_id,
        c.craftsman_name,
        c.craftsman_address,
        c.craftsman_birthday,
        c.craftsman_email,

        
        -- Сумма денег мастера = 90% от суммы цен
        0.9 * SUM(dp.product_price)::numeric(15,2) AS craftsman_money,
        -- Сумма денег платформы = 10%
        0.1 * SUM(dp.product_price)::numeric(15,2) AS platform_money,

        COUNT(*) AS count_order,
        AVG(dp.product_price)::numeric(10,2) AS avg_price_order,

        -- Средний возраст покупателя
        AVG(EXTRACT(YEAR FROM AGE(CURRENT_DATE, dcu.customer_birthday)))::numeric(3,1)
            AS avg_age_customer,

        -- Медианное время (дни) от создания до завершения
        PERCENTILE_CONT(0.5)
          WITHIN GROUP (ORDER BY (fo.order_completion_date - fo.order_created_date))
          AS median_time_order_completed,

        -- Наиболее популярная категория (подзапрос)
        FIRST_VALUE(dp.product_type) OVER (
            PARTITION BY c.craftsman_id, DATE_TRUNC('month', fo.order_created_date)
            ORDER BY COUNT(*) DESC
        ) AS top_product_category,

        -- Кол-во заказов в каждом статусе
        SUM(CASE WHEN fo.order_status = 'created'     THEN 1 ELSE 0 END) AS count_order_created,
        SUM(CASE WHEN fo.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress,
        SUM(CASE WHEN fo.order_status = 'delivery'    THEN 1 ELSE 0 END) AS count_order_delivery,
        SUM(CASE WHEN fo.order_status = 'done'        THEN 1 ELSE 0 END) AS count_order_done,
        SUM(CASE WHEN fo.order_status <> 'done'       THEN 1 ELSE 0 END) AS count_order_not_done,

        -- Период в формате ГГГГ-ММ
        TO_CHAR(DATE_TRUNC('month', fo.order_created_date), 'YYYY-MM') AS report_period

    FROM dwh.f_orders fo
    JOIN dwh.d_craftsmans c  ON c.craftsman_id   = fo.craftsman_id
    JOIN dwh.d_products dp   ON dp.product_id    = fo.product_id
    JOIN dwh.d_customers dcu ON dcu.customer_id  = fo.customer_id

    GROUP BY
        c.craftsman_id,   
        c.craftsman_name,
        c.craftsman_address,
        c.craftsman_birthday,
        c.craftsman_email,
        DATE_TRUNC('month', fo.order_created_date),
        dp.product_type
)
INSERT INTO dwh.craftsman_report_datamart (
    craftsman_id,
    craftsman_name,
    craftsman_address,
    craftsman_birthday,
    craftsman_email,
    craftsman_money,
    platform_money,
    count_order,
    avg_price_order,
    avg_age_customer,
    median_time_order_completed,
    top_product_category,
    count_order_created,
    count_order_in_progress,
    count_order_delivery,
    count_order_done,
    count_order_not_done,
    report_period
)
select DISTINCT
    craftsman_id,
    craftsman_name,
    craftsman_address,
    craftsman_birthday,
    craftsman_email,
    craftsman_money,
    platform_money,
    count_order,
    avg_price_order,
    avg_age_customer,
    median_time_order_completed,
    top_product_category,
    count_order_created,
    count_order_in_progress,
    count_order_delivery,
    count_order_done,
    count_order_not_done,
    report_period
FROM calc
-- Чтобы не дублировать запись за тот же craftsman и тот же период
WHERE NOT EXISTS (
    SELECT 1
    FROM dwh.craftsman_report_datamart t
    WHERE t.craftsman_id  = calc.craftsman_id
      AND t.report_period = calc.report_period
);