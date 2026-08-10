{{
  config(
    materialized = 'incremental',
    schema = 'xfm_benchmark',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      "field": "payment_date",
      "data_type": "date",
      "granularity": "day"
    },
    partitions = bq_date_partitions('ds_start_date', 'ds_end_date'),
    require_partition_filter = true
  )
}}

WITH

-- Step 1: prepare product stock

base_stock AS (
  SELECT
      product_id
    , product_status
    , sales_price
    , stock_quantity
    , DATE(created_at) AS payment_date
  FROM {{ source('naver_shp', 'stock') }}
  WHERE created_at >= DATETIME(DATE_SUB(DATE('{{ var("ds_start_date") }}'), INTERVAL 28 DAY))
    AND created_at < DATETIME(DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 9 DAY))
  QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, DATE(created_at) ORDER BY created_at ASC) = 1
),

-- Step 2: prepare option stock and calculate next-day quantity

option_stock AS (
  SELECT
      opt.product_id
    , opt.option_id
    , prd.product_status
    , prd.sales_price + opt.option_price AS sales_price
    , opt.stock_quantity
    , LEAD(opt.stock_quantity) OVER (
        PARTITION BY opt.product_id, opt.option_id
        ORDER BY opt.payment_date
      ) AS next_stock_quantity
    , opt.payment_date
    , LEAD(opt.payment_date) OVER (
        PARTITION BY opt.product_id, opt.option_id
        ORDER BY opt.payment_date
      ) AS next_payment_date
  FROM (
    SELECT
        product_id
      , option_id
      , COALESCE(option_price, 0) AS option_price
      , stock_quantity
      , DATE(created_at) AS payment_date
    FROM {{ source('naver_shp', 'stock_detail') }}
    WHERE created_at >= DATETIME(DATE_SUB(DATE('{{ var("ds_start_date") }}'), INTERVAL 28 DAY))
      AND created_at < DATETIME(DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 9 DAY))
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, option_id, DATE(created_at) ORDER BY created_at ASC) = 1
  ) AS opt
  INNER JOIN base_stock AS prd
    ON opt.product_id = prd.product_id
      AND opt.payment_date = prd.payment_date
),

-- Step 3: filter non-option stock and calculate next-day quantity

product_stock AS (
  SELECT
      prd.product_id
    , prd.product_id AS option_id
    , prd.product_status
    , prd.sales_price
    , prd.stock_quantity
    , LEAD(prd.stock_quantity) OVER (
        PARTITION BY prd.product_id
        ORDER BY prd.payment_date
      ) AS next_stock_quantity
    , prd.payment_date
    , LEAD(prd.payment_date) OVER (
        PARTITION BY prd.product_id
        ORDER BY prd.payment_date
      ) AS next_payment_date
  FROM base_stock AS prd
  WHERE NOT EXISTS (
    SELECT 1
    FROM option_stock AS opt
    WHERE prd.product_id = opt.product_id
      AND prd.payment_date = opt.payment_date
  )
),

-- Step 4: concat two stock data and filter quantity by range

total_stock AS (
  SELECT
      product_id
    , option_id
    , product_status
    , sales_price
    , (stock_quantity - next_stock_quantity) AS payment_count
    , (stock_quantity - next_stock_quantity) * sales_price AS payment_amount
    , payment_date
  FROM (
    (SELECT * FROM product_stock)
    UNION ALL
    (SELECT * FROM option_stock)
  ) AS t_
  WHERE (sales_price > 0)
    AND (stock_quantity >= next_stock_quantity)
    AND (stock_quantity - next_stock_quantity < 10000)
    AND (payment_date = DATE_SUB(next_payment_date, INTERVAL 1 DAY))
),

-- Step 5: calculate outlier stats between previous 28-days and next 7-days

outlier_stats AS (
  SELECT DISTINCT
      cur.payment_date
    , cur.product_id
    , cur.option_id
    , COUNT(NULLIF(prev.payment_count, 0)) OVER (
        PARTITION BY cur.payment_date, cur.product_id, cur.option_id
      ) AS effective_days_in
    , PERCENTILE_CONT(NULLIF(prev.payment_count, 0), 0.25) OVER (
          PARTITION BY cur.payment_date, cur.product_id, cur.option_id
      ) AS payment_count_q1
    , PERCENTILE_CONT(NULLIF(prev.payment_count, 0), 0.5) OVER (
          PARTITION BY cur.payment_date, cur.product_id, cur.option_id
      ) AS payment_count_q2
    , PERCENTILE_CONT(NULLIF(prev.payment_count, 0), 0.75) OVER (
        PARTITION BY cur.payment_date, cur.product_id, cur.option_id
      ) AS payment_count_q3
  FROM total_stock AS cur
  LEFT JOIN total_stock AS prev
    ON cur.product_id = prev.product_id
      AND cur.option_id = prev.option_id
      AND prev.payment_date >= DATE_SUB(cur.payment_date, INTERVAL 28 DAY)
      AND prev.payment_date != cur.payment_date
      AND prev.payment_date <= DATE_ADD(cur.payment_date, INTERVAL 7 DAY)
),

-- Step 6: remove stock correction outliers

daily_sales AS (
  SELECT
      base.product_id
    , base.option_id
    , base.sales_price
    , base.payment_count
    , base.payment_amount
    , base.payment_date
  FROM total_stock AS base
  INNER JOIN outlier_stats AS stat
    ON base.payment_date = stat.payment_date
      AND base.product_id = stat.product_id
      AND base.option_id = stat.option_id
  WHERE (base.payment_date
      BETWEEN DATE('{{ var("ds_start_date") }}')
          AND DATE('{{ var("ds_end_date") }}'))
    AND (base.product_status = 0)
    AND (CASE
          WHEN base.payment_count < 100
            THEN TRUE
          WHEN stat.effective_days_in < 3
            THEN base.payment_count < 1000
          WHEN stat.effective_days_in < 7
            THEN base.payment_count < GREATEST(
                  CAST(ROUND(10 * stat.payment_count_q2, 0) AS INT64)
                , 100
              )
          ELSE base.payment_count < GREATEST(
                  CAST(ROUND(
                    stat.payment_count_q3 + 5 * (stat.payment_count_q3 - stat.payment_count_q1)
                  , 0) AS INT64)
                , 100
              )
        END)
)

SELECT * FROM daily_sales
