{{
  config(
    materialized = 'incremental',
    schema = 'xfm_stock',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      "field": "ymd",
      "data_type": "date",
      "granularity": "day"
    },
    partitions = bq_date_partitions('ds_start_date', 'ds_end_date'),
    require_partition_filter = true
  )
}}

WITH

ds_date_range AS (
  SELECT ymd
  FROM UNNEST(GENERATE_DATE_ARRAY(
      DATE('{{ var("ds_start_date") }}')
    , DATE('{{ var("ds_end_date") }}')
  )) AS ymd
),

delivery_product_mapping AS (
  {{ smartstore__delivery_product_mapping() }}
),

-- Step 1: prepare daily sold quantity

sabangnet_sold_qty_daily AS (
  SELECT
      product_id
    , 0 AS group_id
    , SUM(sku_quantity) AS sku_quantity
    , order_date
  FROM {{ ref('sabangnet__sales_daily') }}
  WHERE (order_date
      BETWEEN DATE_SUB(DATE('{{ var("ds_start_date") }}'), INTERVAL 30 DAY)
      AND DATE_SUB(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY))
    AND shop_id NOT IN ('chop0022', 'chop9022')
    AND (order_status = 0)
  GROUP BY order_date, product_id
),

smartstore_sold_qty_daily AS (
  SELECT
      product_id
    , group_id
    , SUM(sku_quantity) AS sku_quantity
    , order_date
  FROM (
    SELECT
        COALESCE(map.delivery_product_id, sales.product_id) AS product_id
      , IF(sales.delivery_type = 7, 1, 0) AS group_id
      , sales.sku_quantity
      , sales.order_date
    FROM {{ ref('smartstore__sales_daily') }} AS sales
    LEFT JOIN delivery_product_mapping AS map
      ON sales.delivery_type = map.delivery_type
        AND sales.product_id = map.original_product_id
    WHERE (sales.order_date
        BETWEEN DATE_SUB(DATE('{{ var("ds_start_date") }}'), INTERVAL 30 DAY)
        AND DATE_SUB(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY))
      AND (sales.order_status = 0)
  ) AS t_
  GROUP BY order_date, product_id, group_id
),

coupang_rfm_sold_qty_daily AS (
  SELECT
      product_id
    , 2 AS group_id
    , SUM(sku_quantity) AS sku_quantity
    , order_date
  FROM {{ ref('coupang_rfm__sales_daily') }}
  WHERE (order_date
      BETWEEN DATE_SUB(DATE('{{ var("ds_start_date") }}'), INTERVAL 30 DAY)
      AND DATE_SUB(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY))
    AND (order_status = 0)
  GROUP BY order_date, product_id
),

-- Step 2: aggregate sold quantity by channel

sold_qty_daily AS (
  SELECT
      product_id
    , SUM(IF(group_id = 0, sku_quantity, NULL)) AS sabangnet__sold_qty
    , SUM(IF(group_id = 1, sku_quantity, NULL)) AS cj_eflexs__sold_qty
    , SUM(IF(group_id = 2, sku_quantity, NULL)) AS coupang_rfm__sold_qty
    , SUM(sku_quantity) AS sold_qty
    , order_date
  FROM (
    (SELECT * FROM sabangnet_sold_qty_daily)
    UNION ALL
    (SELECT * FROM smartstore_sold_qty_daily)
    UNION ALL
    (SELECT * FROM coupang_rfm_sold_qty_daily)
  ) AS t_
  GROUP BY order_date, product_id
),

-- Step 3: calculate rolling 30 days quantity

sold_qty_daily_30d AS (
  SELECT
      qty.product_id
    , SUM(qty.sabangnet__sold_qty) AS sabangnet__sold_qty_30d
    , SUM(qty.cj_eflexs__sold_qty) AS cj_eflexs__sold_qty_30d
    , SUM(qty.coupang_rfm__sold_qty) AS coupang_rfm__sold_qty_30d
    , SUM(qty.sold_qty) AS sold_qty_30d
    , dates.ymd
  FROM ds_date_range AS dates
  INNER JOIN sold_qty_daily AS qty
    ON qty.order_date
      BETWEEN DATE_SUB(dates.ymd, INTERVAL 30 DAY)
      AND DATE_SUB(dates.ymd, INTERVAL 1 DAY)
  GROUP BY dates.ymd, qty.product_id
)

SELECT * FROM sold_qty_daily_30d
