{{
  config(
    materialized = 'partitioned_table',
    schema = 'xfm_stock',
    partition_by = {
      "field": "ymd",
      "data_type": "date",
      "granularity": "day"
    },
    partitions = pg_date_partitions('ds_start_date', 'ds_end_date')
  )
}}

WITH{#

#} ds_date_range AS (
  SELECT ymd::date
  FROM generate_series(
      {{ pg_batch_start_date() }}
    , {{ pg_batch_end_date() }}, INTERVAL '1 day'
  ) AS t(ymd)
),{#

#} delivery_product_mapping AS (
  {{ smartstore__delivery_product_mapping() }}
),{#

-- Step 1: prepare daily sold quantity

#} sabangnet_sold_qty_daily AS (
  SELECT
      product_id
    , 0 AS group_id
    , SUM(sku_quantity) AS sku_quantity
    , order_date
  FROM {{ ref('sabangnet__sales_daily') }}
  WHERE (order_date
      BETWEEN {{ pg_batch_start_date() }} - 30
      AND {{ pg_batch_end_date() }} - 1)
    AND (order_status = 0)
  GROUP BY order_date, product_id
),{#

#} smartstore_sold_qty_daily AS (
  SELECT
      product_id
    , group_id
    , SUM(sku_quantity) AS sku_quantity
    , order_date
  FROM (
    SELECT
        COALESCE(map.delivery_product_id, sales.product_id) AS product_id
      , (CASE WHEN sales.delivery_type = 7 THEN 1 ELSE 0 END) AS group_id
      , sales.sku_quantity
      , sales.order_date
    FROM {{ ref('smartstore__sales_daily') }} AS sales
    LEFT JOIN delivery_product_mapping AS map
      ON sales.delivery_type = map.delivery_type
        AND sales.product_id = map.original_product_id
    WHERE (sales.order_date
        BETWEEN {{ pg_batch_start_date() }} - 30
        AND {{ pg_batch_end_date() }} - 1)
      AND (sales.order_status = 0)
  ) AS t_
  GROUP BY order_date, product_id, group_id
),{#

#} coupang_rfm_sold_qty_daily AS (
  SELECT
      product_id
    , 2 AS group_id
    , SUM(sku_quantity) AS sku_quantity
    , order_date
  FROM {{ ref('coupang_rfm__sales_daily') }}
  WHERE (order_date
      BETWEEN {{ pg_batch_start_date() }} - 30
      AND {{ pg_batch_end_date() }} - 1)
    AND (order_status = 0)
  GROUP BY order_date, product_id
),{#

-- Step 2: aggregate sold quantity by channel

#} sold_qty_daily AS (
  SELECT
      order_date
    , product_id
    , SUM(sku_quantity) AS sold_qty
    , SUM((CASE WHEN group_id = 0 THEN sku_quantity ELSE NULL END)) AS sabangnet__sold_qty
    , SUM((CASE WHEN group_id = 1 THEN sku_quantity ELSE NULL END)) AS cj_eflexs__sold_qty
    , SUM((CASE WHEN group_id = 2 THEN sku_quantity ELSE NULL END)) AS coupang_rfm__sold_qty
  FROM (
    (SELECT * FROM sabangnet_sold_qty_daily)
    UNION ALL
    (SELECT * FROM smartstore_sold_qty_daily)
    UNION ALL
    (SELECT * FROM coupang_rfm_sold_qty_daily)
  ) AS t_
  GROUP BY order_date, product_id
),{#

-- Step 3: calculate rolling 30 days quantity

#} sold_qty_daily_30d AS (
  SELECT
      dates.ymd
    , qty.product_id
    , SUM(qty.sold_qty) AS sold_qty_30d
    , SUM(qty.sabangnet__sold_qty) AS sabangnet__sold_qty_30d
    , SUM(qty.cj_eflexs__sold_qty) AS cj_eflexs__sold_qty_30d
    , SUM(qty.coupang_rfm__sold_qty) AS coupang_rfm__sold_qty_30d
  FROM ds_date_range AS dates
  INNER JOIN sold_qty_daily AS qty
    ON qty.order_date BETWEEN dates.ymd - 30 AND dates.ymd - 1
  GROUP BY dates.ymd, qty.product_id
){#

#} SELECT * FROM sold_qty_daily_30d
