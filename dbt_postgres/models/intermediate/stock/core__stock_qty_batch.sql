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

-- Step 1: map sabangnet sku to ecount product

#} ecount_product AS (
  SELECT
      product_code
    , COALESCE((string_to_array(NULLIF(option_id, ''), '-'))[1], '200000') AS product_id
    , NULLIF(option_id, '') AS option_id
    , (CASE
        WHEN expiration_date ~ '^[0-9]{8}$'
          THEN to_date(expiration_date, 'YYYYMMDD')
        ELSE DATE '2999-12-31'
      END) AS expiration_date
    , updated_at
  FROM {{ source('ecount', 'product') }}
),{#

#} sbn_sku_to_eco_prd AS (
  SELECT DISTINCT ON (product_id, expiration_date)
      product_id
    , expiration_date
    , product_code
  FROM ecount_product
  WHERE option_id IS NOT NULL
  ORDER BY product_id, expiration_date, updated_at DESC NULLS LAST, product_code ASC, option_id ASC
),{#

-- Step 2: prepare batch stock quantity

#} ecount_stock_qty_batch AS (
  SELECT
      qty.ymd
    , qty.batch
    , COALESCE(prd.product_code, '') AS product_code
    , 0 AS group_id
    , qty.stock_quantity
  FROM {{ ref('ecount__stock_qty_batch') }} AS qty
  LEFT JOIN ecount_product AS prd
    ON qty.product_code = prd.product_code
  WHERE qty.ymd BETWEEN {{ pg_batch_start_date() }} AND {{ pg_batch_end_date() }}
),{#

#} cj_eflexs_stock_qty_batch AS (
  SELECT
      qty.ymd
    , qty.batch
    , COALESCE(rel.product_code, '') AS product_code
    , 1 AS group_id
    , qty.stock_quantity
  FROM {{ ref('cj_eflexs__stock_qty_batch') }} AS qty
  LEFT JOIN sbn_sku_to_eco_prd AS rel
    ON qty.product_id = rel.product_id
      AND qty.expiration_date = rel.expiration_date
  WHERE qty.ymd BETWEEN {{ pg_batch_start_date() }} AND {{ pg_batch_end_date() }}
),{#

#} coupang_rfm_stock_qty_batch AS (
  SELECT
      qty.ymd
    , qty.batch
    , COALESCE(rel.product_code, '') AS product_code
    , 2 AS group_id
    , qty.stock_quantity
  FROM {{ ref('coupang_rfm__stock_qty_batch') }} AS qty
  LEFT JOIN sbn_sku_to_eco_prd AS rel
    ON qty.product_id = rel.product_id
      AND qty.expiration_date = rel.expiration_date
  WHERE qty.ymd BETWEEN {{ pg_batch_start_date() }} AND {{ pg_batch_end_date() }}
),{#

-- Step 3: aggregate stock quantity by channel

#} stock_qty_batch AS (
  SELECT
      ymd
    , batch
    , product_code
    , SUM(stock_quantity) AS stock_qty
    , SUM((CASE WHEN group_id = 0 THEN stock_quantity ELSE NULL END)) AS ecount__stock_qty
    , SUM((CASE WHEN group_id = 1 THEN stock_quantity ELSE NULL END)) AS cj_eflexs__stock_qty
    , SUM((CASE WHEN group_id = 2 THEN stock_quantity ELSE NULL END)) AS coupang_rfm__stock_qty
  FROM (
    (SELECT * FROM ecount_stock_qty_batch)
    UNION ALL
    (SELECT * FROM cj_eflexs_stock_qty_batch)
    UNION ALL
    (SELECT * FROM coupang_rfm_stock_qty_batch)
  ) AS t_
  GROUP BY ymd, batch, product_code
){#

#} SELECT * FROM stock_qty_batch
