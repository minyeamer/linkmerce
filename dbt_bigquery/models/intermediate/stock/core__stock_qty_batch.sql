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

-- Step 1: map sabangnet sku to ecount product

ecount_product AS (
  SELECT
      product_code
    , COALESCE(SPLIT(NULLIF(option_id, ''), '-')[SAFE_OFFSET(0)], '200000') AS product_id
    , NULLIF(option_id, '') AS option_id
    , COALESCE(SAFE.PARSE_DATE('%Y%m%d', expiration_date), DATE(2999, 12, 31)) AS expiration_date
    , updated_at
  FROM {{ source('ecount', 'product') }}
),

sbn_sku_to_eco_prd AS (
  SELECT
      product_id
    , expiration_date
    , product_code
  FROM ecount_product
  WHERE option_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY product_id, expiration_date
    ORDER BY updated_at DESC NULLS LAST, product_code ASC, option_id ASC
  ) = 1
),

-- Step 2: prepare batch stock quantity

ecount_stock_qty_batch AS (
  SELECT
      qty.ymd
    , qty.batch
    , COALESCE(prd.product_code, '') AS product_code
    , 0 AS group_id
    , qty.stock_quantity
  FROM {{ ref('ecount__stock_qty_batch') }} AS qty
  LEFT JOIN ecount_product AS prd
    ON qty.product_code = prd.product_code
  WHERE qty.ymd BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
),

cj_eflexs_stock_qty_batch AS (
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
  WHERE qty.ymd BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
),

coupang_rfm_stock_qty_batch AS (
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
  WHERE qty.ymd BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
),

-- Step 3: aggregate stock quantity by channel

stock_qty_batch AS (
  SELECT
      ymd
    , batch
    , product_code
    , SUM(IF(group_id = 0, stock_quantity, NULL)) AS ecount__stock_qty
    , SUM(IF(group_id = 1, stock_quantity, NULL)) AS cj_eflexs__stock_qty
    , SUM(IF(group_id = 2, stock_quantity, NULL)) AS coupang_rfm__stock_qty
    , SUM(stock_quantity) AS stock_qty
  FROM (
    (SELECT * FROM ecount_stock_qty_batch)
    UNION ALL
    (SELECT * FROM cj_eflexs_stock_qty_batch)
    UNION ALL
    (SELECT * FROM coupang_rfm_stock_qty_batch)
  ) AS t_
  GROUP BY ymd, batch, product_code
)

SELECT * FROM stock_qty_batch
