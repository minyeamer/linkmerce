{{
  config(
    materialized = 'incremental',
    schema = 'xfm_sales',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      "field": "order_date",
      "data_type": "date",
      "granularity": "day"
    },
    partitions = bq_date_partitions('ds_start_date', 'ds_end_date'),
    require_partition_filter = true
  )
}}

WITH rocket_sales AS (
  SELECT
      order_id
    , option_id
    , vendor_id
    , MAX(settlement_type) AS order_status
    , SUM(order_quantity) AS order_quantity
    , MAX(sales_date) AS sales_date
  FROM {{ source('coupang_rfm', 'sales') }}
  WHERE sales_date
    BETWEEN DATE('{{ bq_week_start_date("ds_start_date") }}')
    AND DATE('{{ bq_week_end_date("ds_end_date") }}')
  GROUP BY order_id, option_id, vendor_id
),

bundle_product_order AS (
  SELECT
      ord.order_id
    , ord.option_id
    , COALESCE(
          rel.bundle_product_ids
        , vdr.bundle_brand_ids
        , '200000'
      ) AS bundle_product_ids
    , ord.order_status
    , ord.order_quantity
    , ord.sales_date AS order_date
  FROM rocket_sales AS ord
  LEFT JOIN {{ source('relation', 'cpg_opt_to_sbn_ids') }} AS rel
    ON ord.option_id = rel.option_id
  LEFT JOIN {{ source('coupang', 'vendor') }} AS vdr
    ON ord.vendor_id = vdr.vendor_id
  WHERE ord.sales_date BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
    AND ord.order_quantity != 0
),

exploded_product_order AS (
  SELECT
      ord.order_id
    , SPLIT(bundle_product, ':')[SAFE_OFFSET(0)] AS product_id
    , (CASE
        WHEN (ord.order_status = 0) AND (LEFT(bundle_product, 1) = '9') THEN 3
        ELSE ord.order_status
      END) AS order_status
    , ord.order_quantity
    , ord.order_date
  FROM bundle_product_order AS ord
  CROSS JOIN UNNEST(SPLIT(ord.bundle_product_ids, ',')) AS bundle_product
),

order_count AS (
  SELECT
      order_id
    , product_id
    , MAX(order_status) AS order_status
    , SUM(order_quantity) AS order_quantity
    , MAX(order_date) AS order_date
  FROM exploded_product_order
  GROUP BY order_id, product_id
)

SELECT *
FROM order_count
WHERE order_status = 0 AND order_quantity != 0
