{{
  config(
    materialized = 'partitioned_table',
    schema = 'xfm_sales',
    partition_by = {
      "field": "order_date",
      "data_type": "date",
      "granularity": "day"
    },
    partitions = pg_date_partitions('ds_start_date', 'ds_end_date')
  )
}}

WITH{#

-- order_status IN (0, 1, 3, 6)

#} rocket_sales AS (
  SELECT
      order_id
    , option_id
    , vendor_id
    , MAX(settlement_type) AS order_status
    , SUM(order_quantity) AS order_quantity
    , MAX(sales_date) AS sales_date
  FROM {{ source('coupang_rfm', 'sales') }}
  WHERE sales_date
    BETWEEN DATE '{{ pg_week_start_date("ds_start_date") }}'
    AND DATE '{{ pg_week_end_date("ds_end_date") }}'
  GROUP BY order_id, option_id, vendor_id
),{#

#} bundle_product_order AS (
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
  WHERE ord.sales_date BETWEEN {{ pg_batch_start_date() }} AND {{ pg_batch_end_date() }}
    AND ord.order_quantity != 0
),{#

#} exploded_product_order AS (
  SELECT
      ord.order_id
    , (string_to_array(bundle_product, ':'))[1] AS product_id
    , (CASE
        WHEN (ord.order_status = 0) AND (LEFT(bundle_product, 1) = '9') THEN 6
        ELSE LEAST(ord.order_status, 3)
      END) AS order_status
    , ord.order_quantity
    , ord.order_date
  FROM bundle_product_order AS ord
  CROSS JOIN LATERAL unnest(string_to_array(ord.bundle_product_ids, ',')) AS t(bundle_product)
),{#

#} order_count AS (
  SELECT
      order_id
    , product_id
    , order_status
    , SUM(order_quantity) AS order_quantity
    , order_date
  FROM exploded_product_order
  GROUP BY order_id, order_date, product_id, order_status
){#

#} SELECT * FROM order_count
