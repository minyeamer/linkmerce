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

#} /* order_status IN (0, 1, 2, 3, 5, 6) */{#

#} order_status_smt AS (
  SELECT
      smt.product_order_id
    , MAX(smt.order_status) AS order_status
  FROM {{ source('smartstore', 'order_status') }} AS smt
  WHERE smt.payment_dt >= {{ pg_batch_start_date() }}::timestamp without time zone
    AND smt.payment_dt < ({{ pg_batch_end_date() }} + 1)::timestamp without time zone
  GROUP BY smt.product_order_id
),{#

#} order_status_cor AS (
  SELECT
      cor.order_id::bigint AS order_id
    , MAX(cor.order_status) AS order_status
  FROM {{ source('core', 'order_status') }} AS cor
  WHERE cor.order_date BETWEEN {{ pg_batch_start_date() }} AND {{ pg_batch_end_date() }}
    AND cor.shop_name = '스마트스토어'
    AND cor.order_id ~ '^[0-9]+$'
  GROUP BY cor.order_id
),{#

#} bundle_product_order AS (
  SELECT
      ord.order_id
    , ord.product_order_id
    , COALESCE(
          rel.bundle_product_ids
        , chl.brand_id
        , '200000'
      ) AS bundle_product_ids
    , (CASE
        WHEN status_cor.order_status IS NOT NULL THEN status_cor.order_status
        WHEN status_smt.order_status = 7 THEN 1
        WHEN status_smt.order_status = 5 THEN 2
        WHEN status_smt.order_status IN (6, 8) THEN 3
        ELSE 0
      END) AS order_status
    , (CASE WHEN ord.delivery_type = 7 THEN 7 ELSE 0 END) AS delivery_type
    , COALESCE(ord.order_quantity, 0) AS order_quantity
    , (ord.payment_dt)::date AS order_date
  FROM {{ source('smartstore', 'order_detail') }} AS ord
  LEFT JOIN {{ ref('relation__smt_opt_to_sbn_ids') }} AS rel
    ON ord.option_id = rel.option_id
  LEFT JOIN {{ source('smartstore', 'channel') }} AS chl
    ON ord.channel_seq = chl.channel_seq
  LEFT JOIN order_status_smt AS status_smt
    ON ord.product_order_id = status_smt.product_order_id
  LEFT JOIN order_status_cor AS status_cor
    ON ord.order_id = status_cor.order_id
  WHERE ord.payment_dt >= {{ pg_batch_start_date() }}::timestamp without time zone
    AND ord.payment_dt < ({{ pg_batch_end_date() }} + 1)::timestamp without time zone
),{#

#} exploded_product_order AS (
  SELECT
      ord.order_id
    , ord.product_order_id
    , (string_to_array(bundle_product, ':'))[1] AS product_id
    , (CASE
        WHEN (ord.order_status = 0) AND (LEFT(bundle_product, 1) = '9')
          THEN 3
        ELSE ord.order_status
      END) AS order_status
    , ord.delivery_type
    , ord.order_quantity
    , ord.order_date
  FROM bundle_product_order AS ord
  CROSS JOIN LATERAL unnest(string_to_array(ord.bundle_product_ids, ',')) AS t(bundle_product)
),{#

#} order_count AS (
  SELECT
      order_id
    , product_order_id
    , product_id
    , delivery_type
    , order_status
    , SUM(order_quantity) AS order_quantity
    , order_date
  FROM exploded_product_order
  GROUP BY order_id, product_order_id, order_date, product_id, delivery_type, order_status
){#

#} SELECT * FROM order_count
