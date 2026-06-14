{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      "field": "order_date",
      "data_type": "date",
      "granularity": "day"
    },
    partitions = bq_date_partitions('ds_start_date', 'ds_end_date'),
    require_partition_filter = false
  )
}}

WITH

order_status_smt AS (
  SELECT
      smt.product_order_id
    , MAX(smt.order_status) AS order_status
  FROM {{ source('smartstore', 'order_status') }} AS smt
  WHERE smt.payment_dt >= DATETIME('{{ var("ds_start_date") }}')
    AND smt.payment_dt < DATETIME(DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY))
  GROUP BY smt.product_order_id
),

order_status_sbn AS (
  SELECT
      SAFE_CAST(sbn.order_id AS INT64) AS order_id
    , MAX(CASE
        WHEN sbn.order_status = '반품' THEN 1
        WHEN sbn.order_status = '교환' THEN 2
        WHEN sbn.order_status = '빈박스' THEN 5
        ELSE NULL
      END) AS order_status
  FROM {{ source('sabangnet', 'order_status') }} AS sbn
  WHERE sbn.order_date >= DATE('{{ var("ds_start_date") }}')
    AND sbn.order_date < DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY)
    AND sbn.shop_name = '스마트스토어'
    AND SAFE_CAST(sbn.order_id AS INT64) IS NOT NULL
  GROUP BY order_id
),

bundle_product_order AS (
  SELECT
      ord.order_id
    , ord.product_order_id
    , COALESCE(
          rel.bundle_product_ids
        , chl.brand_id
        , '200000'
      ) AS bundle_product_ids
    , (CASE
        WHEN status_smt.order_status IN (6, 8) THEN -1
        WHEN status_sbn.order_status IS NOT NULL THEN status_sbn.order_status
        WHEN status_smt.order_status = 7 THEN 1
        WHEN status_smt.order_status = 5 THEN 2
        ELSE 0
      END) AS order_status
    , IF(ord.delivery_type = 7, 7, 0) AS delivery_type
    , COALESCE(ord.order_quantity, 0) AS order_quantity
    , DATE(ord.payment_dt) AS order_date
  FROM {{ source('smartstore', 'order_detail') }} AS ord
  LEFT JOIN {{ ref('relation__smt_opt_to_sbn_ids') }} AS rel
    ON ord.option_id = rel.option_id
  LEFT JOIN {{ source('smartstore', 'channel') }} AS chl
    ON ord.channel_seq = chl.channel_seq
  LEFT JOIN order_status_smt AS status_smt
    ON ord.product_order_id = status_smt.product_order_id
  LEFT JOIN order_status_sbn AS status_sbn
    ON ord.order_id = status_sbn.order_id
  WHERE ord.payment_dt >= DATETIME('{{ var("ds_start_date") }}')
    AND ord.payment_dt < DATETIME(DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY))
),

exploded_product_order AS (
  SELECT
      ord.order_id
    , ord.product_order_id
    , SPLIT(bundle_product, ':')[SAFE_OFFSET(0)] AS product_id
    , IF((ord.order_status = 0) AND (LEFT(bundle_product, 1) = '9'), 3, ord.order_status) AS order_status
    , ord.delivery_type
    , ord.order_quantity
    , ord.order_date
  FROM bundle_product_order AS ord
  CROSS JOIN UNNEST(SPLIT(ord.bundle_product_ids, ',')) AS bundle_product
),

order_count AS (
  SELECT
      order_id
    , product_order_id
    , product_id
    , delivery_type
    , order_status
    , order_quantity
    , order_date
  FROM (
    SELECT
        order_id
      , product_order_id
      , product_id
      , MAX(delivery_type) AS delivery_type
      , MAX(order_status) AS order_status
      , SUM(order_quantity) AS order_quantity
      , MAX(order_date) AS order_date
    FROM exploded_product_order
    GROUP BY order_id, product_order_id, product_id
  ) AS t_
  WHERE order_status = 0 AND order_quantity > 0
)

SELECT * FROM order_count
