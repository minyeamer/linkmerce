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

#} product_renewal_mapping AS (
  {{ core__product_renewal_mapping() }}
),{#

#} order_status_c24 AS (
  SELECT
      mall_id
    , order_item_code
    , MAX(CASE order_status_type WHEN 1 THEN 3 WHEN 3 THEN 1 ELSE 0 END) AS order_status
  FROM {{ source('cafe24', 'order_change') }}
  WHERE order_date BETWEEN {{ pg_batch_start_date() }} - 1 AND {{ pg_batch_end_date() }}
  GROUP BY mall_id, order_item_code
),{#

#} order_detail AS (
  SELECT
      ord.order_id
    , ord.order_item_code
    , ord.mall_id
    , COALESCE(
          rel.bundle_product_ids
        , mall.brand_id
        , '200000'
      ) AS bundle_product_ids
    , COALESCE(status_c24.order_status, 0) AS order_status
    , COALESCE(ord.order_quantity, 0) AS order_quantity
    , ord.order_dt::date AS order_date
  FROM {{ source('cafe24', 'order_detail') }} AS ord
  LEFT JOIN order_status_c24 AS status_c24
    ON (ord.mall_id = status_c24.mall_id) AND (ord.order_item_code = status_c24.order_item_code)
  LEFT JOIN {{ ref('relation__c24_var_to_sbn_ids') }} AS rel
    ON (ord.mall_id = rel.mall_id) AND (ord.variant_code = rel.variant_code)
  LEFT JOIN {{ source('cafe24', 'mall') }} AS mall
    ON ord.mall_id = mall.mall_id
  WHERE ord.order_dt >= {{ pg_batch_start_date() }}::timestamp without time zone
    AND ord.order_dt < ({{ pg_batch_end_date() }} + 1)::timestamp without time zone
),{#

#} exploded_product_order AS (
  SELECT
      ord.order_id
    , ord.order_item_code
    , ord.mall_id
    , COALESCE(
          renewal.product_id_old
        , split_part(bundle_product, ':', 1)
      ) AS product_id
    , (CASE
        WHEN split_part(bundle_product, ':', 2) ~ '^[0-9]+$'
          THEN split_part(bundle_product, ':', 2)::integer
        ELSE 1
      END) AS product_quantity
    , (CASE
        WHEN (ord.order_status = 0) AND (LEFT(bundle_product, 1) = '9') THEN 6
        ELSE ord.order_status
      END) AS order_status
    , ord.order_quantity
    , ord.order_date
  FROM order_detail AS ord
  CROSS JOIN LATERAL unnest(string_to_array(ord.bundle_product_ids, ',')) AS t(bundle_product)
  LEFT JOIN product_renewal_mapping AS renewal
    ON split_part(bundle_product, ':', 1) = renewal.product_id_new
      AND ord.order_date < renewal.renewal_date
),{#

#} order_count AS (
  SELECT
      order_id
    , order_item_code
    , mall_id
    , product_id
    , product_quantity
    , order_status
    , SUM(order_quantity) AS order_quantity
    , order_date
  FROM exploded_product_order
  GROUP BY order_id, order_item_code, order_date, mall_id, product_id, product_quantity, order_status
){#

#} SELECT * FROM order_count
