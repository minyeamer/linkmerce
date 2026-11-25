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

WITH

order_delivery AS (
  SELECT
      mall_id
    , order_item_code
    , MAX(invoice_no) AS invoice_no
  FROM {{ source('cafe24', 'order_delivery') }}
  WHERE order_dt >= DATETIME('{{ var("ds_start_date") }}')
    AND order_dt < DATETIME(DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY))
  GROUP BY mall_id, order_item_code
),

order_status_c24 AS (
  SELECT
      mall_id
    , order_item_code
    , MAX(CASE order_status_type WHEN 1 THEN 3 WHEN 3 THEN 1 ELSE 0 END) AS order_status
  FROM {{ source('cafe24', 'order_change') }}
  WHERE order_date
    BETWEEN DATE_SUB(DATE('{{ var("ds_start_date") }}'), INTERVAL 1 DAY)
      AND DATE('{{ var("ds_end_date") }}')
  GROUP BY mall_id, order_item_code
),

settlement_rate AS (
  SELECT 1.0 - commission_rate AS rate
  FROM {{ source('sabangnet', 'shop') }}
  WHERE shop_id = 'shop0298'
  LIMIT 1
),

order_detail AS (
  SELECT
      DATE(ord.order_dt) AS order_date
    -- Split partitions
    , ord.order_id
    , ord.order_item_code
    , dlv.invoice_no
    -- Sales dimensions
    , ord.mall_id
    , COALESCE(
          rel.bundle_product_ids
        , mall.brand_id
        , '200000'
      ) AS bundle_product_ids
    , COALESCE(status_c24.order_status, 0) AS order_status
      -- Sales metrics
    , COALESCE(ord.order_quantity, 0) AS order_quantity
    , ((COALESCE(ord.product_price, 0) + COALESCE(ord.option_price, 0))
        * COALESCE(ord.order_quantity, 0)
        - COALESCE(ord.additional_discount_price, 0)
        - COALESCE(ord.coupon_discount_price, 0)
        - COALESCE(ord.app_item_discount_amount, 0)
      ) AS payment_amount
    , CAST(ROUND(((COALESCE(ord.product_price, 0) + COALESCE(ord.option_price, 0))
        * COALESCE(ord.order_quantity, 0)
        - COALESCE(ord.additional_discount_price, 0)
        - COALESCE(ord.coupon_discount_price, 0)
        - COALESCE(ord.app_item_discount_amount, 0)
      ) * COALESCE((SELECT rate FROM settlement_rate), 1.0)) AS INTEGER) AS supply_amount
  FROM {{ source('cafe24', 'order_detail') }} AS ord
  LEFT JOIN order_delivery AS dlv
    ON (ord.mall_id = dlv.mall_id) AND (ord.order_item_code = dlv.order_item_code)
  LEFT JOIN order_status_c24 AS status_c24
    ON (ord.mall_id = status_c24.mall_id) AND (ord.order_item_code = status_c24.order_item_code)
  -- Resolve bundle_product_ids
  LEFT JOIN {{ ref('relation__c24_var_to_sbn_ids') }} AS rel
    ON (ord.mall_id = rel.mall_id) AND (ord.variant_code = rel.variant_code)
  LEFT JOIN {{ source('cafe24', 'mall') }} AS mall
    ON ord.mall_id = mall.mall_id
  WHERE ord.order_dt >= DATETIME('{{ var("ds_start_date") }}')
    AND ord.order_dt < DATETIME(DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY))
),

{%- set input_dimensions = [
    'order_id'
  , 'order_item_code'
  , 'invoice_no'
  , 'mall_id'
] -%}

{{ explode_product_order(
    input_dimensions = input_dimensions
) }},

{{ split_sales_amounts(
    input_dimensions = input_dimensions,
    order_partitions = ['mall_id', 'order_item_code']
) }},

{{ split_delivery_fee(
    input_dimensions = input_dimensions,
    delivery_partitions = ['mall_id', 'order_id', 'invoice_no'],
    cj_invoice_key = 'invoice_no',
    cj_order_key = 'order_id'
) }},

sales_daily AS (
  SELECT
      mall_id
    , product_id
    , product_quantity
    , order_status
    , SUM(order_quantity) AS order_quantity
    , SUM(payment_amount) AS payment_amount
    , SUM(supply_amount) AS supply_amount
    , SUM(supply_cost) AS supply_cost
    , SUM(delivery_fee) AS delivery_fee
    , order_date
  FROM product_order_with_delivery_fee
  GROUP BY order_date, mall_id, product_id, product_quantity, order_status
)

SELECT * FROM sales_daily
