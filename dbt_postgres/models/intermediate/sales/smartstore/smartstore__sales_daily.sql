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

-- order_status IN (0, 1, 2, 3, 5, 6)

#} order_delivery AS (
  SELECT
      product_order_id
    , MAX(invoice_no) AS invoice_no
  FROM {{ source('smartstore', 'order_delivery') }}
  WHERE payment_dt >= {{ pg_batch_start_date() }}::timestamp without time zone
    AND payment_dt < ({{ pg_batch_end_date() }} + 1)::timestamp without time zone
  GROUP BY product_order_id
),{#

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

#} order_detail AS (
  SELECT
      (ord.payment_dt)::date AS order_date
    -- Split partitions
    , ord.order_id
    , ord.product_order_id
    , dlv.invoice_no
    -- Sales dimensions
    , ord.channel_seq
    , COALESCE(
          rel.bundle_product_ids
        , chl.brand_id
        , '200000'
      ) AS bundle_product_ids
    , (CASE WHEN ord.delivery_type = 7 THEN 7 ELSE 0 END) AS delivery_type
    , (CASE
        WHEN status_cor.order_status IS NOT NULL THEN status_cor.order_status
        WHEN status_smt.order_status = 7 THEN 1
        WHEN status_smt.order_status = 5 THEN 2
        WHEN status_smt.order_status IN (6, 8) THEN 3
        ELSE 0
      END) AS order_status
    -- Sales metrics
    , COALESCE(ord.order_quantity, 0) AS order_quantity
    , ((COALESCE(ord.unit_price, 0) + COALESCE(ord.option_price, 0))
        * COALESCE(ord.order_quantity, 0)
        - COALESCE(ord.seller_discount_amount, 0)
      ) AS payment_amount
    , COALESCE(ord.supply_amount, 0) AS supply_amount
  FROM {{ source('smartstore', 'order_detail') }} AS ord
  LEFT JOIN order_delivery AS dlv
    ON ord.product_order_id = dlv.product_order_id
  -- Resolve bundle_product_ids
  LEFT JOIN {{ ref('relation__smt_opt_to_sbn_ids') }} AS rel
    ON ord.option_id = rel.option_id
  LEFT JOIN {{ source('smartstore', 'channel') }} AS chl
    ON ord.channel_seq = chl.channel_seq
  -- Resolve order_status
  LEFT JOIN order_status_smt AS status_smt
    ON ord.product_order_id = status_smt.product_order_id
  LEFT JOIN order_status_cor AS status_cor
    ON ord.order_id = status_cor.order_id
  WHERE ord.payment_dt >= {{ pg_batch_start_date() }}::timestamp without time zone
    AND ord.payment_dt < ({{ pg_batch_end_date() }} + 1)::timestamp without time zone
),

{%- set input_dimensions = [
    'order_id'
  , 'product_order_id'
  , 'invoice_no'
  , 'channel_seq'
  , 'delivery_type'
] -%}

{{ explode_product_order(
    input_dimensions = input_dimensions
) }},{#

#} {{ split_sales_amounts(
    input_dimensions = input_dimensions,
    order_partitions = ['product_order_id']
) }},{#

#} {{ split_delivery_fee(
    input_dimensions = input_dimensions,
    delivery_partitions = ['order_id', 'invoice_no'],
    cj_invoice_key = 'invoice_no',
    cj_order_key = 'order_id',
    use_eflexs_invoice = true
) }},{#

#} sales_daily AS (
  SELECT
      channel_seq
    , product_id
    , product_quantity
    , delivery_type
    , order_status
    , SUM(order_quantity) AS order_quantity
    , SUM(payment_amount) AS payment_amount
    , SUM(supply_amount) AS supply_amount
    , SUM(supply_cost) AS supply_cost
    , SUM(delivery_fee) AS delivery_fee
    , order_date
  FROM product_order_with_delivery_fee
  GROUP BY order_date, channel_seq, product_id, product_quantity, delivery_type, order_status
){#

#} SELECT * FROM sales_daily
