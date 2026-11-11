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

#} coupang_claim AS (
  SELECT
      vendor_id
    , shipment_box_id
    , option_id
    , SUM(quantity) AS quantity
  FROM {{ source('coupang', 'order_claim') }}
  WHERE order_dt >= {{ pg_batch_start_date() }}::timestamp without time zone
    AND order_dt < ({{ pg_batch_end_date() }} + 1)::timestamp without time zone
    AND claim_type IN (0, 1)
    AND is_success
  GROUP BY vendor_id, shipment_box_id, option_id
),{#

#} coupang_order AS (
  SELECT
      ord.vendor_id
    , (COALESCE(ord.order_quantity, 0) - COALESCE(clm.quantity, 0)) * COALESCE(ord.unit_price, 0) AS payment_amount
    , ord.order_dt
    , ord.order_dt::date AS order_date
  FROM {{ source('coupang', 'order_detail') }} AS ord
  LEFT JOIN coupang_claim AS clm
    ON ord.vendor_id = clm.vendor_id
      AND ord.shipment_box_id = clm.shipment_box_id
      AND ord.option_id = clm.option_id
  WHERE ord.order_dt >= {{ pg_batch_start_date() }}::timestamp without time zone
    AND ord.order_dt < ({{ pg_batch_end_date() }} + 1)::timestamp without time zone
),{#

#} coupang_rocket_order AS (
  SELECT
      vendor_id
    , COALESCE(order_quantity, 0) * COALESCE(unit_price, 0) AS payment_amount
    , payment_dt AS order_dt
    , payment_dt::date AS order_date
  FROM {{ source('coupang_rfm', 'order') }}
  WHERE payment_dt >= {{ pg_batch_start_date() }}::timestamp without time zone
    AND payment_dt < ({{ pg_batch_end_date() }} + 1)::timestamp without time zone
){#

#} SELECT
    vendor_id
  , SUM(payment_amount) AS payment_amount
  , MAX(order_dt) AS last_order_dt
  , order_date
FROM (
  (SELECT * FROM coupang_order)
  UNION ALL
  (SELECT * FROM coupang_rocket_order)
)
GROUP BY order_date, vendor_id
