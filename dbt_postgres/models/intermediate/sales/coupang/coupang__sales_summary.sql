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

SELECT
    vendor_id
  , SUM(payment_amount) AS payment_amount
  , MAX(order_dt) AS max_order_dt
  , order_date
FROM (
  SELECT
      vendor_id
    , COALESCE(order_quantity, 0) * COALESCE(unit_price, 0) AS payment_amount
    , order_dt
    , order_dt::date AS order_date
  FROM {{ source('coupang', 'order_detail') }}
  WHERE order_dt >= {{ pg_batch_start_date() }}::timestamp without time zone
    AND order_dt < ({{ pg_batch_end_date() }} + 1)::timestamp without time zone{#

  #} UNION ALL{#

  #} SELECT
      vendor_id
    , COALESCE(order_quantity, 0) * COALESCE(unit_price, 0) AS payment_amount
    , payment_dt AS order_dt
    , payment_dt::date AS order_date
  FROM {{ source('coupang_rfm', 'order') }}
  WHERE payment_dt >= {{ pg_batch_start_date() }}::timestamp without time zone
    AND payment_dt < ({{ pg_batch_end_date() }} + 1)::timestamp without time zone
)
GROUP BY order_date, vendor_id
