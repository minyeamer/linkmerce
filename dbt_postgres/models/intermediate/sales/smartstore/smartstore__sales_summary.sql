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

#} order_status AS (
  SELECT
      product_order_id
    , MAX(order_status) AS order_status
  FROM {{ source('smartstore', 'order_status') }}
  WHERE payment_dt >= {{ pg_batch_start_date() }}::timestamp without time zone
    AND payment_dt < ({{ pg_batch_end_date() }} + 1)::timestamp without time zone
  GROUP BY product_order_id
),{#

#} sales_summary AS (
  SELECT
      ord.channel_seq
    , SUM(
        (COALESCE(ord.unit_price, 0) + COALESCE(ord.option_price, 0))
        * COALESCE(ord.order_quantity, 0)
        - COALESCE(ord.seller_discount_amount, 0)
      ) AS payment_amount
    , ord.payment_dt::date AS order_date
  FROM {{ source('smartstore', 'order_detail') }} AS ord
  LEFT JOIN order_status AS status
    ON ord.product_order_id = status.product_order_id
  WHERE ord.payment_dt >= {{ pg_batch_start_date() }}::timestamp without time zone
    AND ord.payment_dt < ({{ pg_batch_end_date() }} + 1)::timestamp without time zone
    AND ((status.order_status < 5) OR (status.order_status IS NULL))
  GROUP BY ord.payment_dt::date, ord.channel_seq
){#

#} SELECT * FROM sales_summary
