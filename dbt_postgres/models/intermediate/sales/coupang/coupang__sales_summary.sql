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
    ord.vendor_id
  , SUM(COALESCE(order_quantity, 0) * COALESCE(unit_price, 0)) AS payment_amount
  , MAX(ord.order_dt) AS max_order_dt
  , ord.order_dt::date AS order_date
FROM {{ source('coupang', 'order_detail') }} AS ord
WHERE ord.order_dt >= {{ pg_batch_start_date() }}::timestamp without time zone
  AND ord.order_dt < ({{ pg_batch_end_date() }} + 1)::timestamp without time zone
GROUP BY ord.order_dt::date, ord.vendor_id
