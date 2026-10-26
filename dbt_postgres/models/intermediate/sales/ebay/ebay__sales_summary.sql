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
    site_type
  , seller_id
  , SUM(COALESCE(payment_amount, 0)) AS payment_amount
  , MAX(payment_dt) AS max_order_dt
  , payment_dt::date AS order_date
FROM {{ source('ebay', 'order') }}
WHERE payment_dt >= {{ pg_batch_start_date() }}::timestamp without time zone
  AND payment_dt < ({{ pg_batch_end_date() }} + 1)::timestamp without time zone
  AND claim_type IS NULL
GROUP BY payment_dt::date, site_type, seller_id
