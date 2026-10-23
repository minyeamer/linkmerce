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
      mall_id
    , order_item_code
    , MAX(order_status_type) AS order_status_type
  FROM {{ source('cafe24', 'order_change') }}
  WHERE order_date BETWEEN {{ pg_batch_start_date() }} AND {{ pg_batch_end_date() }}
  GROUP BY mall_id, order_item_code
),{#

#} sales_summary AS (
  SELECT
      ord.mall_id
    , SUM(
        COALESCE(ord.order_quantity, 0) * (COALESCE(ord.product_price, 0) + COALESCE(ord.option_price, 0))
        - COALESCE(ord.additional_discount_price, 0)
        - COALESCE(ord.coupon_discount_price, 0)
        - COALESCE(ord.app_item_discount_amount, 0)
      ) AS payment_amount
    , MAX(ord.order_dt) AS max_order_dt
    , ord.order_dt::date AS order_date
  FROM {{ source('cafe24', 'order_detail') }} AS ord
  LEFT JOIN order_status AS status
    ON (ord.mall_id = status.mall_id) AND (ord.order_item_code = status.order_item_code)
  WHERE ord.order_dt >= {{ pg_batch_start_date() }}::timestamp without time zone
    AND ord.order_dt < ({{ pg_batch_end_date() }} + 1)::timestamp without time zone
    AND ((status.order_status_type = 0) OR (status.order_status_type IS NULL))
  GROUP BY ord.order_dt::date, ord.mall_id
){#

#} SELECT * FROM sales_summary
