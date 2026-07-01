{{
  config(
    materialized = 'tvf',
    meta = {
      'params': [
        {'name': 'DS_START_DATE', 'type': 'date'},
        {'name': 'DS_END_DATE', 'type': 'date'}
      ]
    },
    schema = 'analytics',
    alias = 'profit_base'
  )
}}

WITH

sales_daily AS (
  SELECT
      product_id
    , shop_id
    , order_status
    , IF(order_status IN (0, 6), COALESCE(sku_quantity, 0), 0) AS sku_quantity
    , (CASE
        -- Exclude payment_amount from extra_sales
        WHEN shop_id = 'adop9000' THEN 0
        ELSE IF(order_status IN (0, 6), COALESCE(payment_amount, 0), 0)
      END) AS payment_amount
    , IF(order_status IN (0, 6), COALESCE(supply_amount, 0), 0) AS supply_amount
    , IF(order_status IN (0, 2, 6), COALESCE(supply_cost, 0), 0) AS supply_cost
    , IF(order_status IN (0, 1, 2, 5, 6, 7), COALESCE(delivery_fee, 0), 0) AS delivery_fee
    , COALESCE(ad_cost, 0) AS ad_cost
    , COALESCE(extra_cost, 0) AS extra_cost
    , order_date
  FROM {{ ref('analytics__sales_daily') }}
  WHERE order_date BETWEEN DS_START_DATE AND DS_END_DATE
),

profit_daily AS (
  SELECT
      product_id
    , shop_id
    , order_status
    , sku_quantity
    , payment_amount
    , supply_amount
    , supply_cost
    , delivery_fee
    , supply_amount - supply_cost - delivery_fee AS margin_amount
    , ad_cost
    , extra_cost
    , supply_amount - supply_cost - delivery_fee - ad_cost - extra_cost AS profit
    , order_date
  FROM sales_daily
),

extra_profit_daily AS (
  SELECT
      product_id
    , shop_id
    , 0 AS order_status
    , 0 AS sku_quantity
    , 0 AS payment_amount
    , 0 AS supply_amount
    , 0 AS supply_cost
    , 0 AS delivery_fee
    , 0 AS margin_amount
    , 0 AS ad_cost
    , 0 AS extra_cost
    , profit
    , ymd AS order_date
  FROM {{ source('core', 'extra_profit') }}
  WHERE ymd BETWEEN DS_START_DATE AND DS_END_DATE
)

(SELECT * FROM profit_daily)
UNION ALL
(SELECT * FROM extra_profit_daily)
