{{
  config(
    materialized = 'tvf',
    params = [
      {'name': 'DS_START_DATE', 'type': 'date'},
      {'name': 'DS_END_DATE', 'type': 'date'},
      {'name': 'DS_INTERVAL_MONTH', 'type': 'int64'}
    ],
    schema = 'analytics',
    alias = 'profit_mom'
  )
}}

WITH

order_status_mapping AS (
  {{ core__order_status_mapping() }}
),

-- Step 1: load daily sales for the lookback window ending on DS_END_DATE

sales_daily AS (
  SELECT
      product_id
    , shop_id
    , order_status
    , IF(order_status IN (0, 6), COALESCE(sku_quantity, 0), 0) AS sku_quantity
    , (CASE
        WHEN shop_id = 'adop9000' THEN 0
        ELSE IF(order_status IN (0, 6), COALESCE(payment_amount, 0), 0)
      END) AS payment_amount
    , IF(order_status IN (0, 6), COALESCE(supply_amount, 0), 0) AS supply_amount
    , IF(order_status IN (0, 2, 6), COALESCE(supply_cost, 0), 0) AS supply_cost
    , IF(order_status IN (0, 1, 2, 5, 6, 7, 9), COALESCE(delivery_fee, 0), 0) AS delivery_fee
    , COALESCE(ad_cost, 0) AS ad_cost
    , COALESCE(extra_cost, 0) AS extra_cost
    , order_date
  FROM {{ ref('analytics__sales_daily') }}
  WHERE order_date
    BETWEEN DATE_TRUNC(DATE_SUB(DS_END_DATE, INTERVAL DS_INTERVAL_MONTH MONTH), MONTH)
    AND DS_END_DATE
),

-- Step 2: aggregate sales within the requested DS_START_DATE to DS_END_DATE range

sales_ds_range AS (
  SELECT
      product_id
    , shop_id
    , order_status
    , SUM(sku_quantity) AS sku_quantity
    , SUM(payment_amount) AS payment_amount
    , SUM(supply_amount) AS supply_amount
    , SUM(supply_cost) AS supply_cost
    , SUM(delivery_fee) AS delivery_fee
    , SUM(ad_cost) AS ad_cost
    , SUM(extra_cost) AS extra_cost
    , FORMAT_DATE('%Y-%m', DS_END_DATE) AS order_ym
    , MIN(order_date) AS order_start_date
    , MAX(order_date) AS order_end_date
  FROM sales_daily
  WHERE order_date BETWEEN DS_START_DATE AND DS_END_DATE
  GROUP BY product_id, shop_id, order_status
),

-- Step 3: aggregate monthly sales for dates before the DS_END_DATE month

sales_monthly AS (
  SELECT
      product_id
    , shop_id
    , order_status
    , SUM(sku_quantity) AS sku_quantity
    , SUM(payment_amount) AS payment_amount
    , SUM(supply_amount) AS supply_amount
    , SUM(supply_cost) AS supply_cost
    , SUM(delivery_fee) AS delivery_fee
    , SUM(ad_cost) AS ad_cost
    , SUM(extra_cost) AS extra_cost
    , FORMAT_DATE('%Y-%m', order_date) AS order_ym
    , MIN(order_date) AS order_start_date
    , MAX(order_date) AS order_end_date
  FROM {{ ref('analytics__sales_daily') }}
  WHERE order_date < DATE_TRUNC(DS_END_DATE, MONTH)
  GROUP BY FORMAT_DATE('%Y-%m', order_date), product_id, shop_id, order_status
),

-- Step 4: combine the requested date-range summary with monthly summaries and unpivot metrics

sales_monthly_unpivot AS (
  SELECT
      product_id
    , shop_id
    , order_status
    , metric_name
    , metric_value
    , order_ym
    , order_start_date
    , order_end_date
  FROM (
    (SELECT * FROM sales_ds_range)
    UNION ALL
    (SELECT * FROM sales_monthly)
  )
  UNPIVOT (
    metric_value FOR metric_name IN (
        sku_quantity
      , payment_amount
      , supply_amount
      , supply_cost
      , delivery_fee
      , ad_cost
      , extra_cost
    )
  )
),

-- Step 5: enrich unpivoted metrics with item, shop, and sales attributes

profit_mom AS (
  SELECT
      fact.product_id
    -- Item attributes
    , COALESCE(item.item_id, 'NA-AAAAAA-00') AS item_id
    , COALESCE(item.item_seq, 99999999) AS item_seq
    , COALESCE(item.team_name, '담당팀 없음') AS team_name
    , COALESCE(item.brand_name, '브랜드 없음') AS brand_name
    , COALESCE(item.category_name1, '-') AS category_name1
    , COALESCE(item.category_name2, '-') AS category_name2
    , COALESCE(item.category_name3, '-') AS category_name3
    , COALESCE(item.category_name4, '-') AS category_name4
    , COALESCE(item.color, '-') AS color
    , COALESCE(item.product_name, '매칭 불가 상품') AS product_name
    , COALESCE(
        IF(item.unit_name IS NULL
          , item.category_name3
          , CONCAT(item.category_name3, ' (', item.unit_name, ')'))
        , '-'
      ) AS category_unit_name
    -- Shop attributes
    , fact.shop_id
    , COALESCE(shop.shop_group, '-') AS shop_group
    , COALESCE(shop.shop_alias, '-') AS shop_name
    -- Sales attributes
    , COALESCE(order_status.label, '알 수 없음') AS order_status
    , fact.metric_name
    , fact.metric_value
    , fact.order_ym
    , fact.order_start_date
    , fact.order_end_date
  FROM sales_monthly_unpivot AS fact
  LEFT JOIN {{ ref('core__product_master') }} AS item
    ON fact.product_id = item.product_id
  LEFT JOIN {{ source('sabangnet', 'shop') }} AS shop
    ON fact.shop_id = shop.shop_id
  LEFT JOIN order_status_mapping AS order_status
    ON fact.order_status = order_status.code
)

SELECT * FROM profit_mom
