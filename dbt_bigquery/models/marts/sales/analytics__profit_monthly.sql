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
    alias = 'profit_monthly'
  )
}}

WITH

order_status_mapping AS (
  {{ core__order_status_mapping() }}
),

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
    , SUM(margin_amount) AS margin_amount
    , SUM(ad_cost) AS ad_cost
    , SUM(extra_cost) AS extra_cost
    , SUM(profit) AS profit
    , MIN(order_date) AS order_start_date
    , MAX(order_date) AS order_end_date
    , DATE_TRUNC(order_date, MONTH) AS order_ym
  FROM {{ ref('analytics__profit_base') }}(DS_START_DATE, DS_END_DATE)
  GROUP BY DATE_TRUNC(order_date, MONTH), product_id, shop_id, order_status
),

profit_monthly AS (
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
    , COALESCE(fact.sku_quantity * COALESCE(item.unit_scale, 1), 0) AS unit_quantity
    , fact.sku_quantity
    , fact.payment_amount
    , fact.supply_amount
    , fact.supply_cost
    , fact.delivery_fee
    , fact.margin_amount
    , fact.ad_cost
    , fact.extra_cost
    , fact.profit
    , fact.order_start_date
    , fact.order_end_date
    , fact.order_ym
  FROM sales_monthly AS fact
  LEFT JOIN {{ ref('core__product_master') }} AS item
    ON fact.product_id = item.product_id
  LEFT JOIN {{ source('sabangnet', 'shop') }} AS shop
    ON fact.shop_id = shop.shop_id
  LEFT JOIN order_status_mapping AS order_status
    ON fact.order_status = order_status.code
)

SELECT * FROM profit_monthly
