{{
  config(
    materialized = 'tvf',
    params = [
      {'name': 'DS_START_DATE', 'type': 'date'},
      {'name': 'DS_END_DATE', 'type': 'date'}
    ],
    schema = 'analytics',
    alias = 'profit_daily'
  )
}}

WITH

order_status_mapping AS (
  {{ core__order_status_mapping() }}
)

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
  , fact.order_date
FROM {{ ref('analytics__profit_base') }}(DS_START_DATE, DS_END_DATE) AS fact
LEFT JOIN {{ ref('core__product_master') }} AS item
  ON fact.product_id = item.product_id
LEFT JOIN {{ source('sabangnet', 'shop') }} AS shop
  ON fact.shop_id = shop.shop_id
LEFT JOIN order_status_mapping AS order_status
  ON fact.order_status = order_status.code
