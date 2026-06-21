{{
  config(
    materialized = 'tvf',
    params = [
      {'name': 'DS_START_DATE', 'type': 'date'},
      {'name': 'DS_END_DATE', 'type': 'date'}
    ],
    schema = 'analytics',
    alias = 'order_count'
  )
}}

WITH

order_status_mapping AS (
  {{ core__order_status_mapping() }}
),

sabangnet_order_count AS (
  SELECT
      order_id
    , product_order_id
    , shop_id
    , product_id
    , order_status
    , order_quantity
    , order_date
  FROM {{ ref('sabangnet__order_count') }}
  WHERE order_date BETWEEN DS_START_DATE AND DS_END_DATE
),

smartstore_order_count AS (
  SELECT
      CAST(order_id AS STRING) AS order_id
    , CAST(product_order_id AS STRING) AS product_order_id
    , IF(delivery_type = 7, 'shop9000', 'shop0055') AS shop_id
    , product_id
    , order_status
    , order_quantity
    , order_date
  FROM {{ ref('smartstore__order_count') }}
  WHERE order_date BETWEEN DS_START_DATE AND DS_END_DATE
),

coupang_rfm_order_count AS (
  SELECT
      CAST(order_id AS STRING) AS order_id
    , CAST(NULL AS STRING) AS product_order_id
    , 'shop9001' AS shop_id
    , product_id
    , order_status
    , order_quantity
    , order_date
  FROM {{ ref('coupang_rfm__order_count') }}
  WHERE order_date BETWEEN DS_START_DATE AND DS_END_DATE
),

order_count AS (
  SELECT
      order_id
    , product_order_id
    , shop_id
    , product_id
    , order_status
    , order_quantity
    , order_date
  FROM (
    (SELECT * FROM sabangnet_order_count)
    UNION ALL
    (SELECT * FROM smartstore_order_count)
    UNION ALL
    (SELECT * FROM coupang_rfm_order_count)
  ) AS t_
)

SELECT
    fact.order_id
  , fact.product_order_id
  , COALESCE(item.item_id, 'NA-AAAAAA-00') AS item_id
  , COALESCE(item.item_seq, 99999999) AS item_seq
  , COALESCE(item.team_name, '담당팀 없음') AS team_name
  , COALESCE(item.brand_name, '브랜드 없음') AS brand_name
  , COALESCE(item.category_name1, '-') AS category_name1
  , COALESCE(item.category_name2, '-') AS category_name2
  , COALESCE(item.category_name3, '-') AS category_name3
  , COALESCE(item.category_name4, '-') AS category_name4
  , COALESCE(item.color, '-') AS color
  , fact.product_id
  , COALESCE(item.product_name, '매칭 불가 상품') AS product_name
  , COALESCE(
      IF(item.unit_name IS NULL
        , item.category_name3
        , CONCAT(item.category_name3, ' (', item.unit_name, ')'))
      , '-'
    ) AS category_unit_name
  , fact.shop_id
  , COALESCE(shop.shop_group, '-') AS shop_group
  , COALESCE(shop.shop_alias, '-') AS shop_name
  , COALESCE(order_status.label, '알 수 없음') AS order_status
  , fact.order_quantity
  , fact.order_date
FROM order_count AS fact
LEFT JOIN {{ ref('core__product_master') }} AS item
  ON fact.product_id = item.product_id
LEFT JOIN {{ source('sabangnet', 'shop') }} AS shop
  ON fact.shop_id = shop.shop_id
LEFT JOIN order_status_mapping AS order_status
  ON fact.order_status = order_status.code
