{{
  config(
    materialized = 'tvf',
    meta = {
      'params': [
        {'name': 'DS_START_DATE', 'type': 'date'},
        {'name': 'DS_END_DATE', 'type': 'date'},
        {'name': 'SHOP_FILTER', 'type': 'text'}
      ]
    },
    schema = 'analytics',
    alias = 'sales_summary'
  )
}}

WITH{#

#} cafe24_sales_summary AS (
  SELECT
      '카페24' AS shop_name
    , mall.mall_id AS account_no
    , mall.mall_name AS account_name
    , mall.corp_name
    , mall.team_name
    , summary.payment_amount
    , summary.max_order_dt
    , summary.order_date
  FROM {{ ref('cafe24__sales_summary') }} AS summary
  LEFT JOIN {{ source('cafe24', 'mall') }} AS mall
    ON summary.mall_id = mall.mall_id
  WHERE summary.order_date BETWEEN DS_START_DATE AND DS_END_DATE
),{#

#} coupang_sales_summary AS (
  SELECT
      '쿠팡' AS shop_name
    , vendor.vendor_id AS account_no
    , vendor.vendor_name AS account_name
    , vendor.corp_name
    , vendor.team_name
    , summary.payment_amount
    , summary.max_order_dt
    , summary.order_date
  FROM {{ ref('coupang__sales_summary') }} AS summary
  LEFT JOIN {{ source('coupang', 'vendor') }} AS vendor
    ON summary.vendor_id = vendor.vendor_id
  WHERE summary.order_date BETWEEN DS_START_DATE AND DS_END_DATE
),{#

#} ebay_sales_summary AS (
  SELECT
      '이베이' AS shop_name
    , seller.seller_id AS account_no
    , seller.seller_name || (CASE summary.site_type WHEN 1 THEN '(옥션)' WHEN 2 THEN '(G마켓)' ELSE '' END) AS account_name
    , seller.corp_name
    , seller.team_name
    , summary.payment_amount
    , summary.max_order_dt
    , summary.order_date
  FROM {{ ref('ebay__sales_summary') }} AS summary
  LEFT JOIN {{ source('ebay', 'seller') }} AS seller
    ON summary.site_type = seller.site_type AND summary.seller_id = seller.seller_id
  WHERE summary.order_date BETWEEN DS_START_DATE AND DS_END_DATE
),{#

#} smartstore_sales_summary AS (
  SELECT
      '스마트스토어' AS shop_name
    , summary.channel_seq::text AS account_no
    , channel.channel_name AS account_name
    , channel.corp_name
    , channel.team_name
    , summary.payment_amount
    , summary.max_order_dt
    , summary.order_date
  FROM {{ ref('smartstore__sales_summary') }} AS summary
  LEFT JOIN {{ source('smartstore', 'channel') }} AS channel
    ON summary.channel_seq = channel.channel_seq
  WHERE summary.order_date BETWEEN DS_START_DATE AND DS_END_DATE
){#

#} SELECT *
FROM (
  SELECT * FROM cafe24_sales_summary
  UNION ALL
  SELECT * FROM coupang_sales_summary
  UNION ALL
  SELECT * FROM ebay_sales_summary
  UNION ALL
  SELECT * FROM smartstore_sales_summary
)
WHERE shop_name = SHOP_FILTER
