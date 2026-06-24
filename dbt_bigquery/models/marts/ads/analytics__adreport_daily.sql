{{
  config(
    materialized = 'tvf',
    params = [
      {'name': 'DS_START_DATE', 'type': 'date'},
      {'name': 'DS_END_DATE', 'type': 'date'}
    ],
    schema = 'analytics',
    alias = 'adreport_daily'
  )
}}

SELECT
    '네이버' AS platform_name
  , account_name
  , campaign_name
  , adgroup_name
  , title AS ad_name
  , ad_type
  , ad_cost
  , conv_amount
  , product_id
  , team_name
  , brand_name
  , category_name1
  , category_name2
  , category_name3
  , category_name4
  , color
  , product_name
  , ymd
FROM {{ ref('searchad__report_daily') }}(DS_START_DATE, DS_END_DATE)

UNION ALL

SELECT
    '쿠팡' AS platform_name
  , vendor_name AS account_name
  , campaign_name
  , '-' AS adgroup_name
  , option_name AS ad_name
  , goal_type AS ad_type
  , ad_cost
  , conv_amount
  , product_id
  , team_name
  , brand_name
  , category_name1
  , category_name2
  , category_name3
  , category_name4
  , color
  , product_name
  , ymd
FROM {{ ref('coupang_ads__report_daily') }}(DS_START_DATE, DS_END_DATE)

UNION ALL

SELECT
    '구글' AS platform_name
  , account_name
  , campaign_name
  , adgroup_name
  , ad_name
  , ad_type
  , ad_cost
  , NULL AS conv_amount
  , product_id
  , team_name
  , brand_name
  , category_name1
  , category_name2
  , category_name3
  , category_name4
  , color
  , product_name
  , ymd
FROM {{ ref('google_ads__report_daily') }}(DS_START_DATE, DS_END_DATE)

UNION ALL

SELECT
    '메타' AS platform_name
  , account_name
  , campaign_name
  , adset_name AS adgroup_name
  , ad_name
  , objective AS ad_type
  , ad_cost
  , NULL AS conv_amount
  , product_id
  , team_name
  , brand_name
  , category_name1
  , category_name2
  , category_name3
  , category_name4
  , color
  , product_name
  , ymd
FROM {{ ref('meta_ads__report_daily') }}(DS_START_DATE, DS_END_DATE)

UNION ALL

SELECT
    REPLACE(shop.shop_alias, '(광고)', '') AS platform_name
  , '-' AS account_name
  , '-' AS campaign_name
  , '-' AS adgroup_name
  , '-' AS ad_name
  , '-' AS ad_type
  , ad_cost
  , NULL AS conv_amount
  , ads.brand_id AS product_id
  , COALESCE(product.team_name, '담당팀 없음') AS team_name
  , COALESCE(product.brand_name, '브랜드 없음') AS brand_name
  , COALESCE(product.category_name1, '-') AS category_name1
  , COALESCE(product.category_name2, '-') AS category_name2
  , COALESCE(product.category_name3, '-') AS category_name3
  , COALESCE(product.category_name4, '-') AS category_name4
  , COALESCE(product.color, '-') AS color
  , COALESCE(product.product_name, '-') AS product_name
  , ymd
FROM {{ source('core', 'extra_ads') }} AS ads
LEFT JOIN {{ source('sabangnet', 'shop') }} AS shop
  ON ads.shop_id = shop.shop_id
LEFT JOIN {{ ref('core__product_master') }} AS product
  ON ads.brand_id = product.product_id
WHERE ymd BETWEEN DS_START_DATE AND DS_END_DATE
