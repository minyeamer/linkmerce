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
FROM {{ ref('searchad__report_daily') }}(DS_START_DATE, DS_END_DATE){#

#} UNION ALL{#

#} SELECT
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
FROM {{ ref('coupang_ads__report_daily') }}(DS_START_DATE, DS_END_DATE){#

#} UNION ALL{#

#} SELECT
    '구글' AS platform_name
  , account_name
  , campaign_name
  , adgroup_name
  , ad_name
  , ad_type
  , ad_cost
  , NULL::integer AS conv_amount
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
FROM {{ ref('google_ads__report_daily') }}(DS_START_DATE, DS_END_DATE){#

#} UNION ALL{#

#} SELECT
    '메타' AS platform_name
  , account_name
  , campaign_name
  , adset_name AS adgroup_name
  , ad_name
  , objective AS ad_type
  , ad_cost
  , NULL::integer AS conv_amount
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
FROM {{ ref('meta_ads__report_daily') }}(DS_START_DATE, DS_END_DATE){#

#} UNION ALL{#

#} SELECT
    '데이블' AS platform_name
  , '-' AS account_name
  , COALESCE(cmp.campaign_name, '-') AS campaign_name
  , '-' AS adgroup_name
  , '-' AS ad_name
  , '-' AS ad_type
  , report.ad_cost
  , NULL::integer AS conv_amount
  , report.product_id
  , COALESCE(product.team_name, '담당팀 없음') AS team_name
  , COALESCE(product.brand_name, '브랜드 없음') AS brand_name
  , COALESCE(product.category_name1, '-') AS category_name1
  , COALESCE(product.category_name2, '-') AS category_name2
  , COALESCE(product.category_name3, '-') AS category_name3
  , COALESCE(product.category_name4, '-') AS category_name4
  , COALESCE(product.color, '-') AS color
  , COALESCE(product.product_name, '-') AS product_name
  , report.ymd
FROM {{ ref('dable__report_daily') }} AS report
LEFT JOIN {{ source('dable', 'campaign') }} AS cmp
  ON report.campaign_id = cmp.campaign_id
LEFT JOIN {{ ref('core__product_master') }} AS product
  ON report.product_id = product.product_id
WHERE report.ymd BETWEEN DS_START_DATE AND DS_END_DATE{#

#} UNION ALL{#

#} SELECT
    '쇼핑커넥트' AS platform_name
  , COALESCE(space.space_name, '-') AS account_name
  , '-' AS campaign_name
  , '-' AS adgroup_name
  , COALESCE(mall_prd.product_name, '-') AS ad_name
  , '-' AS ad_type
  , insight.ad_cost
  , insight.conv_amount
  , insight.product_id
  , COALESCE(product.team_name, '담당팀 없음') AS team_name
  , COALESCE(product.brand_name, '브랜드 없음') AS brand_name
  , COALESCE(product.category_name1, '-') AS category_name1
  , COALESCE(product.category_name2, '-') AS category_name2
  , COALESCE(product.category_name3, '-') AS category_name3
  , COALESCE(product.category_name4, '-') AS category_name4
  , COALESCE(product.color, '-') AS color
  , COALESCE(product.product_name, '-') AS product_name
  , insight.ymd
FROM {{ ref('naver_connect__insight_daily') }} AS insight
LEFT JOIN {{ source('naver_connect', 'space') }} AS space
  ON insight.space_id = space.space_id
LEFT JOIN {{ source('smartstore', 'product') }} AS mall_prd
  ON insight.mall_product_id = mall_prd.product_id
LEFT JOIN {{ ref('core__product_master') }} AS product
  ON insight.product_id = product.product_id
WHERE insight.ymd BETWEEN DS_START_DATE AND DS_END_DATE{#

#} UNION ALL{#

#} SELECT
    REPLACE(shop.shop_alias, '(광고)', '') AS platform_name
  , '-' AS account_name
  , '-' AS campaign_name
  , '-' AS adgroup_name
  , '-' AS ad_name
  , '-' AS ad_type
  , ads.ad_cost
  , NULL::integer AS conv_amount
  , ads.brand_id AS product_id
  , COALESCE(product.team_name, '담당팀 없음') AS team_name
  , COALESCE(product.brand_name, '브랜드 없음') AS brand_name
  , COALESCE(product.category_name1, '-') AS category_name1
  , COALESCE(product.category_name2, '-') AS category_name2
  , COALESCE(product.category_name3, '-') AS category_name3
  , COALESCE(product.category_name4, '-') AS category_name4
  , COALESCE(product.color, '-') AS color
  , COALESCE(product.product_name, '-') AS product_name
  , ads.ymd
FROM {{ source('core', 'extra_ads') }} AS ads
LEFT JOIN {{ source('sabangnet', 'shop') }} AS shop
  ON ads.shop_id = shop.shop_id
LEFT JOIN {{ ref('core__product_master') }} AS product
  ON ads.brand_id = product.product_id
WHERE ads.ymd BETWEEN DS_START_DATE AND DS_END_DATE
