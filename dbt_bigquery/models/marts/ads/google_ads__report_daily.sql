{{
  config(
    materialized = 'tvf',
    params = [
      {'name': 'DS_START_DATE', 'type': 'date'},
      {'name': 'DS_END_DATE', 'type': 'date'}
    ],
    schema = 'google_ads',
    alias = 'report_daily'
  )
}}

WITH

device_type_mapping AS (
  {{ google_ads__device_type_mapping() }}
)

SELECT
    master.customer_id
  , master.account_name
  -- Campaign attrs
  , master.campaign_id
  , master.campaign_name
  , COALESCE(master.campaign_type, '캠페인 없음') AS campaign_type
  -- Adgroup attrs
  , master.adgroup_id
  , master.adgroup_name
  , COALESCE(master.adgroup_type, '그룹 없음') AS adgroup_type
  -- Ad attrs
  , insight.ad_id
  , master.ad_name
  , COALESCE(master.ad_type, '유형 없음') AS ad_type
  , COALESCE(master.ad_status, '알 수 없음') AS ad_status
  -- Product attrs
  , insight.product_id
  , product.item_id
  , COALESCE(product.item_seq, 99999999) AS item_seq
  , COALESCE(product.team_name, '담당팀 없음') AS team_name
  , COALESCE(product.brand_name, '브랜드 없음') AS brand_name
  , COALESCE(product.category_name1, '-') AS category_name1
  , COALESCE(product.category_name2, '-') AS category_name2
  , COALESCE(product.category_name3, '-') AS category_name3
  , COALESCE(product.category_name4, '-') AS category_name4
  , COALESCE(product.color, '-') AS color
  , COALESCE(product.product_name, '-') AS product_name
  -- Insight attrs
  , COALESCE(device_type.label, '-') AS device_type
  , insight.impression_count
  , insight.click_count
  , insight.ad_cost
  , insight.ymd
FROM {{ ref('google_ads__insight_daily') }} AS insight
LEFT JOIN device_type_mapping AS device_type
  ON insight.device_type = device_type.code
LEFT JOIN {{ ref('google_ads__ad_master') }} AS master
  ON insight.ad_id = master.ad_id
LEFT JOIN {{ ref('core__product_master') }} AS product
  ON insight.product_id = product.product_id
WHERE insight.ymd BETWEEN DS_START_DATE AND DS_END_DATE
