{{
  config(
    materialized = 'tvf',
    params = [
      {'name': 'DS_START_DATE', 'type': 'date'},
      {'name': 'DS_END_DATE', 'type': 'date'}
    ],
    schema = 'coupang_ads',
    alias = 'report_daily'
  )
}}

WITH

placement_group_mapping AS (
  {{ coupang_ads__placement_group_mapping() }}
)

SELECT
    master.vendor_id
  , master.vendor_name
  , master.vendor_alias
  , master.vendor_type
  -- Campaign attributes
  , insight.campaign_id
  , master.campaign_name
  , COALESCE(master.campaign_type, '캠페인 없음') AS campaign_type
  , COALESCE(master.goal_type, '-') AS goal_type
  , master.is_active
  , master.is_deleted
  -- Product attributes
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
  -- Insight attributes
  , insight.option_id
  , COALESCE(placement_group.label, '-') AS placement_group
  , insight.impression_count
  , insight.click_count
  , insight.ad_cost
  , insight.conv_count
  , insight.direct_conv_count
  , insight.conv_amount
  , insight.direct_conv_amount
  , insight.ymd
FROM {{ ref('coupang_ads__insight_daily') }} AS insight
LEFT JOIN placement_group_mapping AS placement_group
  ON insight.placement_group = placement_group.code
LEFT JOIN {{ ref('coupang_ads__campaign_master') }} AS master
  ON insight.campaign_id = master.campaign_id
LEFT JOIN {{ ref('core__product_master') }} AS product
  ON insight.product_id = product.product_id
WHERE insight.ymd BETWEEN DS_START_DATE AND DS_END_DATE
