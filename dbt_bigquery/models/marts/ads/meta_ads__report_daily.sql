{{
  config(
    materialized = 'tvf',
    params = [
      {'name': 'DS_START_DATE', 'type': 'date'},
      {'name': 'DS_END_DATE', 'type': 'date'}
    ],
    schema = 'meta_ads',
    alias = 'report_daily'
  )
}}

SELECT
    master.account_id
  , master.account_name
  -- Campaign attributes
  , master.campaign_id
  , master.campaign_name
  , COALESCE(master.objective, '-') AS objective
  -- Adset attributes
  , master.adset_id
  , master.adset_name
  -- Ad attributes
  , insight.ad_id
  , master.ad_name
  , COALESCE(master.effective_status, '-') AS effective_status
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
  -- Insight metrics
  , insight.impression_count
  , insight.reach_count
  , insight.click_count
  , insight.link_click_count
  , insight.ad_cost
  , insight.ymd
FROM {{ ref('meta_ads__insight_daily') }} AS insight
LEFT JOIN {{ ref('meta_ads__ad_master') }} AS master
  ON insight.ad_id = master.ad_id
LEFT JOIN {{ ref('core__product_master') }} AS product
  ON insight.product_id = product.product_id
WHERE insight.ymd BETWEEN DS_START_DATE AND DS_END_DATE
