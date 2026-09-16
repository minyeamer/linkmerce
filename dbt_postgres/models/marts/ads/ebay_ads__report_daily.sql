{{
  config(
    materialized = 'tvf',
    meta = {
      'params': [
        {'name': 'DS_START_DATE', 'type': 'date'},
        {'name': 'DS_END_DATE', 'type': 'date'}
      ]
    },
    schema = 'ebay_ads',
    alias = 'report_daily'
  )
}}

SELECT
  -- Account attributes
    insight.seller_id
  , (CASE WHEN insight.site_type = 1 THEN '옥션' ELSE 'G마켓' END) AS site_name
  -- Group attributes
  , insight.campaign_group_id
  , master.campaign_group_name
  , COALESCE(master.campaign_group_type, '캠페인 없음') AS campaign_group_type
  -- Campaign attributes
  , insight.campaign_id
  , master.campaign_name
  , master.campaign_status
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
  -- Item attributes
  , insight.site_item_id
  , (CASE
      WHEN item.option_name IS NOT NULL
        THEN CONCAT(item.item_name, ' / ', item.option_name)
      ELSE item.item_name
    END) AS site_item_name
  , item.category_name AS category_name_eby
  -- Insight attributes
  , insight.impression_count
  , insight.click_count
  , insight.ad_cost
  , insight.conv_count
  , insight.conv_amount
  , insight.ymd
FROM {{ ref('ebay_ads__insight_daily') }} AS insight
LEFT JOIN {{ ref('ebay_ads__campaign_master') }} AS master
  ON insight.campaign_id = master.campaign_id
LEFT JOIN {{ ref('core__product_master') }} AS product
  ON insight.product_id = product.product_id
LEFT JOIN {{ source('ebay', 'item') }} AS item
  ON (insight.site_item_id = item.site_item_id) AND (insight.site_type = item.site_type)
WHERE insight.ymd BETWEEN DS_START_DATE AND DS_END_DATE
