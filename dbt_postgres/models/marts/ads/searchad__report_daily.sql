{{
  config(
    materialized = 'tvf',
    meta = {
      'params': [
        {'name': 'DS_START_DATE', 'type': 'date'},
        {'name': 'DS_END_DATE', 'type': 'date'}
      ]
    },
    schema = 'searchad',
    alias = 'report_daily'
  )
}}

WITH{#

#} device_type_mapping AS (
  {{ searchad__device_type_mapping() }}
){#

#} SELECT
    COALESCE(ad.customer_id, cmp.customer_id) AS customer_id
  , COALESCE(ad.account_name, cmp.account_name) AS account_name
  , COALESCE(ad.account_type, cmp.account_type) AS account_type
  -- Campaign attributes
  , insight.campaign_id
  , COALESCE(ad.campaign_name, cmp.campaign_name) AS campaign_name
  , COALESCE(ad.campaign_type, cmp.campaign_type, '캠페인 없음') AS campaign_type
  -- Adgroup attributes
  , ad.adgroup_id
  , ad.adgroup_name
  , COALESCE(ad.adgroup_type, CONCAT(cmp.account_type, '-기타'), '그룹 없음') AS adgroup_type
  -- Ad attributes
  , insight.ad_id
  , ad.title
  , ad.description
  , COALESCE(ad.ad_type, cmp.ad_type, '유형 없음') AS ad_type
  , COALESCE(ad.is_enabled, cmp.is_enabled) AS is_enabled
  , COALESCE(ad.is_deleted, cmp.is_deleted) AS is_deleted
  , ad.mall_product_id
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
  , COALESCE(device_type.label, '-') AS device_type
  , insight.impression_count
  , insight.click_count
  , insight.ad_cost
  , insight.ad_rank_sum
  , insight.conv_count
  , insight.direct_conv_count
  , insight.conv_amount
  , insight.direct_conv_amount
  , insight.ymd
FROM {{ ref('searchad__insight_daily') }} AS insight
LEFT JOIN device_type_mapping AS device_type
  ON insight.device_type = device_type.code
LEFT JOIN {{ ref('searchad__campaign_master') }} AS cmp
  ON insight.campaign_id = cmp.campaign_id
LEFT JOIN {{ ref('searchad__ad_master') }} AS ad
  ON insight.ad_id = ad.ad_id
LEFT JOIN {{ ref('core__product_master') }} AS product
  ON insight.product_id = product.product_id
WHERE insight.ymd BETWEEN DS_START_DATE AND DS_END_DATE{#

#} UNION ALL{#

#} SELECT
    master.customer_id
  , master.account_name
  , master.account_type
  -- Campaign attributes
  , master.campaign_id
  , master.campaign_name
  , COALESCE(master.campaign_type, '캠페인 없음') AS campaign_type
  -- Adgroup attributes
  , contract.adgroup_id
  , master.adgroup_name
  , COALESCE(master.adgroup_type, '그룹 없음') AS adgroup_type
  -- Ad attributes
  , master.contract_id AS ad_id
  , master.contract_name AS title
  , NULL::text AS description
  , COALESCE(master.contract_type, '유형 없음') AS ad_type
  , (master.cancel_date IS NULL) AS is_enabled
  , (master.cancel_date IS NOT NULL) AS is_deleted
  , NULL::bigint AS mall_product_id
  -- Product attributes
  , contract.product_id
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
  , '계약' AS device_type
  , NULL::integer AS impression_count
  , NULL::integer AS click_count
  , contract.ad_cost
  , NULL::integer AS ad_rank_sum
  , NULL::integer AS conv_count
  , NULL::integer AS direct_conv_count
  , NULL::integer AS conv_amount
  , NULL::integer AS direct_conv_amount
  , contract.ymd
FROM {{ ref('searchad__contract_daily') }} AS contract
LEFT JOIN {{ ref('searchad__contract_master') }} AS master
  ON contract.contract_id = master.contract_id
LEFT JOIN {{ ref('core__product_master') }} AS product
  ON contract.product_id = product.product_id
WHERE contract.ymd BETWEEN DS_START_DATE AND DS_END_DATE
