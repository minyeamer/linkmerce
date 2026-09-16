{{
  config(
    materialized = 'incremental',
    schema = 'xfm_ads',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      "field": "ymd",
      "data_type": "date",
      "granularity": "day"
    },
    partitions = bq_date_partitions('ds_start_date', 'ds_end_date'),
    require_partition_filter = true
  )
}}

WITH

product_renewal_mapping AS (
  {{ core__product_renewal_mapping() }}
),

eby_itm_to_sbn_ids AS (
  SELECT
      item_id
    , ARRAY_TO_STRING(
        ARRAY(
          SELECT SPLIT(bundle_product_id, ':')[SAFE_OFFSET(0)]
          FROM UNNEST(SPLIT(bundle_product_ids, ',')) AS bundle_product_id
        ),
        ','
      ) AS bundle_product_ids
  FROM {{ source('relation', 'eby_itm_to_sbn_ids') }}
),

-- Step 1: prepare auction adreports with item_id and site_item_id mapping

insight_ai_daily AS (
  SELECT
      COALESCE(itm.seller_id, '') AS seller_id
    , iac_ai.site_type
    , 1 AS campaign_group_id
    , 1 AS campaign_id
    , COALESCE(itm.item_id, 0) AS item_id
    , iac_ai.item_id AS site_item_id
    , COALESCE(rel.bundle_product_ids, '200000') AS bundle_product_ids
    , CAST(NULL AS INT64) AS impression_count
    , iac_ai.click_count
    , iac_ai.ad_cost
    , iac_ai.conv_count
    , iac_ai.conv_amount
    , iac_ai.ymd
  FROM {{ source('ebay_ads', 'report_ai') }} AS iac_ai
  LEFT JOIN {{ source('ebay', 'item') }} AS itm
    ON (iac_ai.item_id = itm.site_item_id) AND (iac_ai.site_type = itm.site_type)
  LEFT JOIN eby_itm_to_sbn_ids AS rel
    ON itm.item_id = rel.item_id
  WHERE iac_ai.ymd BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
),

insight_cpc_daily AS (
  SELECT
      COALESCE(itm.seller_id, '') AS seller_id
    , iac_cpc.site_type
    , 2 AS campaign_group_id
    , 2 AS campaign_id
    , COALESCE(itm.item_id, 0) AS item_id
    , iac_cpc.item_id AS site_item_id
    , COALESCE(rel.bundle_product_ids, '200000') AS bundle_product_ids
    , iac_cpc.impression_count
    , iac_cpc.click_count
    , iac_cpc.ad_cost
    , iac_cpc.conv_count
    , iac_cpc.conv_amount
    , iac_cpc.ymd
  FROM {{ source('ebay_ads', 'report_cpc') }} AS iac_cpc
  LEFT JOIN {{ source('ebay', 'item') }} AS itm
    ON (iac_cpc.item_id = itm.site_item_id) AND (iac_cpc.site_type = itm.site_type)
  LEFT JOIN eby_itm_to_sbn_ids AS rel
    ON itm.item_id = rel.item_id
  WHERE iac_cpc.ymd BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
),

-- Step 2: prepare gmarket adreport

insight_gmkt_daily AS (
  SELECT
      COALESCE(itm.seller_id, '') AS seller_id
    , 2 AS site_type
    , gmkt.campaign_group_id
    , gmkt.campaign_id
    , COALESCE(itm.item_id, 0) AS item_id
    , CAST(gmkt.item_id AS STRING) AS site_item_id
    , COALESCE(rel.bundle_product_ids, '200000') AS bundle_product_ids
    , gmkt.impression_count
    , gmkt.click_count
    , gmkt.ad_cost
    , gmkt.conv_count
    , gmkt.conv_amount
    , gmkt.ymd
  FROM {{ source('ebay_ads', 'report_gmkt') }} AS gmkt
  LEFT JOIN {{ source('ebay', 'item') }} AS itm
    ON (CAST(gmkt.item_id AS STRING) = itm.site_item_id) AND (itm.site_type = 2)
  LEFT JOIN eby_itm_to_sbn_ids AS rel
    ON itm.item_id = rel.item_id
  WHERE gmkt.ymd BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
),

-- Step 3: aggregate insights and distribute metrics across bundle products

bundle_product_insight AS (
  SELECT
      seller_id
    , site_type
    , campaign_group_id
    , campaign_id
    , item_id
    , site_item_id
    , ANY_VALUE(bundle_product_ids) AS bundle_product_ids
    , SUM(impression_count) AS impression_count
    , SUM(click_count) AS click_count
    , SUM(ad_cost) AS ad_cost
    , SUM(conv_count) AS conv_count
    , SUM(conv_amount) AS conv_amount
    , ymd
  FROM (
    (SELECT * FROM insight_ai_daily)
    UNION ALL
    (SELECT * FROM insight_cpc_daily)
    UNION ALL
    (SELECT * FROM insight_gmkt_daily)
  ) AS t_
  GROUP BY ymd, seller_id, site_type, campaign_group_id, campaign_id, item_id, site_item_id
),

exploded_product_insight AS (
  SELECT
      seller_id
    , site_type
    , campaign_group_id
    , campaign_id
    , item_id
    , site_item_id
    , bundle_product_id AS product_id
    , (DIV(impression_count, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(impression_count, bundle_product_count), 0)) AS impression_count
    , (DIV(click_count, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(click_count, bundle_product_count), 0)) AS click_count
    , (DIV(ad_cost, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(ad_cost, bundle_product_count), 0)) AS ad_cost
    , (DIV(conv_count, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(conv_count, bundle_product_count), 0)) AS conv_count
    , (DIV(conv_amount, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(conv_amount, bundle_product_count), 0)) AS conv_amount
    , ymd
  FROM (
    SELECT
        insight.*
      , COALESCE(renewal.product_id_old, bundle_product_id) AS bundle_product_id
      , bundle_product_offset
      , ARRAY_LENGTH(SPLIT(insight.bundle_product_ids, ',')) AS bundle_product_count
    FROM bundle_product_insight AS insight
    CROSS JOIN UNNEST(SPLIT(insight.bundle_product_ids, ',')) AS bundle_product_id WITH OFFSET AS bundle_product_offset
    LEFT JOIN product_renewal_mapping AS renewal
      ON (bundle_product_id = renewal.product_id_new) AND (insight.ymd < renewal.renewal_date)
  ) AS t_
)

SELECT * FROM exploded_product_insight
