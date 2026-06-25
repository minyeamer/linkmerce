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

insight_pa_daily AS (
  SELECT
      pa.campaign_id
    , pa.option_id
    , pa.placement_group
    , COALESCE(
          rel_opt.bundle_product_ids
        , vdr.bundle_brand_ids
        , '200000'
      ) AS bundle_product_ids
    , pa.impression_count
    , pa.click_count
    , pa.ad_cost
    , pa.conv_count
    , pa.direct_conv_count
    , pa.conv_amount
    , pa.direct_conv_amount
    , pa.ymd
  FROM {{ source('coupang_ads', 'report_pa') }} AS pa
  LEFT JOIN {{ source('relation', 'cpg_opt_to_sbn_ids') }} AS rel_opt
    ON pa.option_id = rel_opt.option_id
  LEFT JOIN {{ source('coupang', 'vendor') }} AS vdr
    ON pa.vendor_id = vdr.vendor_id
  WHERE pa.ymd BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
),

insight_nca_daily AS (
  SELECT
      nca.campaign_id
    , COALESCE(nca.option_id, ad.option_id) AS option_id
    , nca.placement_group
    , COALESCE(
          rel_opt.bundle_product_ids
        , vdr.bundle_brand_ids
        , '200000'
      ) AS bundle_product_ids
    , nca.impression_count
    , nca.click_count
    , nca.ad_cost
    , SAFE_CAST(NULL AS INT64) AS conv_count
    , SAFE_CAST(NULL AS INT64) AS direct_conv_count
    , SAFE_CAST(NULL AS INT64) AS conv_amount
    , SAFE_CAST(NULL AS INT64) AS direct_conv_amount
    , nca.ymd
  FROM {{ source('coupang_ads', 'report_nca') }} AS nca
  LEFT JOIN {{ source('coupang_ads', 'creative') }} AS ad
    ON nca.creative_id = ad.creative_id
  LEFT JOIN {{ source('relation', 'cpg_opt_to_sbn_ids') }} AS rel_opt
    ON COALESCE(nca.option_id, ad.option_id) = rel_opt.option_id
  LEFT JOIN {{ source('coupang', 'vendor') }} AS vdr
    ON nca.vendor_id = vdr.vendor_id
  WHERE nca.ymd BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
),

bundle_product_insight AS (
  SELECT
      campaign_id
    , option_id
    , placement_group
    , ANY_VALUE(bundle_product_ids) AS bundle_product_ids
    , SUM(impression_count) AS impression_count
    , SUM(click_count) AS click_count
    , SUM(ad_cost) AS ad_cost
    , SUM(conv_count) AS conv_count
    , SUM(direct_conv_count) AS direct_conv_count
    , SUM(conv_amount) AS conv_amount
    , SUM(direct_conv_amount) AS direct_conv_amount
    , ymd
  FROM (
    SELECT * FROM insight_pa_daily
    UNION ALL
    SELECT * FROM insight_nca_daily
  ) AS t_
  GROUP BY ymd, campaign_id, option_id, placement_group
),

exploded_product_insight AS (
  SELECT
      campaign_id
    , option_id
    , placement_group
    , bundle_product_id AS product_id
    , (DIV(impression_count, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(impression_count, bundle_product_count), 0)) AS impression_count
    , (DIV(click_count, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(click_count, bundle_product_count), 0)) AS click_count
    , (DIV(ad_cost, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(ad_cost, bundle_product_count), 0)) AS ad_cost
    , (DIV(conv_count, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(conv_count, bundle_product_count), 0)) AS conv_count
    , (DIV(direct_conv_count, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(direct_conv_count, bundle_product_count), 0)) AS direct_conv_count
    , (DIV(conv_amount, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(conv_amount, bundle_product_count), 0)) AS conv_amount
    , (DIV(direct_conv_amount, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(direct_conv_amount, bundle_product_count), 0)) AS direct_conv_amount
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
