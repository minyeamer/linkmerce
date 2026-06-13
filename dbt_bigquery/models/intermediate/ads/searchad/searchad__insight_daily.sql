{{
  config(
    materialized = 'incremental',
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

ad_id_to_sbn_ids AS (
  SELECT
      ad_id
    , ad_level
    , bundle_product_ids
  FROM {{ source('relation', 'ad_id_to_sbn_ids') }}
  WHERE platform_name = '네이버'
),

product_renewal_mapping AS (
  {{ core__product_renewal_mapping() }}
),

insight_sad_daily AS (
  SELECT
      sad.ad_id
    , sad.pc_mobile_type AS device_type
    , COALESCE(
          rel_prd.bundle_product_ids
        , rel_ad.bundle_product_ids
        , rel_grp.bundle_product_ids
        , rel_cmp.bundle_product_ids
        , acc.bundle_brand_ids
        , '900000'
      ) AS bundle_product_ids
    , sad.impression_count
    , sad.click_count
    , CAST(ROUND(IF(sad.ymd < DATE '2026-03-30', sad.ad_cost * 1.1, sad.ad_cost)) AS INT64) AS ad_cost
    , sad.ad_rank_sum
    , sad.conv_count
    , sad.direct_conv_count
    , sad.conv_amount
    , sad.direct_conv_amount
    , sad.ymd
  FROM {{ source('searchad', 'report_sad') }} AS sad
  LEFT JOIN {{ source('searchad', 'ad') }} AS ad
    ON sad.ad_id = ad.adgroup_id
  LEFT JOIN {{ source('searchad', 'adgroup') }} AS grp
    ON ad.adgroup_id = grp.adgroup_id
  -- Resolve bundle_product_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 0) AS rel_cmp
    ON grp.campaign_id = rel_cmp.ad_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 1) AS rel_grp
    ON ad.adgroup_id = rel_grp.ad_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 2) AS rel_ad
    ON sad.ad_id = rel_ad.ad_id
  LEFT JOIN {{ source('relation', 'smt_prd_to_sbn_ids') }} AS rel_prd
    ON ad.product_id = rel_prd.product_id
  LEFT JOIN {{ source('searchad', 'account') }} AS acc
    ON sad.customer_id = acc.customer_id
  WHERE sad.ymd BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
),

insight_gfa_daily AS (
  SELECT
      CAST(gfa.creative_no AS STRING) AS ad_id
    , 9 AS device_type
    , COALESCE(
          rel_ad.bundle_product_ids
        , rel_adset.bundle_product_ids
        , rel_cmp.bundle_product_ids
        , acc.bundle_brand_ids
        , '900000'
      ) AS bundle_product_ids
    , gfa.impression_count
    , gfa.click_count
    , gfa.ad_cost
    , NULL AS ad_rank_sum
    , gfa.conv_count
    , NULL AS direct_conv_count
    , gfa.conv_amount
    , NULL AS direct_conv_amount
    , gfa.ymd
  FROM {{ source('searchad', 'report_gfa') }} AS gfa
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 0) AS rel_cmp
    ON CAST(gfa.campaign_no AS STRING) = rel_cmp.ad_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 1) AS rel_adset
    ON CAST(gfa.adset_no AS STRING) = rel_adset.ad_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 2) AS rel_ad
    ON CAST(gfa.creative_no AS STRING) = rel_ad.ad_id
  LEFT JOIN {{ source('searchad', 'account') }} AS acc
    ON gfa.account_no = acc.customer_id
  WHERE gfa.ymd BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
),

bundle_product_insight AS (
  SELECT
      ad_id
    , device_type
    , ANY_VALUE(bundle_product_ids) AS bundle_product_ids
    , SUM(impression_count) AS impression_count
    , SUM(click_count) AS click_count
    , SUM(ad_cost) AS ad_cost
    , SUM(ad_rank_sum) AS ad_rank_sum
    , SUM(conv_count) AS conv_count
    , SUM(direct_conv_count) AS direct_conv_count
    , SUM(conv_amount) AS conv_amount
    , SUM(direct_conv_amount) AS direct_conv_amount
    , ymd
  FROM (
    SELECT * FROM insight_sad_daily
    UNION ALL
    SELECT * FROM insight_gfa_daily
  ) AS t_
  GROUP BY ymd, ad_id, device_type
),

exploded_product_insight AS (
  SELECT
      ad_id
    , device_type
    , bundle_product_id AS product_id
    , (DIV(impression_count, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(impression_count, bundle_product_count), 0)) AS impression_count
    , (DIV(click_count, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(click_count, bundle_product_count), 0)) AS click_count
    , (DIV(ad_cost, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(ad_cost, bundle_product_count), 0)) AS ad_cost
    , (DIV(ad_rank_sum, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(ad_rank_sum, bundle_product_count), 0)) AS ad_rank_sum
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
