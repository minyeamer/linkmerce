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
  WHERE platform_name = '메타'
),

product_renewal_mapping AS (
  {{ core__product_renewal_mapping() }}
),

insight_daily AS (
  SELECT
      insight.ad_id
    , COALESCE(
          rel_ad.bundle_product_ids
        , rel_adset.bundle_product_ids
        , rel_cmp.bundle_product_ids
        , acc.bundle_brand_ids
        , '200000'
      ) AS bundle_product_ids
    , insight.impression_count
    , insight.reach_count
    , insight.click_count
    , insight.link_click_count
    , insight.ad_cost
    , insight.ymd
  FROM {{ source('meta_ads', 'insight') }} AS insight
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 0) AS rel_cmp
    ON insight.campaign_id = rel_cmp.ad_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 1) AS rel_adset
    ON insight.adset_id = rel_adset.ad_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 2) AS rel_ad
    ON insight.ad_id = rel_ad.ad_id
  LEFT JOIN {{ source('meta_ads', 'account') }} AS acc
    ON insight.account_id = acc.account_id
  WHERE insight.ymd BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
),

bundle_product_insight AS (
  SELECT
      ad_id
    , ANY_VALUE(bundle_product_ids) AS bundle_product_ids
    , SUM(impression_count) AS impression_count
    , SUM(reach_count) AS reach_count
    , SUM(click_count) AS click_count
    , SUM(link_click_count) AS link_click_count
    , SUM(ad_cost) AS ad_cost
    , ymd
  FROM insight_daily
  GROUP BY ymd, ad_id
),

exploded_product_insight AS (
  SELECT
      ad_id
    , bundle_product_id AS product_id
    , (DIV(impression_count, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(impression_count, bundle_product_count), 0)) AS impression_count
    , (DIV(reach_count, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(reach_count, bundle_product_count), 0)) AS reach_count
    , (DIV(click_count, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(click_count, bundle_product_count), 0)) AS click_count
    , (DIV(link_click_count, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(link_click_count, bundle_product_count), 0)) AS link_click_count
    , (DIV(ad_cost, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(ad_cost, bundle_product_count), 0)) AS ad_cost
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
