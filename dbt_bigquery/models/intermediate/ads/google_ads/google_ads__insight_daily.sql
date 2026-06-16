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

ad_id_to_ranged_sbn_ids AS (
  SELECT
      ad_id
    , ad_level
    , bundle_product_ids
    , start_date
    , end_date
  FROM {{ ref('relation__ad_id_to_ranged_sbn_ids') }}
  WHERE platform_name = '구글'
),

product_renewal_mapping AS (
  {{ core__product_renewal_mapping() }}
),

insight_daily AS (
  SELECT
      insight.ad_id
    , insight.device_type
    , COALESCE(
          rel_ad.bundle_product_ids
        , rel_grp.bundle_product_ids
        , rel_cmp.bundle_product_ids
        , acc.bundle_brand_ids
        , '200000'
      ) AS bundle_product_ids
    , insight.impression_count
    , insight.click_count
    , insight.ad_cost
    , insight.ymd
  FROM {{ source('google_ads', 'insight') }} AS insight
  LEFT JOIN (SELECT * FROM ad_id_to_ranged_sbn_ids WHERE ad_level = 0) AS rel_cmp
    ON insight.campaign_id = rel_cmp.ad_id
    AND insight.ymd BETWEEN rel_cmp.start_date AND rel_cmp.end_date
  LEFT JOIN (SELECT * FROM ad_id_to_ranged_sbn_ids WHERE ad_level = 1) AS rel_grp
    ON insight.adgroup_id = rel_grp.ad_id
    AND insight.ymd BETWEEN rel_grp.start_date AND rel_grp.end_date
  LEFT JOIN (SELECT * FROM ad_id_to_ranged_sbn_ids WHERE ad_level = 2) AS rel_ad
    ON insight.ad_id = rel_ad.ad_id
    AND insight.ymd BETWEEN rel_ad.start_date AND rel_ad.end_date
  LEFT JOIN {{ source('google_ads', 'account') }} AS acc
    ON insight.customer_id = acc.customer_id
  WHERE insight.ymd BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
),

bundle_product_insight AS (
  SELECT
      ad_id
    , device_type
    , ANY_VALUE(bundle_product_ids) AS bundle_product_ids
    , SUM(impression_count) AS impression_count
    , SUM(click_count) AS click_count
    , SUM(ad_cost) AS ad_cost
    , ymd
  FROM insight_daily
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
