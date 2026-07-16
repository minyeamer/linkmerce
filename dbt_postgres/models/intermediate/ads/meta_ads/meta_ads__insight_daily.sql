{{
  config(
    materialized = 'partitioned_table',
    schema = 'xfm_ads',
    partition_by = {
      "field": "ymd",
      "data_type": "date",
      "granularity": "day"
    },
    partitions = pg_date_partitions('ds_start_date', 'ds_end_date')
  )
}}

WITH{#

#} ad_id_to_ranged_sbn_ids AS (
  SELECT
      ad_id
    , ad_level
    , bundle_product_ids
    , start_date
    , end_date
  FROM {{ ref('relation__ad_id_to_ranged_sbn_ids') }}
  WHERE platform_name = '메타'
),{#

#} product_renewal_mapping AS (
  {{ core__product_renewal_mapping() }}
),{#

#} insight_daily AS (
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
  LEFT JOIN (SELECT * FROM ad_id_to_ranged_sbn_ids WHERE ad_level = 0) AS rel_cmp
    ON insight.campaign_id = rel_cmp.ad_id
    AND insight.ymd BETWEEN rel_cmp.start_date AND rel_cmp.end_date
  LEFT JOIN (SELECT * FROM ad_id_to_ranged_sbn_ids WHERE ad_level = 1) AS rel_adset
    ON insight.adset_id = rel_adset.ad_id
    AND insight.ymd BETWEEN rel_adset.start_date AND rel_adset.end_date
  LEFT JOIN (SELECT * FROM ad_id_to_ranged_sbn_ids WHERE ad_level = 2) AS rel_ad
    ON insight.ad_id = rel_ad.ad_id
    AND insight.ymd BETWEEN rel_ad.start_date AND rel_ad.end_date
  LEFT JOIN {{ source('meta_ads', 'account') }} AS acc
    ON insight.account_id = acc.account_id
  WHERE insight.ymd BETWEEN DATE '{{ var("ds_start_date") }}' AND DATE '{{ var("ds_end_date") }}'
),{#

#} bundle_product_insight AS (
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
),{#

#} exploded_product_insight AS (
  SELECT
      ad_id
    , bundle_product_id AS product_id
    , (DIV(impression_count, bundle_product_count)
      + (CASE WHEN bundle_product_offset = 1 THEN MOD(impression_count, bundle_product_count) ELSE 0 END)) AS impression_count
    , (DIV(reach_count, bundle_product_count)
      + (CASE WHEN bundle_product_offset = 1 THEN MOD(reach_count, bundle_product_count) ELSE 0 END)) AS reach_count
    , (DIV(click_count, bundle_product_count)
      + (CASE WHEN bundle_product_offset = 1 THEN MOD(click_count, bundle_product_count) ELSE 0 END)) AS click_count
    , (DIV(link_click_count, bundle_product_count)
      + (CASE WHEN bundle_product_offset = 1 THEN MOD(link_click_count, bundle_product_count) ELSE 0 END)) AS link_click_count
    , (DIV(ad_cost, bundle_product_count)
      + (CASE WHEN bundle_product_offset = 1 THEN MOD(ad_cost, bundle_product_count) ELSE 0 END)) AS ad_cost
    , ymd
  FROM (
    SELECT
        insight.*
      , COALESCE(renewal.product_id_old, bundle_product_id) AS bundle_product_id
      , bundle_product_offset
      , cardinality(string_to_array(insight.bundle_product_ids, ',')) AS bundle_product_count
    FROM bundle_product_insight AS insight
    CROSS JOIN LATERAL (
      SELECT bundle_product_id, bundle_product_offset
      FROM unnest(string_to_array(insight.bundle_product_ids, ','))
      WITH ORDINALITY AS t(bundle_product_id, bundle_product_offset)
    ) AS t1_
    LEFT JOIN product_renewal_mapping AS renewal
      ON (bundle_product_id = renewal.product_id_new) AND (insight.ymd < renewal.renewal_date)
  ) AS t2_
){#

#} SELECT * FROM exploded_product_insight
