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
      ad_id AS campaign_id
    , bundle_product_ids
    , start_date
    , end_date
  FROM {{ ref('relation__ad_id_to_ranged_sbn_ids') }}
  WHERE platform_name = '틱톡'
),

product_renewal_mapping AS (
  {{ core__product_renewal_mapping() }}
),

report_daily AS (
  SELECT
      report.campaign_id
    , report.adgroup_id
    , report.ad_id
    , report.ad_type
    , COALESCE(rel.bundle_product_ids, '200000') AS bundle_product_ids
    , report.impression_count
    , report.click_count
    , report.reach_count
    , report.ad_cost
    , report.conv_count
    , report.ymd
  FROM {{ source('tiktok_ads', 'report') }} AS report
  LEFT JOIN ad_id_to_ranged_sbn_ids AS rel
    ON report.campaign_id = rel.campaign_id
    AND report.ymd BETWEEN rel.start_date AND rel.end_date
  WHERE report.ymd BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
),

bundle_product_report AS (
  SELECT
      campaign_id
    , adgroup_id
    , ad_id
    , ad_type
    , ANY_VALUE(bundle_product_ids) AS bundle_product_ids
    , SUM(impression_count) AS impression_count
    , SUM(click_count) AS click_count
    , SUM(reach_count) AS reach_count
    , SUM(ad_cost) AS ad_cost
    , SUM(conv_count) AS conv_count
    , ymd
  FROM report_daily
  GROUP BY ymd, campaign_id, adgroup_id, ad_id, ad_type
),

exploded_product_report AS (
  SELECT
      campaign_id
    , adgroup_id
    , ad_id
    , ad_type
    , bundle_product_id AS product_id
    , (DIV(impression_count, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(impression_count, bundle_product_count), 0)) AS impression_count
    , (DIV(click_count, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(click_count, bundle_product_count), 0)) AS click_count
    , (DIV(reach_count, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(reach_count, bundle_product_count), 0)) AS reach_count
    , (DIV(ad_cost, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(ad_cost, bundle_product_count), 0)) AS ad_cost
    , (DIV(conv_count, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(conv_count, bundle_product_count), 0)) AS conv_count
    , ymd
  FROM (
    SELECT
        report.*
      , COALESCE(renewal.product_id_old, bundle_product_id) AS bundle_product_id
      , bundle_product_offset
      , ARRAY_LENGTH(SPLIT(report.bundle_product_ids, ',')) AS bundle_product_count
    FROM bundle_product_report AS report
    CROSS JOIN UNNEST(SPLIT(report.bundle_product_ids, ',')) AS bundle_product_id WITH OFFSET AS bundle_product_offset
    LEFT JOIN product_renewal_mapping AS renewal
      ON (bundle_product_id = renewal.product_id_new) AND (report.ymd < renewal.renewal_date)
  ) AS t_
)

SELECT * FROM exploded_product_report
