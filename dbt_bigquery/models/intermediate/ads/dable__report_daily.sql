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
  WHERE platform_name = '데이블'
),

product_renewal_mapping AS (
  {{ core__product_renewal_mapping() }}
),

report_daily AS (
  SELECT
      report.campaign_id
    , COALESCE(rel.bundle_product_ids, '200000') AS bundle_product_ids
    , report.expose_count
    , report.impression_count
    , report.click_count
    , report.ad_cost
    , report.conv_count
    , report.ymd
  FROM {{ source('dable', 'report') }} AS report
  LEFT JOIN ad_id_to_ranged_sbn_ids AS rel
    ON report.campaign_id = rel.campaign_id
    AND report.ymd BETWEEN rel.start_date AND rel.end_date
  WHERE report.ymd BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
),

bundle_product_report AS (
  SELECT
      campaign_id
    , ANY_VALUE(bundle_product_ids) AS bundle_product_ids
    , SUM(expose_count) AS expose_count
    , SUM(impression_count) AS impression_count
    , SUM(click_count) AS click_count
    , SUM(ad_cost) AS ad_cost
    , SUM(conv_count) AS conv_count
    , ymd
  FROM report_daily
  GROUP BY ymd, campaign_id
),

exploded_product_report AS (
  SELECT
      campaign_id
    , bundle_product_id AS product_id
    , (DIV(expose_count, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(expose_count, bundle_product_count), 0)) AS expose_count
    , (DIV(impression_count, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(impression_count, bundle_product_count), 0)) AS impression_count
    , (DIV(click_count, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(click_count, bundle_product_count), 0)) AS click_count
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
