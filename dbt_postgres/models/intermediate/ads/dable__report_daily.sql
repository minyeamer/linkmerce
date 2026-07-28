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
      ad_id AS campaign_id
    , bundle_product_ids
    , start_date
    , end_date
  FROM {{ ref('relation__ad_id_to_ranged_sbn_ids') }}
  WHERE platform_name = '데이블'
),{#

#} product_renewal_mapping AS (
  {{ core__product_renewal_mapping() }}
),{#

#} report_daily AS (
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
  WHERE report.ymd BETWEEN {{ pg_batch_start_date() }} AND {{ pg_batch_end_date() }}
),{#

#} bundle_product_report AS (
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
),{#

#} exploded_product_report AS (
  SELECT
      campaign_id
    , bundle_product_id AS product_id
    , (DIV(expose_count, bundle_product_count)
      + (CASE WHEN bundle_product_offset = 0 THEN MOD(expose_count, bundle_product_count) ELSE 0 END)) AS expose_count
    , (DIV(impression_count, bundle_product_count)
      + (CASE WHEN bundle_product_offset = 0 THEN MOD(impression_count, bundle_product_count) ELSE 0 END)) AS impression_count
    , (DIV(click_count, bundle_product_count)
      + (CASE WHEN bundle_product_offset = 0 THEN MOD(click_count, bundle_product_count) ELSE 0 END)) AS click_count
    , (DIV(ad_cost, bundle_product_count)
      + (CASE WHEN bundle_product_offset = 0 THEN MOD(ad_cost, bundle_product_count) ELSE 0 END)) AS ad_cost
    , (DIV(conv_count, bundle_product_count)
      + (CASE WHEN bundle_product_offset = 0 THEN MOD(conv_count, bundle_product_count) ELSE 0 END)) AS conv_count
    , ymd
  FROM (
    SELECT
        report.*
      , COALESCE(renewal.product_id_old, bundle_product_id) AS bundle_product_id
      , bundle_product_offset
      , cardinality(string_to_array(report.bundle_product_ids, ',')) AS bundle_product_count
    FROM bundle_product_report AS report
    CROSS JOIN LATERAL (
      SELECT bundle_product_id, bundle_product_offset
      FROM unnest(string_to_array(report.bundle_product_ids, ','))
      WITH ORDINALITY AS t(bundle_product_id, bundle_product_offset)
    ) AS t1_
    LEFT JOIN product_renewal_mapping AS renewal
      ON (bundle_product_id = renewal.product_id_new) AND (report.ymd < renewal.renewal_date)
  ) AS t_
){#

#} SELECT * FROM exploded_product_report
