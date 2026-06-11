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
    require_partition_filter = true,
    schema = 'google_ads',
    alias = 'report'
  )
}}

WITH

renewal_mapping AS (
  SELECT *
  FROM UNNEST([
    STRUCT('100169' AS product_id_old, '100863' AS product_id_new, DATE(2026, 2, 10) AS renewal_date)
  ])
),

insight_daily AS (
  SELECT
      ad_id
    , SUM(impression_count) AS impression_count
    , SUM(click_count) AS click_count
    , SUM(ad_cost) AS ad_cost
    , ymd
  FROM {{ source('google_ads', 'insight') }}
  WHERE ymd BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
  GROUP BY ymd, ad_id
),

bundle_product_insight AS (
  SELECT
      insight.ad_id
    , COALESCE(master.bundle_product_ids, '900000') AS bundle_product_ids
    , insight.impression_count
    , insight.click_count
    , insight.ad_cost
    , insight.ymd
  FROM insight_daily AS insight
  LEFT JOIN {{ ref('google_ads__master') }} AS master
    ON insight.ad_id = master.ad_id
),

exploded_product_insight AS (
  SELECT
      ad_id
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
    LEFT JOIN renewal_mapping AS renewal
      ON (bundle_product_id = renewal.product_id_new) AND (insight.ymd < renewal.renewal_date)
  ) AS t_
)

SELECT * FROM exploded_product_insight
