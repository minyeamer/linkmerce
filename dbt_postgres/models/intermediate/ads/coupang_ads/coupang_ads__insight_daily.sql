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

#} product_renewal_mapping AS (
  {{ core__product_renewal_mapping() }}
),{#

#} cpg_opt_to_sbn_ids AS (
  SELECT
      option_id
    , (
        SELECT string_agg((string_to_array(bundle_product_id, ':'))[1], ',')
        FROM unnest(string_to_array(bundle_product_ids, ',')) AS t(bundle_product_id)
      ) AS bundle_product_ids
  FROM {{ source('relation', 'cpg_opt_to_sbn_ids') }}
),{#

#} insight_pa_daily AS (
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
    , (pa.ad_cost * 1.1)::integer AS ad_cost
    , pa.conv_count
    , pa.direct_conv_count
    , pa.conv_amount
    , pa.direct_conv_amount
    , pa.ymd
  FROM {{ source('coupang_ads', 'report_pa') }} AS pa
  LEFT JOIN cpg_opt_to_sbn_ids AS rel_opt
    ON pa.option_id = rel_opt.option_id
  LEFT JOIN {{ source('coupang', 'vendor') }} AS vdr
    ON pa.vendor_id = vdr.vendor_id
  WHERE pa.ymd BETWEEN {{ pg_batch_start_date() }} AND {{ pg_batch_end_date() }}
),{#

#} insight_nca_daily AS (
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
    , (nca.ad_cost * 1.1)::integer AS ad_cost
    , NULL::integer AS conv_count
    , NULL::integer AS direct_conv_count
    , NULL::integer AS conv_amount
    , NULL::integer AS direct_conv_amount
    , nca.ymd
  FROM {{ source('coupang_ads', 'report_nca') }} AS nca
  LEFT JOIN {{ source('coupang_ads', 'creative') }} AS ad
    ON nca.creative_id = ad.creative_id
  LEFT JOIN cpg_opt_to_sbn_ids AS rel_opt
    ON COALESCE(nca.option_id, ad.option_id) = rel_opt.option_id
  LEFT JOIN {{ source('coupang', 'vendor') }} AS vdr
    ON nca.vendor_id = vdr.vendor_id
  WHERE nca.ymd BETWEEN {{ pg_batch_start_date() }} AND {{ pg_batch_end_date() }}
),{#

#} bundle_product_insight AS (
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
),{#

#} exploded_product_insight AS (
  SELECT
      campaign_id
    , option_id
    , placement_group
    , bundle_product_id AS product_id
    , (DIV(impression_count, bundle_product_count)
      + (CASE WHEN bundle_product_offset = 1 THEN MOD(impression_count, bundle_product_count) ELSE 0 END)) AS impression_count
    , (DIV(click_count, bundle_product_count)
      + (CASE WHEN bundle_product_offset = 1 THEN MOD(click_count, bundle_product_count) ELSE 0 END)) AS click_count
    , (DIV(ad_cost, bundle_product_count)
      + (CASE WHEN bundle_product_offset = 1 THEN MOD(ad_cost, bundle_product_count) ELSE 0 END)) AS ad_cost
    , (DIV(conv_count, bundle_product_count)
      + (CASE WHEN bundle_product_offset = 1 THEN MOD(conv_count, bundle_product_count) ELSE 0 END)) AS conv_count
    , (DIV(direct_conv_count, bundle_product_count)
      + (CASE WHEN bundle_product_offset = 1 THEN MOD(direct_conv_count, bundle_product_count) ELSE 0 END)) AS direct_conv_count
    , (DIV(conv_amount, bundle_product_count)
      + (CASE WHEN bundle_product_offset = 1 THEN MOD(conv_amount, bundle_product_count) ELSE 0 END)) AS conv_amount
    , (DIV(direct_conv_amount, bundle_product_count)
      + (CASE WHEN bundle_product_offset = 1 THEN MOD(direct_conv_amount, bundle_product_count) ELSE 0 END)) AS direct_conv_amount
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
