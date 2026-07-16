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

-- Step 1: prepare relations

#} ad_id_to_ranged_sbn_ids AS (
  SELECT
      ad_id
    , ad_level
    , bundle_product_ids
    , start_date
    , end_date
  FROM {{ ref('relation__ad_id_to_ranged_sbn_ids') }}
  WHERE platform_name = '네이버'
),{#

#} smt_prd_to_ranged_sbn_ids AS (
  SELECT
      product_id
    , bundle_product_ids
    , start_date
    , end_date
  FROM {{ ref('relation__smt_prd_to_ranged_sbn_ids') }}
),{#

#} smt_prd_to_ranged_prd_ids AS (
  SELECT
      product_id
    , bundle_product_ids
    , start_date
    , end_date
  FROM smt_prd_to_ranged_sbn_ids
  WHERE NOT starts_with(bundle_product_ids, '2')
),{#

#} smt_prd_to_ranged_brd_ids AS (
  SELECT
      product_id
    , bundle_product_ids AS bundle_brand_ids
    , start_date
    , end_date
  FROM smt_prd_to_ranged_sbn_ids
  WHERE starts_with(bundle_product_ids, '2')
),{#

-- Step 2: prepare searchad and gfa reports

#} insight_sad_daily AS (
  SELECT
      grp.campaign_id
    , sad.ad_id
    , sad.pc_mobile_type AS device_type
    , COALESCE(
          rel_prd.bundle_product_ids
        , rel_ad.bundle_product_ids
        , rel_grp.bundle_product_ids
        , rel_cmp.bundle_product_ids
        , rel_brd.bundle_brand_ids
        , acc.bundle_brand_ids
        , '200000'
      ) AS bundle_product_ids
    , sad.impression_count
    , sad.click_count
    , CAST(ROUND((
        CASE
          WHEN sad.ymd < DATE '2026-03-30' THEN sad.ad_cost * 1.1
          ELSE sad.ad_cost
        END
      )) AS bigint) AS ad_cost
    , sad.ad_rank_sum
    , sad.conv_count
    , sad.direct_conv_count
    , sad.conv_amount
    , sad.direct_conv_amount
    , sad.ymd
  FROM {{ source('searchad', 'report_sad') }} AS sad
  LEFT JOIN {{ source('searchad', 'ad') }} AS ad
    ON sad.ad_id = ad.ad_id
  LEFT JOIN {{ source('searchad', 'adgroup') }} AS grp
    ON ad.adgroup_id = grp.adgroup_id
  -- Resolve bundle_product_ids using ad_id
  LEFT JOIN (SELECT * FROM ad_id_to_ranged_sbn_ids WHERE ad_level = 0) AS rel_cmp
    ON grp.campaign_id = rel_cmp.ad_id
    AND sad.ymd BETWEEN rel_cmp.start_date AND rel_cmp.end_date
  LEFT JOIN (SELECT * FROM ad_id_to_ranged_sbn_ids WHERE ad_level = 1) AS rel_grp
    ON ad.adgroup_id = rel_grp.ad_id
    AND sad.ymd BETWEEN rel_grp.start_date AND rel_grp.end_date
  LEFT JOIN (SELECT * FROM ad_id_to_ranged_sbn_ids WHERE ad_level = 2) AS rel_ad
    ON sad.ad_id = rel_ad.ad_id
    AND sad.ymd BETWEEN rel_ad.start_date AND rel_ad.end_date
  -- Resolve bundle_product_ids using product_id
  LEFT JOIN smt_prd_to_ranged_prd_ids AS rel_prd
    ON ad.product_id = rel_prd.product_id
    AND sad.ymd BETWEEN rel_prd.start_date AND rel_prd.end_date
  LEFT JOIN smt_prd_to_ranged_brd_ids AS rel_brd
    ON ad.product_id = rel_brd.product_id
    AND sad.ymd BETWEEN rel_brd.start_date AND rel_brd.end_date
  -- Resolve bundle_product_ids using customer_id
  LEFT JOIN {{ source('searchad', 'account') }} AS acc
    ON sad.customer_id = acc.customer_id
  WHERE sad.ymd BETWEEN DATE '{{ var("ds_start_date") }}' AND DATE '{{ var("ds_end_date") }}'
),{#

#} insight_gfa_daily AS (
  SELECT
      CAST(gfa.campaign_no AS text) AS campaign_id
    , CAST(gfa.creative_no AS text) AS ad_id
    , 9 AS device_type
    , COALESCE(
          rel_prd.bundle_product_ids
        , rel_ad.bundle_product_ids
        , rel_adset.bundle_product_ids
        , rel_cmp.bundle_product_ids
        , rel_brd.bundle_brand_ids
        , acc.bundle_brand_ids
        , '200000'
      ) AS bundle_product_ids
    , gfa.impression_count
    , gfa.click_count
    , gfa.ad_cost
    , NULL::integer AS ad_rank_sum
    , gfa.conv_count
    , NULL::integer AS direct_conv_count
    , gfa.conv_amount
    , NULL::integer AS direct_conv_amount
    , gfa.ymd
  FROM {{ source('searchad', 'report_gfa') }} AS gfa
  LEFT JOIN {{ source('searchad', 'ad') }} AS ad
    ON CAST(gfa.creative_no AS text) = ad.ad_id
  -- Resolve bundle_product_ids using ad_id
  LEFT JOIN (SELECT * FROM ad_id_to_ranged_sbn_ids WHERE ad_level = 0) AS rel_cmp
    ON CAST(gfa.campaign_no AS text) = rel_cmp.ad_id
    AND gfa.ymd BETWEEN rel_cmp.start_date AND rel_cmp.end_date
  LEFT JOIN (SELECT * FROM ad_id_to_ranged_sbn_ids WHERE ad_level = 1) AS rel_adset
    ON CAST(gfa.adset_no AS text) = rel_adset.ad_id
    AND gfa.ymd BETWEEN rel_adset.start_date AND rel_adset.end_date
  LEFT JOIN (SELECT * FROM ad_id_to_ranged_sbn_ids WHERE ad_level = 2) AS rel_ad
    ON CAST(gfa.creative_no AS text) = rel_ad.ad_id
    AND gfa.ymd BETWEEN rel_ad.start_date AND rel_ad.end_date
  -- Resolve bundle_product_ids using product_id
  LEFT JOIN smt_prd_to_ranged_prd_ids AS rel_prd
    ON ad.product_id = rel_prd.product_id
    AND gfa.ymd BETWEEN rel_prd.start_date AND rel_prd.end_date
  LEFT JOIN smt_prd_to_ranged_brd_ids AS rel_brd
    ON ad.product_id = rel_brd.product_id
    AND gfa.ymd BETWEEN rel_brd.start_date AND rel_brd.end_date
  -- Resolve bundle_product_ids using customer_id
  LEFT JOIN {{ source('searchad', 'account') }} AS acc
    ON gfa.account_no = acc.customer_id
  WHERE gfa.ymd BETWEEN DATE '{{ var("ds_start_date") }}' AND DATE '{{ var("ds_end_date") }}'
),{#

-- Step 3: union the reports and aggregate by dimensions

#} bundle_product_insight AS (
  SELECT
      campaign_id
    , ad_id
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
  GROUP BY ymd, campaign_id, ad_id, device_type
),{#

-- Step 4: explode bundle products and allocate metrics with equal weight

#} exploded_product_insight AS (
  SELECT
      campaign_id
    , ad_id
    , device_type
    , bundle_product_id AS product_id
    , (DIV(impression_count, bundle_product_count)
      + (CASE WHEN bundle_product_offset = 1 THEN MOD(impression_count, bundle_product_count) ELSE 0 END)) AS impression_count
    , (DIV(click_count, bundle_product_count)
      + (CASE WHEN bundle_product_offset = 1 THEN MOD(click_count, bundle_product_count) ELSE 0 END)) AS click_count
    , (DIV(ad_cost, bundle_product_count)
      + (CASE WHEN bundle_product_offset = 1 THEN MOD(ad_cost, bundle_product_count) ELSE 0 END)) AS ad_cost
    , (DIV(ad_rank_sum, bundle_product_count)
      + (CASE WHEN bundle_product_offset = 1 THEN MOD(ad_rank_sum, bundle_product_count) ELSE 0 END)) AS ad_rank_sum
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
