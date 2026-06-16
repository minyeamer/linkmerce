{{
  config(
    materialized = 'table',
    schema = 'xfm_ads',
    partition_by = {
      "field": "ymd",
      "data_type": "date",
      "granularity": "day"
    },
    require_partition_filter = false
  )
}}

WITH

product_renewal_mapping AS (
  {{ core__product_renewal_mapping() }}
),

-- Step 1: prepare relations

ad_id_to_ranged_sbn_ids AS (
  SELECT
      ad_id
    , ad_level
    , bundle_product_ids
    , start_date
    , end_date
  FROM {{ ref('relation__ad_id_to_ranged_sbn_ids') }}
  WHERE platform_name = '네이버'
),

smt_prd_to_ranged_sbn_ids AS (
  SELECT
      product_id
    , bundle_product_ids
    , start_date
    , end_date
  FROM {{ ref('relation__smt_prd_to_ranged_sbn_ids') }}
),

smt_prd_to_ranged_prd_ids AS (
  SELECT
      product_id
    , bundle_product_ids
    , start_date
    , end_date
  FROM smt_prd_to_ranged_sbn_ids
  WHERE LEFT(bundle_product_ids, 1) != '2'
),

smt_prd_to_ranged_brd_ids AS (
  SELECT
      product_id
    , bundle_product_ids AS bundle_brand_ids
    , start_date
    , end_date
  FROM smt_prd_to_ranged_sbn_ids
  WHERE LEFT(bundle_product_ids, 1) = '2'
),

-- Step 2: prepare contracts and expand them by day

contract_base AS (
  SELECT
      sad.contract_id
    , sad.adgroup_id
    , sad.customer_id
    , sad.contract_amount - COALESCE(sad.refund_amount, 0) AS ad_cost
    , sad.exposure_start_date
    , sad.exposure_end_date
    , DATE_DIFF(sad.exposure_end_date, sad.exposure_start_date, DAY) + 1 AS date_count
  FROM {{ source('searchad', 'contract') }} AS sad
  WHERE sad.contract_end_date >= DATE('{{ var("ds_start_date") }}')
    AND sad.exposure_start_date IS NOT NULL
    AND sad.exposure_end_date IS NOT NULL
),

contract_expand AS (
  SELECT
      sad.contract_id
    , sad.adgroup_id
    , sad.customer_id
    , SAFE_CAST(sad.ad_cost / NULLIF(sad.date_count, 0) AS INT64) AS ad_cost
    , DATE_ADD(sad.exposure_start_date, INTERVAL date_offset DAY) AS ymd
  FROM contract_base AS sad
  CROSS JOIN UNNEST(GENERATE_ARRAY(0, sad.date_count - 1)) AS date_offset
  WHERE DATE_ADD(sad.exposure_start_date, INTERVAL date_offset DAY)
    BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
),

contract_dates AS (
  SELECT DISTINCT
      adgroup_id
    , ymd
  FROM contract_expand
),

-- Step 3: build adgroup-level bundle mappings from ad-level ranged rules

adgroup_ad_id_to_ranged_sbn_ids AS (
  SELECT
      ad.adgroup_id
    , rel_ad.bundle_product_ids
    , rel_ad.start_date
    , rel_ad.end_date
  FROM {{ source('searchad', 'ad') }} AS ad
  INNER JOIN (SELECT * FROM ad_id_to_ranged_sbn_ids WHERE ad_level = 2) AS rel_ad
    ON ad.ad_id = rel_ad.ad_id

  UNION ALL

  SELECT
      ad.adgroup_id
    , rel_prd.bundle_product_ids
    , rel_prd.start_date
    , rel_prd.end_date
  FROM {{ source('searchad', 'ad') }} AS ad
  INNER JOIN smt_prd_to_ranged_prd_ids AS rel_prd
    ON ad.product_id = rel_prd.product_id

  UNION ALL

  SELECT
      ad.adgroup_id
    , rel_brd.bundle_brand_ids AS bundle_product_ids
    , rel_brd.start_date
    , rel_brd.end_date
  FROM {{ source('searchad', 'ad') }} AS ad
  INNER JOIN smt_prd_to_ranged_brd_ids AS rel_brd
    ON ad.product_id = rel_brd.product_id
),

adgroup_ad_id_to_daily_sbn_ids AS (
  SELECT
      rel.adgroup_id
    , NULLIF(TRIM(bundle_product_id), '') AS bundle_product_id
    , dates.ymd
  FROM adgroup_ad_id_to_ranged_sbn_ids AS rel
  INNER JOIN contract_dates AS dates
    ON rel.adgroup_id = dates.adgroup_id
    AND dates.ymd BETWEEN rel.start_date AND rel.end_date
  CROSS JOIN UNNEST(SPLIT(rel.bundle_product_ids, ',')) AS bundle_product_id
  WHERE rel.bundle_product_ids IS NOT NULL
    AND bundle_product_id != '200000'
),

adgroup_ad_id_to_daily_prd_ids AS (
  SELECT
      adgroup_id
    , STRING_AGG(DISTINCT bundle_product_id, ',') AS bundle_product_ids
    , ymd
  FROM adgroup_ad_id_to_daily_sbn_ids
  WHERE NOT STARTS_WITH(bundle_product_id, '2')
  GROUP BY adgroup_id, ymd
),

adgroup_ad_id_to_daily_brd_ids AS (
  SELECT
      adgroup_id
    , STRING_AGG(DISTINCT bundle_product_id, ',') AS bundle_product_ids
    , ymd
  FROM adgroup_ad_id_to_daily_sbn_ids
  WHERE STARTS_WITH(bundle_product_id, '2')
  GROUP BY adgroup_id, ymd
),

-- Step 4: attach bundle_product_ids to each daily contract row

bundle_product_contract AS (
  SELECT
      sad.contract_id
    , sad.adgroup_id
    , COALESCE(
          rel_grp_prd.bundle_product_ids
        , rel_grp.bundle_product_ids
        , rel_cmp.bundle_product_ids
        , rel_grp_brd.bundle_product_ids
        , acc.bundle_brand_ids
        , '200000'
      ) AS bundle_product_ids
    , sad.ad_cost
    , sad.ymd
  FROM contract_expand AS sad
  LEFT JOIN {{ source('searchad', 'adgroup') }} AS grp
    ON sad.adgroup_id = grp.adgroup_id
  -- Resolve bundle_product_ids using ad_id
  LEFT JOIN (SELECT * FROM ad_id_to_ranged_sbn_ids WHERE ad_level = 0) AS rel_cmp
    ON grp.campaign_id = rel_cmp.ad_id
    AND sad.ymd BETWEEN rel_cmp.start_date AND rel_cmp.end_date
  LEFT JOIN (SELECT * FROM ad_id_to_ranged_sbn_ids WHERE ad_level = 1) AS rel_grp
    ON sad.adgroup_id = rel_grp.ad_id
    AND sad.ymd BETWEEN rel_grp.start_date AND rel_grp.end_date
  -- Resolve bundle_product_ids using adgroup_id
  LEFT JOIN adgroup_ad_id_to_daily_prd_ids AS rel_grp_prd
    ON sad.adgroup_id = rel_grp_prd.adgroup_id
    AND sad.ymd = rel_grp_prd.ymd
  LEFT JOIN adgroup_ad_id_to_daily_brd_ids AS rel_grp_brd
    ON sad.adgroup_id = rel_grp_brd.adgroup_id
    AND sad.ymd = rel_grp_brd.ymd
  -- Resolve bundle_product_ids using customer_id
  LEFT JOIN {{ source('searchad', 'account') }} AS acc
    ON sad.customer_id = acc.customer_id
),

-- Step 5: explode bundle products and allocate contract cost with equal weight

exploded_product_contract AS (
  SELECT
      contract_id
    , adgroup_id
    , bundle_product_id AS product_id
    , (DIV(ad_cost, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(ad_cost, bundle_product_count), 0)) AS ad_cost
    , ymd
  FROM (
    SELECT
        sad.*
      , COALESCE(renewal.product_id_old, bundle_product_id) AS bundle_product_id
      , bundle_product_offset
      , ARRAY_LENGTH(SPLIT(sad.bundle_product_ids, ',')) AS bundle_product_count
    FROM bundle_product_contract AS sad
    CROSS JOIN UNNEST(SPLIT(sad.bundle_product_ids, ',')) AS bundle_product_id WITH OFFSET AS bundle_product_offset
    LEFT JOIN product_renewal_mapping AS renewal
      ON (bundle_product_id = renewal.product_id_new) AND (sad.ymd < renewal.renewal_date)
  ) AS t_
)

SELECT * FROM exploded_product_contract
