{{
  config(
    materialized = 'table',
    schema = 'xfm_ads'
  )
}}

WITH{#

#} ad_id_to_sbn_ids AS (
  SELECT
      ad_id
    , ad_level
    , bundle_product_ids
  FROM {{ source('relation', 'ad_id_to_sbn_ids') }}
  WHERE platform_name = '메타'
),{#

#} objective_mapping AS (
  {{ meta_ads__objective_mapping() }}
),{#

#} effective_status_mapping AS (
  {{ meta_ads__effective_status_mapping() }}
),{#

#} ad_master AS (
  SELECT
      ad.account_id
    , acc.account_name
    -- Campaign attributes
    , ad.campaign_id
    , cmp.campaign_name
    , objective.label AS objective
    -- Adset attributes
    , ad.adset_id
    , adset.adset_name
    -- Ad attributes
    , ad.ad_id
    , ad.ad_name
    , status_fin.label AS effective_status
    , COALESCE(
          rel_ad.bundle_product_ids
        , rel_adset.bundle_product_ids
        , rel_cmp.bundle_product_ids
      ) AS bundle_product_ids
    , COALESCE(ad.created_at, adset.created_at, cmp.created_at) AS created_at
    -- Sort key
    , (
        (CASE WHEN status_fin.code = 'DELETED' THEN 2 ELSE 1 END) * 100 * 100
        + COALESCE(acc.account_seq, 99)                           * 100
        + COALESCE(objective.seq, 99)
      ) AS sort_key
  FROM {{ source('meta_ads', 'ad') }} AS ad
  LEFT JOIN {{ source('meta_ads', 'account') }} AS acc
    ON ad.account_id = acc.account_id
  LEFT JOIN {{ source('meta_ads', 'campaign') }} AS cmp
    ON ad.campaign_id = cmp.campaign_id
  LEFT JOIN {{ source('meta_ads', 'adset') }} AS adset
    ON ad.adset_id = adset.adset_id
  -- Map codes to labels
  LEFT JOIN objective_mapping AS objective
    ON cmp.objective = objective.code
  -- Resolve final status label
  LEFT JOIN effective_status_mapping AS status_cmp
    ON cmp.effective_status = status_cmp.code
  LEFT JOIN effective_status_mapping AS status_adset
    ON adset.effective_status = status_adset.code
  LEFT JOIN effective_status_mapping AS status_ad
    ON ad.effective_status = status_ad.code
  LEFT JOIN effective_status_mapping AS status_fin
    ON GREATEST(status_cmp.seq, status_adset.seq, status_ad.seq) = status_fin.seq
  -- Resolve bundle_product_ids
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 0) AS rel_cmp
    ON ad.campaign_id = rel_cmp.ad_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 1) AS rel_adset
    ON ad.adset_id = rel_adset.ad_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 2) AS rel_ad
    ON ad.ad_id = rel_ad.ad_id
){#

#} SELECT * FROM ad_master
