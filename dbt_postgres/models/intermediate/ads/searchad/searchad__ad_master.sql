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
  WHERE platform_name = '네이버'
),{#

#} campaign_type_mapping AS (
  {{ searchad__campaign_type_mapping() }}
),{#

#} adgroup_type_mapping AS (
  {{ searchad__adgroup_type_mapping() }}
),{#

#} ad_type_mapping AS (
  {{ searchad__ad_type_mapping() }}
),{#

#} ad_master AS (
  SELECT
      ad.customer_id
    , acc.account_name
    , acc.account_type
    -- Campaign attributes
    , grp.campaign_id
    , cmp.campaign_name
    , campaign_type.label AS campaign_type
    -- Adgroup attributes
    , ad.adgroup_id
    , grp.adgroup_name
    , adgroup_type.label AS adgroup_type
    -- Ad attributes
    , ad.ad_id
    , ad.title
    , ad.description
    , ad_type.label AS ad_type
    , ad.product_id AS mall_product_id
    , COALESCE(
          rel_prd.bundle_product_ids
        , rel_ad.bundle_product_ids
        , rel_grp.bundle_product_ids
        , rel_cmp.bundle_product_ids
      ) AS bundle_product_ids
    , ad.bid_amount
    , COALESCE(cmp.is_enabled AND grp.is_enabled AND ad.is_enabled, ad.is_enabled) AS is_enabled
    , ad.is_deleted
    , ad.landing_url_pc
    , ad.landing_url_mobile
    , ad.created_at
    , ad.deleted_at
    -- Sort key
    , (
        (CASE WHEN ad.is_deleted THEN 2 ELSE 1 END) * 100 * 100 * 100 * 100
        + COALESCE(acc.account_seq, 99)             * 100 * 100 * 100
        + COALESCE(campaign_type.seq, 99)           * 100 * 100
        + COALESCE(adgroup_type.seq, 99)            * 100
        + COALESCE(ad_type.seq, 99)
      ) AS sort_key
  FROM {{ source('searchad', 'ad') }} AS ad
  LEFT JOIN {{ source('searchad', 'account') }} AS acc
    ON ad.customer_id = acc.customer_id
  LEFT JOIN {{ source('searchad', 'adgroup') }} AS grp
    ON ad.adgroup_id = grp.adgroup_id
  LEFT JOIN {{ source('searchad', 'campaign') }} AS cmp
    ON grp.campaign_id = cmp.campaign_id
  -- Map codes to labels
  LEFT JOIN campaign_type_mapping AS campaign_type
    ON cmp.campaign_type = campaign_type.code
  LEFT JOIN adgroup_type_mapping AS adgroup_type
    ON grp.adgroup_type = adgroup_type.code
  LEFT JOIN ad_type_mapping AS ad_type
    ON ad.ad_type = ad_type.code
  -- Resolve bundle_product_ids
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 0) AS rel_cmp
    ON grp.campaign_id = rel_cmp.ad_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 1) AS rel_grp
    ON ad.adgroup_id = rel_grp.ad_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 2) AS rel_ad
    ON ad.ad_id = rel_ad.ad_id
  LEFT JOIN {{ source('relation', 'smt_prd_to_sbn_ids') }} AS rel_prd
    ON ad.product_id = rel_prd.product_id
){#

#} SELECT * FROM ad_master
