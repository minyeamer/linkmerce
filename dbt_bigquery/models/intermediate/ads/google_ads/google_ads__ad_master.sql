WITH

ad_id_to_sbn_ids AS (
  SELECT
      ad_id
    , ad_level
    , bundle_product_ids
  FROM {{ source('relation', 'ad_id_to_sbn_ids') }}
  WHERE platform_name = '구글'
),

campaign_type_mapping AS (
  {{ google_ads__campaign_type_mapping() }}
),

bidding_strategy_mapping AS (
  {{ google_ads__bidding_strategy_mapping() }}
),

adgroup_type_mapping AS (
  {{ google_ads__adgroup_type_mapping() }}
),

ad_type_mapping AS (
  {{ google_ads__ad_type_mapping() }}
),

status_mapping AS (
  {{ google_ads__status_mapping() }}
),

ad_master AS (
  SELECT
      ad.customer_id
    , acc.account_name
    -- Campaign attrs
    , ad.campaign_id
    , cmp.campaign_name
    , campaign_type.label AS campaign_type
    , bidding_strategy.label AS bidding_strategy
    -- Adgroup attrs
    , ad.adgroup_id
    , grp.adgroup_name
    , adgroup_type.label AS adgroup_type
    -- Ad attrs
    , ad.ad_id
    , CONCAT(
          IF(status_fin.code = 'REMOVED', '1', '0')
        , COALESCE(FORMAT('%02d', acc.account_seq), '99')
        , COALESCE(FORMAT('%02d', campaign_type.seq), '99')
        , COALESCE(FORMAT('%02d', adgroup_type.seq), '99')
        , COALESCE(FORMAT('%02d', ad_type.seq), '99')
      ) AS ad_seq
    , ad.ad_name
    , ad_type.label AS ad_type
    , status_fin.label AS ad_status
    , COALESCE(
          rel_ad.bundle_product_ids
        , rel_grp.bundle_product_ids
        , rel_cmp.bundle_product_ids
        , '900000'
      ) AS bundle_product_ids
    , cmp.created_at
  FROM {{ source('google_ads', 'ad') }} AS ad
  LEFT JOIN {{ source('google_ads', 'account') }} AS acc
    ON ad.customer_id = acc.customer_id
  LEFT JOIN {{ source('google_ads', 'campaign') }} AS cmp
    ON ad.campaign_id = cmp.campaign_id
  LEFT JOIN {{ source('google_ads', 'adgroup') }} AS grp
    ON ad.adgroup_id = grp.adgroup_id
  -- Map codes to labels
  LEFT JOIN campaign_type_mapping AS campaign_type
    ON cmp.campaign_type = campaign_type.code
  LEFT JOIN bidding_strategy_mapping AS bidding_strategy
    ON cmp.bidding_strategy = bidding_strategy.code
  LEFT JOIN adgroup_type_mapping AS adgroup_type
    ON grp.adgroup_type = adgroup_type.code
  LEFT JOIN ad_type_mapping AS ad_type
    ON ad.ad_type = ad_type.code
  -- Resolve final status label
  LEFT JOIN status_mapping AS status_cmp
    ON cmp.campaign_status = status_cmp.code
  LEFT JOIN status_mapping AS status_grp
    ON grp.adgroup_status = status_grp.code
  LEFT JOIN status_mapping AS status_ad
    ON ad.ad_status = status_ad.code
  LEFT JOIN status_mapping AS status_fin
    ON GREATEST(status_cmp.seq, status_grp.seq, status_ad.seq) = status_fin.seq
  -- Resolve bundle_product_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 0) AS rel_cmp
    ON ad.campaign_id = rel_cmp.ad_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 1) AS rel_grp
    ON ad.adgroup_id = rel_grp.ad_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 2) AS rel_ad
    ON ad.ad_id = rel_ad.ad_id
)

SELECT * FROM ad_master
