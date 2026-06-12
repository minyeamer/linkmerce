WITH

ad_id_to_sbn_ids AS (
  SELECT
      ad_id
    , ad_level
    , bundle_product_ids
  FROM {{ source('relation', 'ad_id_to_sbn_ids') }}
  WHERE platform_name = '메타'
),

objective_mapping AS (
  {{ meta_ads__objective_mapping() }}
),

effective_status_mapping AS (
  {{ meta_ads__effective_status_mapping() }}
),

ad_master AS (
  SELECT
      ad.account_id
    , acc.account_name
    -- Campaign attrs
    , ad.campaign_id
    , cmp.campaign_name
    , objective.label AS objective
    -- Adset attrs
    , ad.adset_id
    , adset.adset_name
    -- Ad attrs
    , ad.ad_id
    , CONCAT(
          COALESCE(IF(status_fin.code = 'DELETED', '1', '0'), '0')
        , COALESCE(FORMAT('%02d', acc.account_seq), '99')
        , COALESCE(FORMAT('%02d', objective.seq), '99')
      ) AS ad_seq
    , ad.ad_name
    , status_fin.label AS ad_status
    , COALESCE(
          rel_ad.bundle_product_ids
        , rel_adset.bundle_product_ids
        , rel_cmp.bundle_product_ids
        , '900000'
      ) AS bundle_product_ids
    , COALESCE(ad.created_at, adset.created_at, cmp.created_at) AS created_at
  FROM {{ source('meta_ads', 'ad') }} AS ad
  LEFT JOIN {{ source('meta_ads', 'account') }} AS acc
    ON ad.account_id = acc.account_id
  LEFT JOIN {{ source('meta_ads', 'campaign') }} AS cmp
    ON ad.account_id = cmp.account_id AND ad.campaign_id = cmp.campaign_id
  LEFT JOIN {{ source('meta_ads', 'adset') }} AS adset
    ON ad.account_id = adset.account_id AND ad.adset_id = adset.adset_id
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
  -- Resolve bundle_product_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 0) AS rel_cmp
    ON ad.campaign_id = rel_cmp.ad_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 1) AS rel_adset
    ON ad.adset_id = rel_adset.ad_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 2) AS rel_ad
    ON ad.ad_id = rel_ad.ad_id
)

SELECT * FROM ad_master
