{{
  config(
    materialized = 'table',
    schema = 'xfm_ads'
  )
}}

WITH

vendor_type_mapping AS (
  {{ coupang_ads__vendor_type_mapping() }}
),

campaign_type_mapping AS (
  {{ coupang_ads__campaign_type_mapping() }}
),

goal_type_mapping AS (
  {{ coupang_ads__goal_type_mapping() }}
),

campaign_master AS (
  SELECT
      grp.vendor_id
    , vdr.vendor_name
    , vdr.vendor_alias
    , vendor_type.label AS vendor_type
    -- Campaign attributes
    , grp.campaign_id
    , cmp.campaign_name
    , COALESCE(campaign_type.label, cmp.campaign_type) AS campaign_type
    -- Adgroup attributes
    , adgroup.adgroup_id
    , adgroup.adgroup_name
    , CONCAT(
          IF(grp.is_deleted, '1', '0')
        , COALESCE(FORMAT('%02d', vdr.vendor_seq), '99')
        , COALESCE(FORMAT('%01d', goal_type.seq), '9')
      ) AS campaign_seq
    , goal_type.label AS goal_type
    , grp.is_active
    , grp.is_deleted
    , grp.roas_target
    , grp.created_at
    , grp.updated_at
  FROM {{ source('coupang_ads', 'adgroup') }} AS grp
  LEFT JOIN {{ ref('coupang_ads__campaign_master') }} AS cmp
    ON grp.campaign_id = cmp.campaign_id
  LEFT JOIN {{ source('coupang', 'vendor') }} AS vdr
    ON grp.vendor_id = vdr.vendor_id
  -- Map codes to labels
  LEFT JOIN vendor_type_mapping AS vendor_type
    ON cmp.vendor_type = vendor_type.code
  LEFT JOIN campaign_type_mapping AS campaign_type
    ON cmp.campaign_type = campaign_type.code
  LEFT JOIN goal_type_mapping AS goal_type
    ON grp.goal_type = goal_type.code
)

SELECT * FROM adgroup_master
