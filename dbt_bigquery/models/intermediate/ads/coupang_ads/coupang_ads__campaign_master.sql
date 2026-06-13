{{ config(materialized = 'table') }}

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
      cmp.vendor_id
    , vdr.vendor_name
    , vdr.vendor_alias
    , vendor_type.label AS vendor_type
    -- Campaign attributes
    , cmp.campaign_id
    , CONCAT(
          IF(cmp.is_deleted, '1', '0')
        , COALESCE(FORMAT('%02d', vdr.vendor_seq), '99')
        , COALESCE(FORMAT('%01d', goal_type.seq), '9')
      ) AS campaign_seq
    , cmp.campaign_name
    , COALESCE(campaign_type.label, cmp.campaign_type) AS campaign_type
    , goal_type.label AS goal_type
    , cmp.is_active
    , cmp.is_deleted
    , cmp.roas_target
    , cmp.created_at
    , cmp.updated_at
  FROM {{ source('coupang_ads', 'campaign') }} AS cmp
  LEFT JOIN {{ source('coupang', 'vendor') }} AS vdr
    ON cmp.vendor_id = vdr.vendor_id
  -- Map codes to labels
  LEFT JOIN vendor_type_mapping AS vendor_type
    ON cmp.vendor_type = vendor_type.code
  LEFT JOIN campaign_type_mapping AS campaign_type
    ON cmp.campaign_type = campaign_type.code
  LEFT JOIN goal_type_mapping AS goal_type
    ON cmp.goal_type = goal_type.code
)

SELECT * FROM campaign_master
