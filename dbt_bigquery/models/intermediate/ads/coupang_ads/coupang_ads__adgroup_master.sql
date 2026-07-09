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

adgroup_master AS (
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
    , grp.adgroup_id
    , grp.adgroup_name
    , goal_type.label AS goal_type
    , grp.is_active
    , grp.is_deleted
    , grp.roas_target
    , grp.created_at
    , grp.updated_at
    -- Sort key
    , (
        IF(grp.is_deleted, 2, 1)        * 100 * 10
        + COALESCE(vdr.vendor_seq, 99)  * 10
        + COALESCE(goal_type.seq, 9)
      ) AS sort_key
  FROM {{ source('coupang_ads', 'adgroup') }} AS grp
  LEFT JOIN {{ source('coupang_ads', 'campaign') }} AS cmp
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
