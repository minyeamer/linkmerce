{{
  config(
    materialized = 'table',
    schema = 'xfm_ads'
  )
}}

WITH{#

#} vendor_type_mapping AS (
  {{ coupang_ads__vendor_type_mapping() }}
),{#

#} campaign_type_mapping AS (
  {{ coupang_ads__campaign_type_mapping() }}
),{#

#} goal_type_mapping AS (
  {{ coupang_ads__goal_type_mapping() }}
),{#

#} campaign_master AS (
  SELECT
      cmp.vendor_id
    , vdr.vendor_name
    , vdr.vendor_alias
    , vendor_type.label AS vendor_type
    -- Campaign attributes
    , cmp.campaign_id
    , cmp.campaign_name
    , COALESCE(campaign_type.label, cmp.campaign_type) AS campaign_type
    , goal_type.label AS goal_type
    , cmp.is_active
    , cmp.is_deleted
    , cmp.roas_target
    , cmp.created_at
    , cmp.updated_at
    -- Sort key
    , (
        (CASE WHEN cmp.is_deleted THEN 2 ELSE 1 END)  * 10 * 100
        + COALESCE(vdr.vendor_seq, 99)                * 10
        + COALESCE(goal_type.seq, 9)
      ) AS sort_key
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
){#

#} SELECT * FROM campaign_master
