{{
  config(
    materialized = 'table',
    schema = 'xfm_ads'
  )
}}

WITH

campaign_group_type_mapping AS (
  {{ ebay_ads__campaign_group_type_mapping() }}
),

status_mapping AS (
  {{ ebay_ads__status_mapping() }}
),

group_master AS (
  SELECT
    -- Group attributes
      grp.campaign_group_id
    , grp.campaign_group_name
    , campaign_group_type.label AS campaign_group_type
    , campaign_group_status.label AS campaign_group_status
    -- Sort key
    , (
        COALESCE(campaign_group_type.seq, 9) * 10
        + COALESCE(campaign_group_status.seq, 9)
      ) AS sort_key
  FROM {{ source('ebay_ads', 'campaign_group') }} AS grp
  LEFT JOIN campaign_group_type_mapping AS campaign_group_type
    ON grp.campaign_group_type = campaign_group_type.code
  LEFT JOIN status_mapping AS campaign_group_status
    ON grp.campaign_group_status = campaign_group_status.code
)

SELECT * FROM group_master
