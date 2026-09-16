{{
  config(
    materialized = 'table',
    schema = 'xfm_ads'
  )
}}

WITH{#

#} campaign_group_type_mapping AS (
  {{ ebay_ads__campaign_group_type_mapping() }}
),{#

#} status_mapping AS (
  {{ ebay_ads__status_mapping() }}
),{#

#} campaign_master AS (
  SELECT
    -- Group attributes
      cmp.campaign_group_id
    , grp.campaign_group_name
    , campaign_group_type.label AS campaign_group_type
    -- Campaign attributes
    , cmp.campaign_id
    , cmp.campaign_name
    , (CASE
        WHEN COALESCE(campaign_group_status.seq, 0) > COALESCE(campaign_status.seq, 0)
          THEN campaign_group_status.label
        ELSE campaign_status.label
      END) AS campaign_status
    , cmp.daily_budget
    -- Sort key
    , (
        COALESCE(campaign_group_type.seq, 9)      * 10 * 10
        + COALESCE(campaign_group_status.seq, 9)  * 10
        + COALESCE(campaign_status.seq, 9)
      ) AS sort_key
  FROM {{ source('ebay_ads', 'campaign') }} AS cmp
  LEFT JOIN {{ source('ebay_ads', 'campaign_group') }} AS grp
    ON cmp.campaign_group_id = grp.campaign_group_id
  LEFT JOIN campaign_group_type_mapping AS campaign_group_type
    ON grp.campaign_group_type = campaign_group_type.code
  LEFT JOIN status_mapping AS campaign_group_status
    ON grp.campaign_group_status = campaign_group_status.code
  LEFT JOIN status_mapping AS campaign_status
    ON cmp.campaign_status = campaign_status.code
){#

#} SELECT * FROM campaign_master
