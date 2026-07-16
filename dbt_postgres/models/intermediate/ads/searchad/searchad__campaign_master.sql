{{
  config(
    materialized = 'table',
    schema = 'xfm_ads'
  )
}}

WITH{#

#} campaign_type_mapping AS (
  {{ searchad__campaign_type_mapping() }}
),{#

#} campaign_ad_type_mapping AS (
  {{ searchad__campaign_ad_type_mapping() }}
),{#

#} campaign_master AS (
  SELECT
      cmp.customer_id
    , acc.account_name
    , acc.account_type
    -- Campaign attributes
    , cmp.campaign_id
    , cmp.campaign_name
    , campaign_type.label AS campaign_type
    , campaign_ad_type.label AS ad_type
    , cmp.is_enabled
    , cmp.is_deleted
    , cmp.created_at
    , cmp.deleted_at
    -- Sort key
    , (
        (CASE WHEN cmp.is_deleted THEN 2 ELSE 1 END)  * 100 * 100
        + COALESCE(acc.account_seq, 99)               * 100
        + COALESCE(campaign_type.seq, 99)
      ) AS sort_key
  FROM {{ source('searchad', 'campaign') }} AS cmp
  LEFT JOIN {{ source('searchad', 'account') }} AS acc
    ON cmp.customer_id = acc.customer_id
  -- Map codes to labels
  LEFT JOIN campaign_type_mapping AS campaign_type
    ON cmp.campaign_type = campaign_type.code
  LEFT JOIN campaign_ad_type_mapping AS campaign_ad_type
    ON cmp.campaign_type = campaign_ad_type.code
){#

#} SELECT * FROM campaign_master
