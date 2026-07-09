{{
  config(
    materialized = 'table',
    schema = 'xfm_ads'
  )
}}

WITH

campaign_type_mapping AS (
  {{ searchad__campaign_type_mapping() }}
),

adgroup_type_mapping AS (
  {{ searchad__adgroup_type_mapping() }}
),

adgroup_master AS (
  SELECT
      grp.customer_id
    , acc.account_name
    , acc.account_type
    -- Campaign attributes
    , grp.campaign_id
    , cmp.campaign_name
    , campaign_type.label AS campaign_type
    -- Adgroup attributes
    , grp.adgroup_id
    , grp.adgroup_name
    , adgroup_type.label AS adgroup_type
    , grp.bid_amount
    , COALESCE(cmp.is_enabled AND grp.is_enabled, grp.is_enabled) AS is_enabled
    , grp.is_deleted
    , grp.created_at
    , grp.deleted_at
    -- Sort key
    , (
        IF(grp.is_deleted, 2, 1)          * 100 * 100 * 100
        + COALESCE(acc.account_seq, 99)   * 100 * 100
        + COALESCE(campaign_type.seq, 99) * 100
        + COALESCE(adgroup_type.seq, 99)
      ) AS sort_key
  FROM {{ source('searchad', 'adgroup') }} AS grp
  LEFT JOIN {{ source('searchad', 'account') }} AS acc
    ON grp.customer_id = acc.customer_id
  LEFT JOIN {{ source('searchad', 'campaign') }} AS cmp
    ON grp.campaign_id = cmp.campaign_id
  -- Map codes to labels
  LEFT JOIN campaign_type_mapping AS campaign_type
    ON cmp.campaign_type = campaign_type.code
  LEFT JOIN adgroup_type_mapping AS adgroup_type
    ON grp.adgroup_type = adgroup_type.code
)

SELECT * FROM adgroup_master
