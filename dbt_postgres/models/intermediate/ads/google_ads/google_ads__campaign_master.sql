{{
  config(
    materialized = 'table',
    schema = 'xfm_ads'
  )
}}

WITH{#

#} campaign_type_mapping AS (
  {{ google_ads__campaign_type_mapping() }}
),{#

#} bidding_strategy_mapping AS (
  {{ google_ads__bidding_strategy_mapping() }}
),{#

#} status_mapping AS (
  {{ google_ads__status_mapping() }}
),{#

#} campaign_master AS (
  SELECT
      cmp.customer_id
    , acc.account_name
    -- Campaign attributes
    , cmp.campaign_id
    , cmp.campaign_name
    , campaign_type.label AS campaign_type
    , campaign_status.label AS campaign_status
    , bidding_strategy.label AS bidding_strategy
    , cmp.campaign_budget
    , cmp.impression_count_30d
    , cmp.click_count_30d
    , cmp.ad_cost_30d
    , cmp.created_at
    -- Sort key
    , (
        (CASE WHEN cmp.campaign_status = 'REMOVED' THEN 2 ELSE 1 END) * 100 * 100
        + COALESCE(acc.account_seq, 99)                               * 100
        + COALESCE(campaign_type.seq, 99)
      ) AS sort_key
  FROM {{ source('google_ads', 'campaign') }} AS cmp
  LEFT JOIN {{ source('google_ads', 'account') }} AS acc
    ON cmp.customer_id = acc.customer_id
  -- Map codes to labels
  LEFT JOIN campaign_type_mapping AS campaign_type
    ON cmp.campaign_type = campaign_type.code
  LEFT JOIN bidding_strategy_mapping AS bidding_strategy
    ON cmp.bidding_strategy = bidding_strategy.code
  LEFT JOIN status_mapping AS campaign_status
    ON cmp.campaign_status = campaign_status.code
){#

#} SELECT * FROM campaign_master
