{{
  config(
    materialized = 'table',
    schema = 'xfm_ads'
  )
}}

WITH{{var("line_break")

}} campaign_type_mapping AS (
  {{ google_ads__campaign_type_mapping() }}
),{{var("line_break")

}} bidding_strategy_mapping AS (
  {{ google_ads__bidding_strategy_mapping() }}
),{{var("line_break")

}} adgroup_type_mapping AS (
  {{ google_ads__adgroup_type_mapping() }}
),{{var("line_break")

}} status_mapping AS (
  {{ google_ads__status_mapping() }}
),{{var("line_break")

}} adgroup_master AS (
  SELECT
      grp.customer_id
    , acc.account_name
    -- Campaign attributes
    , grp.campaign_id
    , cmp.campaign_name
    , campaign_type.label AS campaign_type
    , bidding_strategy.label AS bidding_strategy
    -- Adgroup attributes
    , grp.adgroup_id
    , grp.adgroup_name
    , adgroup_type.label AS adgroup_type
    , status_fin.label AS adgroup_status
    , grp.target_cpa
    , grp.impression_count_30d
    , grp.click_count_30d
    , grp.ad_cost_30d
    , cmp.created_at
    -- Sort key
    , (
        (CASE WHEN status_fin.code = 'REMOVED' THEN 2 ELSE 1 END) * 100 * 100 * 100
        + COALESCE(acc.account_seq, 99)                           * 100 * 100
        + COALESCE(campaign_type.seq, 99)                         * 100
        + COALESCE(adgroup_type.seq, 99)
      ) AS sort_key
  FROM {{ source('google_ads', 'adgroup') }} AS grp
  LEFT JOIN {{ source('google_ads', 'account') }} AS acc
    ON grp.customer_id = acc.customer_id
  LEFT JOIN {{ source('google_ads', 'campaign') }} AS cmp
    ON grp.campaign_id = cmp.campaign_id
  -- Map codes to labels
  LEFT JOIN campaign_type_mapping AS campaign_type
    ON cmp.campaign_type = campaign_type.code
  LEFT JOIN bidding_strategy_mapping AS bidding_strategy
    ON cmp.bidding_strategy = bidding_strategy.code
  LEFT JOIN adgroup_type_mapping AS adgroup_type
    ON grp.adgroup_type = adgroup_type.code
  -- Resolve final status label
  LEFT JOIN status_mapping AS status_cmp
    ON cmp.campaign_status = status_cmp.code
  LEFT JOIN status_mapping AS status_grp
    ON grp.adgroup_status = status_grp.code
  LEFT JOIN status_mapping AS status_fin
    ON GREATEST(status_cmp.seq, status_grp.seq) = status_fin.seq
){{var("line_break")

}} SELECT * FROM adgroup_master
