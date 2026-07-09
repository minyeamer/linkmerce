{{
  config(
    materialized = 'table',
    schema = 'xfm_ads'
  )
}}

WITH

objective_mapping AS (
  {{ meta_ads__objective_mapping() }}
),

effective_status_mapping AS (
  {{ meta_ads__effective_status_mapping() }}
),

campaign_master AS (
  SELECT
      cmp.account_id
    , acc.account_name
    -- Campaign attributes
    , cmp.campaign_id
    , cmp.campaign_name
    , objective.label AS objective
    , effective_status.label AS effective_status
    , cmp.created_at
    -- Sort key
    , (
        IF(cmp.effective_status = 'DELETED', 2, 1)  * 100 * 100
        + COALESCE(acc.account_seq, 99)             * 100
        + COALESCE(objective.seq, 99)
      ) AS sort_key
  FROM {{ source('meta_ads', 'campaign') }} AS cmp
  LEFT JOIN {{ source('meta_ads', 'account') }} AS acc
    ON cmp.account_id = acc.account_id
  -- Map codes to labels
  LEFT JOIN objective_mapping AS objective
    ON cmp.objective = objective.code
  LEFT JOIN effective_status_mapping AS effective_status
    ON cmp.effective_status = effective_status.code
)

SELECT * FROM campaign_master
