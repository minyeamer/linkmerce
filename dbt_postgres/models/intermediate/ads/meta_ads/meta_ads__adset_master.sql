{{
  config(
    materialized = 'table',
    schema = 'xfm_ads'
  )
}}

WITH{#

#} objective_mapping AS (
  {{ meta_ads__objective_mapping() }}
),{#

#} effective_status_mapping AS (
  {{ meta_ads__effective_status_mapping() }}
),{#

#} adset_master AS (
  SELECT
      adset.account_id
    , acc.account_name
    -- Campaign attributes
    , adset.campaign_id
    , cmp.campaign_name
    , objective.label AS objective
    -- Adset attributes
    , adset.adset_id
    , adset.adset_name
    , status_fin.label AS effective_status
    , adset.daily_budget
    , COALESCE(adset.created_at, cmp.created_at) AS created_at
    -- Sort key
    , (
        (CASE WHEN status_fin.code = 'DELETED' THEN 2 ELSE 1 END) * 100 * 100
        + COALESCE(acc.account_seq, 99)                           * 100
        + COALESCE(objective.seq, 99)
      ) AS sort_key
  FROM {{ source('meta_ads', 'adset') }} AS adset
  LEFT JOIN {{ source('meta_ads', 'account') }} AS acc
    ON adset.account_id = acc.account_id
  LEFT JOIN {{ source('meta_ads', 'campaign') }} AS cmp
    ON adset.account_id = cmp.account_id AND adset.campaign_id = cmp.campaign_id
  -- Map codes to labels
  LEFT JOIN objective_mapping AS objective
    ON cmp.objective = objective.code
  -- Resolve final status label
  LEFT JOIN effective_status_mapping AS status_cmp
    ON cmp.effective_status = status_cmp.code
  LEFT JOIN effective_status_mapping AS status_adset
    ON adset.effective_status = status_adset.code
  LEFT JOIN effective_status_mapping AS status_fin
    ON GREATEST(status_cmp.seq, status_adset.seq) = status_fin.seq
){#

#} SELECT * FROM adset_master
