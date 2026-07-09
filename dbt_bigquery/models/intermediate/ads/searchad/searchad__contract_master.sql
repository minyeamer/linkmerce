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

contract_type_mapping AS (
  {{ searchad__contract_type_mapping() }}
),

contract_master AS (
  SELECT
      sad.customer_id
    , acc.account_name
    , acc.account_type
    -- Campaign attributes
    , grp.campaign_id
    , cmp.campaign_name
    , campaign_type.label AS campaign_type
    -- Adgroup attributes
    , sad.adgroup_id
    , grp.adgroup_name
    , adgroup_type.label AS adgroup_type
    -- Ad attributes
    , sad.contract_id
    , sad.contract_name
    , contract_type.label AS contract_type
    , sad.contract_amount
    , sad.contract_start_date
    , sad.contract_end_date
    , sad.exposure_start_date
    , sad.exposure_end_date
    , sad.cancel_date
    -- Sort key
    , (
        IF(sad.cancel_date IS NULL, 1, 2) * 10 * 100 * 100 * 100
        + COALESCE(acc.account_seq, 99)   * 10 * 100 * 100
        + COALESCE(campaign_type.seq, 99) * 10 * 100
        + COALESCE(adgroup_type.seq, 99)  * 10
        + COALESCE(contract_type.seq, 9)
      ) AS sort_key
  FROM {{ source('searchad', 'contract') }} AS sad
  LEFT JOIN {{ source('searchad', 'account') }} AS acc
    ON sad.customer_id = acc.customer_id
  LEFT JOIN {{ source('searchad', 'adgroup') }} AS grp
    ON sad.adgroup_id = grp.adgroup_id
  LEFT JOIN {{ source('searchad', 'campaign') }} AS cmp
    ON grp.campaign_id = cmp.campaign_id
  -- Map codes to labels
  LEFT JOIN campaign_type_mapping AS campaign_type
    ON cmp.campaign_type = campaign_type.code
  LEFT JOIN adgroup_type_mapping AS adgroup_type
    ON grp.adgroup_type = adgroup_type.code
  LEFT JOIN contract_type_mapping AS contract_type
    ON sad.contract_type = contract_type.code
)

SELECT * FROM contract_master
