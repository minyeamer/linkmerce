{{ config(materialized = 'table') }}

WITH

campaign_type_mapping AS (
  {{ searchad__campaign_type_mapping() }}
),

campaign_master AS (
  SELECT
      cmp.customer_id
    , acc.account_name
    , acc.account_type
    -- Campaign attrs
    , cmp.campaign_id
    , CONCAT(
          IF(cmp.is_deleted, '2', '1')
        , COALESCE(FORMAT('%02d', acc.account_seq), '99')
        , COALESCE(FORMAT('%02d', campaign_type.seq), '99')
      ) AS campaign_seq
    , cmp.campaign_name
    , campaign_type.label AS campaign_type
    , cmp.is_enabled
    , cmp.is_deleted
    , cmp.created_at
    , cmp.deleted_at
  FROM {{ source('searchad', 'campaign') }} AS cmp
  LEFT JOIN {{ source('searchad', 'account') }} AS acc
    ON cmp.customer_id = acc.customer_id
  -- Map codes to labels
  LEFT JOIN campaign_type_mapping AS campaign_type
    ON cmp.campaign_type = campaign_type.code
)

SELECT * FROM campaign_master
