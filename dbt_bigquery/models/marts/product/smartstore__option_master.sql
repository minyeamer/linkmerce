{{
  config(
    materialized = 'tvf',
    schema = 'smartstore',
    alias = 'option_master'
  )
}}

WITH

product_type_mapping AS (
  {{ smartstore__product_type_mapping() }}
),

option_master AS (
  SELECT
      opt.product_id
    , opt.option_id
    , chl.team_name
    , chl.brand_name
    , opt.product_name
    , opt.option_name
    , product_type.label AS product_type
    , opt.seller_product_code
    , opt.seller_option_code
    , opt.sales_price
    , opt.option_price
    , opt.update_date
    -- Sort key
    , COALESCE(
          prd.sort_key
        , CAST(REPEAT('9', LENGTH(CAST(MAX(prd.sort_key) OVER () AS STRING))) AS INT64)
      ) AS sort_key1
    , opt.product_type AS sort_key2
  FROM {{ source('smartstore', 'order_option') }} AS opt
  LEFT JOIN {{ source('smartstore', 'channel') }} AS chl
    ON opt.channel_seq = chl.channel_seq
  LEFT JOIN {{ ref('smartstore__product_master') }}() AS prd
    ON opt.product_id = prd.product_id
  LEFT JOIN product_type_mapping AS product_type
    ON opt.product_type = product_type.code
)

SELECT * FROM option_master
