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

default_product_seq AS (
  SELECT
      CONCAT(
          '99999-'
        , REPEAT('9', COALESCE(LENGTH(SPLIT(product_seq, '-')[SAFE_OFFSET(1)]), 1))
      ) AS product_seq
  FROM (
    SELECT MAX(product_seq) AS product_seq
    FROM {{ ref('smartstore__product_master') }}()
  ) AS t_
),

option_master AS (
  SELECT
      opt.product_id
    , opt.option_id
    , CONCAT(
          COALESCE(prd.product_seq, def.product_seq)
        , '-'
        , opt.product_type
        , '-'
        , FORMAT(
              CONCAT('%0', CAST(LENGTH(CAST(
                MAX(opt.option_id) OVER () - MIN(opt.option_id) OVER () AS STRING)) AS STRING), 'd')
            , opt.option_id - MIN(opt.option_id) OVER ())
      ) AS option_seq
    , chl.team_name
    , chl.brand_name
    , product_type.label AS product_type
    , opt.product_name
    , opt.option_name
    , opt.seller_product_code
    , opt.seller_option_code
    , opt.sales_price
    , opt.option_price
    , opt.update_date
  FROM {{ source('smartstore', 'order_option') }} AS opt
  LEFT JOIN {{ source('smartstore', 'channel') }} AS chl
    ON opt.channel_seq = chl.channel_seq
  LEFT JOIN {{ ref('smartstore__product_master') }}() AS prd
    ON opt.product_id = prd.product_id
  LEFT JOIN product_type_mapping AS product_type
    ON opt.product_type = product_type.code
  CROSS JOIN default_product_seq AS def
)

SELECT * FROM option_master
