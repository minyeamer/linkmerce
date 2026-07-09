{{
  config(
    materialized = 'tvf',
    schema = 'smartstore',
    alias = 'product_master'
  )
}}

WITH

product_status_mapping AS (
  {{ smartstore__product_status_mapping() }}
),

display_type_mapping AS (
  {{ smartstore__display_type_mapping() }}
),

delivery_type_mapping AS (
  {{ smartstore__delivery_type_mapping() }}
),

product_master AS (
  SELECT
      prd.product_id
    , prd.product_no
    , prd.catalog_id
    , chl.team_name
    , chl.brand_name
    , prd.product_name
    , product_status.label AS status_type
    , display_type.label AS display_type
    , delivery_type.label AS delivery_type
    , prd.category_id
    , cat.category_name1
    , cat.category_name2
    , cat.category_name3
    , cat.category_name4
    , prd.tags
    , prd.price
    , prd.sales_price
    , prd.delivery_fee
    , prd.register_dt
    , prd.modify_dt
    -- Sort key
    , (
        COALESCE(chl.brand_seq, 99)       * 10 * 100
        + COALESCE(prd.delivery_type, 99) * 10
        + COALESCE(product_status.seq, 9)
      ) AS sort_key
  FROM {{ source('smartstore', 'product') }} AS prd
  LEFT JOIN {{ source('smartstore', 'channel') }} AS chl
    ON prd.channel_seq = chl.channel_seq
  LEFT JOIN product_status_mapping AS product_status
    ON prd.status_type = product_status.code
  LEFT JOIN display_type_mapping AS display_type
    ON prd.display_type = display_type.code
  LEFT JOIN delivery_type_mapping AS delivery_type
    ON prd.delivery_type = delivery_type.code
  LEFT JOIN {{ source('naver_shp', 'category') }} AS cat
    ON prd.category_id = cat.category_id
)

SELECT * FROM product_master
