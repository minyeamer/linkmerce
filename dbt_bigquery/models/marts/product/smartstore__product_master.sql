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
    , CONCAT(
          FORMAT('%02d', COALESCE(chl.brand_seq, 99))
        , FORMAT('%02d', COALESCE(prd.delivery_type, 99))
        , CAST(COALESCE(product_status.seq, 9) AS STRING)
        , '-'
        , FORMAT(
              CONCAT('%0', CAST(LENGTH(CAST(
                MAX(prd.product_id) OVER () - MIN(prd.product_id) OVER () AS STRING)) AS STRING), 'd')
            , prd.product_id - MIN(prd.product_id) OVER ())
      ) AS product_seq
    , prd.product_name
    , chl.team_name
    , chl.brand_name
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
