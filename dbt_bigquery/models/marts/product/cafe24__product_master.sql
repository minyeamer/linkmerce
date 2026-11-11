{{
  config(
    materialized = 'view',
    schema = 'cafe24',
    alias = 'product_master'
  )
}}

WITH

product_master AS (
  SELECT
      prd.product_no
    , prd.product_code
    , prd.mall_id
    , mall.mall_name
    , prd.product_name
    , prd.model_name
    , prd.custom_product_code
    , mall.mall_url
    , prd.image_url
    , prd.display
    , prd.selling
    , prd.sold_out
    , prd.price
    , prd.retail_price
    , prd.supply_price
    , prd.register_dt
    , prd.modify_dt
    -- Sort key
    , (
        COALESCE(mall.mall_seq, 99) * 10
        + 3 - (IF(prd.display, 1, 0) + IF(prd.display, 1, 0) + IF(prd.sold_out, 0, 1))
      ) AS sort_key
  FROM {{ source('cafe24', 'product') }} AS prd
  LEFT JOIN {{ source('cafe24', 'mall') }} AS mall
    ON prd.mall_id = mall.mall_id
)

SELECT * FROM product_master
