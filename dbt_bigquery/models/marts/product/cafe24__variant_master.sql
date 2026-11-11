{{
  config(
    materialized = 'view',
    schema = 'cafe24',
    alias = 'variant_master'
  )
}}

WITH

variant_master AS (
  SELECT
      var.product_no
    , var.variant_code
    , var.mall_id
    , mall.mall_name
    , prd.product_name
    , CONCAT(
          var.option_group1, '=', var.option_name1
        , IF(var.option_group2 IS NOT NULL, CONCAT(', ', var.option_group2, '=', var.option_name2), '')
        , IF(var.option_group3 IS NOT NULL, CONCAT(', ', var.option_group3, '=', var.option_name3), '')
      ) AS option_name
    , brd.brand_name
    , prd.model_name
    , prd.custom_product_code
    , mall.mall_url
    , COALESCE(var.image_url, prd.image_url) AS image_url
    , (COALESCE(prd.display, FALSE) AND COALESCE(var.display, FALSE)) AS display
    , (COALESCE(prd.selling, FALSE) AND COALESCE(var.selling, FALSE)) AS selling
    , prd.sold_out
    , prd.price AS product_price
    , var.option_price
    , var.supply_price
    , var.display_order
    , opt.first_order_dt
    , opt.last_order_dt
    -- Sort key
    , (
        COALESCE(mall.mall_seq, 99) * 10
        + 3 - (
          IF(COALESCE(prd.display, FALSE) AND COALESCE(var.display, FALSE), 1, 0)
          + IF(COALESCE(prd.selling, FALSE) AND COALESCE(var.selling, FALSE), 1, 0)
          + IF(prd.sold_out, 0, 1)
        )
      ) AS sort_key
  FROM {{ source('cafe24', 'product_variant') }} AS var
  LEFT JOIN {{ source('cafe24', 'product') }} AS prd
    ON (var.mall_id = prd.mall_id) AND (var.product_no = prd.product_no)
  LEFT JOIN {{ source('cafe24', 'mall') }} AS mall
    ON var.mall_id = mall.mall_id
  LEFT JOIN {{ source('cafe24', 'order_option') }} AS opt
    ON (var.mall_id = opt.mall_id) AND (var.variant_code = opt.variant_code)
  LEFT JOIN {{ source('relation', 'c24_var_to_sbn_ids') }} AS rel
    ON (var.mall_id = rel.mall_id) AND (var.variant_code = rel.variant_code)
  LEFT JOIN {{ ref('core__product_master') }} AS brd
    ON LEFT(rel.bundle_product_ids, 6) = brd.product_id
)

SELECT * FROM variant_master
