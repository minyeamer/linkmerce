{{
  config(
    materialized = 'view',
    schema = 'coupang',
    alias = 'item_master'
  )
}}

WITH

product_master AS (
  SELECT
      *
    , ARRAY(
        SELECT name
        FROM UNNEST([
            category_name1, category_name2, category_name3
          , category_name4, category_name5, category_name6
        ]) AS name WITH OFFSET AS idx
        WHERE name IS NOT NULL
        ORDER BY idx DESC
        LIMIT 2
      ) AS category_names
  FROM {{ ref('coupang__product_master') }}
),

item_master AS (
  SELECT
      itm.vendor_inventory_id
    , itm.vendor_inventory_item_id
    , itm.product_id
    , itm.option_id
    , itm.item_id
    , itm.vendor_id
    , prd.vendor_name
    , prd.seller_product_name
    , prd.display_product_name
    , itm.option_name
    , prd.product_status
    , prd.category_names[SAFE_OFFSET(1)] AS parent_category_name
    , prd.category_names[SAFE_OFFSET(0)] AS child_category_name
    , COALESCE(brd.brand_name, prd.brand_name) AS brand_name
    , prd.maker_name
    , CONCAT('https://image1.coupangcdn.com/image/', itm.image_url) AS image_url
    , itm.price
    , itm.sales_price
    , prd.delivery_fee
    , prd.created_at
    , opt.first_order_dt
    , opt.last_order_dt
    -- Sort key
    , COALESCE(
          prd.sort_key
        , CAST(REPEAT('9', LENGTH(CAST(MAX(prd.sort_key) OVER () AS STRING))) AS INT64)
      ) AS sort_key
  FROM {{ source('coupang', 'item') }} AS itm
  LEFT JOIN product_master AS prd
    ON itm.vendor_inventory_id = prd.vendor_inventory_id
  LEFT JOIN {{ source('coupang', 'order_option') }} AS opt
    ON itm.option_id = opt.option_id
  LEFT JOIN {{ source('relation', 'cpg_opt_to_sbn_ids') }} AS rel
    ON itm.option_id = rel.option_id
  LEFT JOIN {{ ref('core__product_master') }} AS brd
    ON LEFT(rel.bundle_product_ids, 6) = brd.product_id
)

SELECT * FROM item_master
