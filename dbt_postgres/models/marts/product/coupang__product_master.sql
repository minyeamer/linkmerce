{{
  config(
    materialized = 'view',
    schema = 'coupang',
    alias = 'product_master'
  )
}}

WITH{#

#} product_status_mapping AS (
  {{ coupang__product_status_mapping() }}
),{#

#} product_master AS (
  SELECT
      prd.vendor_inventory_id
    , prd.product_id
    , prd.vendor_id
    , vdr.vendor_name
    , prd.seller_product_name
    , prd.display_product_name
    , product_status.label AS product_status
    , prd.display_category_id AS category_id
    , cat.category_name1
    , cat.category_name2
    , cat.category_name3
    , cat.category_name4
    , cat.category_name5
    , cat.category_name6
    , prd.brand_name
    , prd.maker_name
    , prd.delivery_fee
    , prd.sales_started_at
    , prd.sales_ended_at
    , prd.created_at
    -- Sort key
    , (
        COALESCE(vdr.vendor_seq, 99) * 10
        + COALESCE(product_status.seq, 9)
      )::integer AS sort_key
  FROM {{ source('coupang', 'product') }} AS prd
  LEFT JOIN {{ source('coupang', 'vendor') }} AS vdr
    ON prd.vendor_id = vdr.vendor_id
  LEFT JOIN product_status_mapping AS product_status
    ON prd.product_status = product_status.code
  LEFT JOIN {{ source('coupang', 'category') }} AS cat
    ON prd.display_category_id = cat.category_id
){#

#} SELECT * FROM product_master
