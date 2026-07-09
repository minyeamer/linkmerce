{{
  config(
    materialized = 'tvf',
    schema = 'sabangnet',
    alias = 'product_master'
  )
}}

WITH

product_status_mapping AS (
  {{ sabangnet__product_status_mapping() }}
),

option_type_mapping AS (
  {{ sabangnet__option_type_mapping() }}
),

brand_order AS (
  SELECT
      brand_name
    , MIN(item_seq) AS item_seq
  FROM {{ ref('core__brand_master') }}
  GROUP BY brand_name
),

primary_option AS (
  SELECT
      product_id
    , item_id
    , item_seq
    , option_type
    , option_count
    , option_quantity
  FROM (
    SELECT
        SPLIT(opt.option_id, '-')[0] AS product_id
      , itm.item_id
      , itm.item_seq
      , opt.option_type
      , COUNT(*) OVER (PARTITION BY SPLIT(opt.option_id, '-')[0]) AS option_count
      , COALESCE(SAFE_CAST(SPLIT(product, ':')[SAFE_OFFSET(1)] AS INT64), 1) AS option_quantity
    FROM {{ source('sabangnet', 'option') }} AS opt
    LEFT JOIN UNNEST(SPLIT(COALESCE(opt.bundle_option_ids, opt.option_id), ',')) AS product
    LEFT JOIN {{ ref('core__product_master') }} AS itm
      ON SPLIT(product, '-')[0] = itm.product_id
  ) AS t_
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY product_id
    ORDER BY item_seq ASC NULLS LAST, option_quantity DESC
  ) = 1
),

max_quantity AS (
  SELECT POWER(10, MAX(LENGTH(CAST(option_quantity AS STRING)))) AS scale
  FROM primary_option
),

product_master AS (
  SELECT
      prd.product_id
    , prd.model_code
    , prd.model_id
    , prd.product_name
    , prd.product_keyword
    , prd.brand_name
    , itm.category_name1
    , itm.category_name2
    , itm.category_name3
    , itm.category_name4
    , product_status.label AS product_status
    , (CASE
        WHEN main.product_id IS NOT NULL THEN '대표'
        ELSE option_type.label
      END) AS option_type
    , opt.option_count
    , prd.manufacture_year
    , prd.sales_price
    , prd.org_price
    , (CASE
        WHEN prd.image_file IS NOT NULL
          THEN CONCAT("https://pic.sabangnet.co.kr/product_image/mw115815/100/", prd.image_file)
        ELSE NULL
      END) AS image_url
    , prd.register_dt
    , prd.modify_dt
    -- Sort key
    , (
      COALESCE(opt.item_seq, COALESCE(brd.item_seq, 99000000) + 999999)  * quantity.scale * 10
      + IF(main.product_id IS NOT NULL, 0, COALESCE(opt.option_type, 9)) * quantity.scale
      + option_quantity
    ) AS sort_key
  FROM {{ source('sabangnet', 'product') }} AS prd
  LEFT JOIN primary_option AS opt
    ON prd.product_id = opt.product_id
  LEFT JOIN {{ source('core', 'item') }} AS itm
    ON opt.item_id = itm.item_id
  LEFT JOIN {{ ref('core__product_master') }} AS main
    ON prd.product_id = main.product_id
  LEFT JOIN product_status_mapping AS product_status
    ON prd.product_status = product_status.code
  LEFT JOIN option_type_mapping AS option_type
    ON opt.option_type = option_type.code
  LEFT JOIN brand_order AS brd
    ON prd.brand_name = brd.brand_name
  CROSS JOIN max_quantity AS quantity
)

SELECT * FROM product_master
