{{
  config(
    materialized = 'view',
    schema = 'sabangnet',
    alias = 'option_master'
  )
}}

WITH

option_status_mapping AS (
  {{ sabangnet__option_status_mapping() }}
),

option_type_mapping AS (
  {{ sabangnet__option_type_mapping() }}
),

primary_option AS (
  SELECT
      opt.option_id
    , itm.item_id
  FROM {{ source('sabangnet', 'option') }} AS opt
  LEFT JOIN UNNEST(SPLIT(COALESCE(opt.bundle_option_ids, opt.option_id), ',')) AS product
  LEFT JOIN {{ ref('core__product_master') }} AS itm
    ON SPLIT(product, '-')[0] = itm.product_id
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY opt.option_id
    ORDER BY itm.item_seq ASC NULLS LAST
  ) = 1
),

bundle_options AS (
  SELECT
      opt.option_id
    , STRING_AGG(
        COALESCE(
          CONCAT(prd.product_name, ' x ', COALESCE(SPLIT(product, ':')[SAFE_OFFSET(1)], '1'))
          , '상품코드 불일치'
        ), '\n'
        ORDER BY offset_
      ) AS option_names
  FROM {{ source('sabangnet', 'option') }} AS opt
  CROSS JOIN UNNEST(SPLIT(opt.bundle_option_ids, ',')) AS product WITH OFFSET AS offset_
  LEFT JOIN {{ ref('core__product_master') }} AS prd
    ON SPLIT(SPLIT(product, ':')[SAFE_OFFSET(0)], '-')[SAFE_OFFSET(0)] = prd.product_id
  WHERE opt.bundle_option_ids IS NOT NULL
  GROUP BY opt.option_id
),

option_master AS (
  SELECT
      SPLIT(opt.option_id, '-')[0] AS product_id
    , opt.option_id
    , prd.model_code
    , prd.model_id
    , prd.product_name
    , prd.product_keyword
    , opt.option_group
    , opt.option_name
    , prd.brand_name
    , itm.category_name1
    , itm.category_name2
    , itm.category_name3
    , itm.category_name4
    , opt.bundle_option_ids
    , bundle.option_names AS bundle_option_names
    , option_status.label AS option_status
    , IF(prd.option_type = '대표', '대표', option_type.label) AS option_type
    , opt.option_quantity
    , opt.option_price
    , opt.register_dt
    -- Sort key
    , COALESCE(
          prd.sort_key
        , CAST(REPEAT('9', LENGTH(CAST(MAX(prd.sort_key) OVER () AS STRING))) AS INT64)
      ) AS sort_key
  FROM {{ source('sabangnet', 'option') }} AS opt
  LEFT JOIN {{ ref('sabangnet__product_master') }} AS prd
    ON SPLIT(opt.option_id, '-')[0] = prd.product_id
  LEFT JOIN primary_option AS main
    ON opt.option_id = main.option_id
  LEFT JOIN {{ source('core', 'item') }} AS itm
    ON main.item_id = itm.item_id
  LEFT JOIN bundle_options AS bundle
    ON opt.option_id = bundle.option_id
  LEFT JOIN option_status_mapping AS option_status
    ON opt.option_status = option_status.code
  LEFT JOIN option_type_mapping AS option_type
    ON opt.option_type = option_type.code
)

SELECT * FROM option_master
