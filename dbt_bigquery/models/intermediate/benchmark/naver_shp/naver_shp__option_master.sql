{{
  config(
    materialized = 'view',
    schema = 'xfm_benchmark'
  )
}}

WITH

-- Step 1: prepare naver product

naver_product AS (
  SELECT
      product_id
    , product_id AS option_id
    , mall_seq
    , COALESCE(category_id, category_id3) AS category_id
    , product_name
    , '-' AS option_name
    , sales_price
    , first_payment_date
    , last_payment_date
  FROM {{ source('ss_hcenter', 'product') }}
),

-- Step 2: prepare stock product and option

stock_product AS (
  SELECT
      product_id
    , mall_seq
    , category_id
    , product_name
    , '-' AS option_name
    , sales_price
    , DATE(first_timestamp) AS first_payment_date
    , DATE(last_timestamp) AS last_payment_date
  FROM {{ source('naver_shp', 'stock_product') }}
),

stock_option AS (
  SELECT
      prd.product_id
    , COALESCE(opt.option_id, prd.product_id) AS option_id
    , prd.mall_seq
    , prd.category_id
    , prd.product_name
    , ARRAY_TO_STRING(
        ARRAY(
          SELECT option_name
          FROM UNNEST([opt.option_name1, opt.option_name2, opt.option_name3]) AS option_name
          WHERE option_name IS NOT NULL
        ), ' / '
      ) AS option_name
    , prd.sales_price
    , prd.first_payment_date
    , prd.last_payment_date
  FROM stock_product AS prd
  LEFT JOIN {{ source('naver_shp', 'stock_option') }} AS opt
    ON prd.product_id = opt.product_id
),

-- Step 3: prepare category group relation with names

nsh_prd_to_grp_id AS (
  SELECT
      rel.product_id
    , grp.group_name1
    , grp.group_name2
  FROM {{ ref('relation__nsh_prd_to_grp_id') }} AS rel
  INNER JOIN (
    SELECT
        group_id
      , group_name1
      , group_name2
    FROM {{ source('ss_hcenter', 'category_group') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY group_id) = 1
  ) AS grp
    ON rel.group_id = grp.group_id
),

-- Step 3: merge product option and group relation

naver_option AS (
  SELECT
      opt.product_id
    , opt.option_id
    , opt.mall_seq
    , IF(mall.mall_url LIKE 'https://brand%', '브랜드스토어', '스마트스토어') AS mall_type
    , mall.mall_group
    , mall.mall_name
    , opt.category_id
    , cat.category_name
    , cat.category_name1
    , cat.category_name2
    , cat.category_name3
    , cat.category_name4
    , ARRAY_TO_STRING(
        ARRAY(
          SELECT category
          FROM UNNEST([
              cat.category_name1
            , cat.category_name2
            , cat.category_name3
            , cat.category_name4
          ]) AS category
          WHERE category IS NOT NULL
            AND category != ''
        ), '>'
      ) AS full_category_name
    , rel.group_name1
    , rel.group_name2
    , opt.product_name
    , opt.option_name
    , CONCAT(mall.mall_url, '/products/', opt.product_id) AS product_url
    , opt.sales_price
    , opt.first_payment_date
    , opt.last_payment_date
    , mall.first_payment_date AS first_mall_payment_date
  FROM (
    (SELECT * FROM naver_product)
    UNION ALL
    (SELECT * FROM stock_option)
  ) AS opt
  LEFT JOIN {{ source('ss_hcenter', 'mall') }} AS mall
    ON opt.mall_seq = mall.mall_seq
  LEFT JOIN {{ source('naver_shp', 'category') }} AS cat
    ON opt.category_id = cat.category_id
  LEFT JOIN nsh_prd_to_grp_id AS rel
    ON opt.product_id = rel.product_id
)

SELECT * FROM naver_option
