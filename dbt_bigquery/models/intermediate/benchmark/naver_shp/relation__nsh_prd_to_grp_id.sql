{{
  config(
    materialized = 'table',
    schema = 'xfm_benchmark'
  )
}}

WITH

-- Step 1: prepare category data

naver_category AS (
  SELECT
      category_id
    , category_id1
    , category_id2
    , category_id3
    , category_id4
    , depth
  FROM {{ source('naver_shp', 'category') }}
),

category_group AS (
  SELECT
      mapping_seq
    , group_id
    , mapping_id
    , mapping_keyword
    , mapping_depth
  FROM {{ source('ss_hcenter', 'category_group') }}
),

-- Step 2: prepare product data

naver_product AS (
  SELECT
      prd.product_id
    , prd.product_name
    , cat.category_id
    , cat.category_id1
    , cat.category_id2
    , cat.category_id3
    , cat.category_id4
    , cat.depth AS category_depth
  FROM {{ source('ss_hcenter', 'product') }} AS prd
  INNER JOIN naver_category AS cat
    ON COALESCE(prd.category_id, prd.category_id3) = cat.category_id
),

stock_product AS (
  SELECT
      prd.product_id
    , prd.product_name
    , cat.category_id
    , cat.category_id1
    , cat.category_id2
    , cat.category_id3
    , cat.category_id4
    , cat.depth AS category_depth
  FROM {{ source('naver_shp', 'stock_product') }} AS prd
  INNER JOIN naver_category AS cat
    ON prd.category_id = cat.category_id
),

-- Step 3: match products to category groups across four levels

mapping_depth_4 AS (
  SELECT
      prd.product_id
    , prd.product_name
    , prd.category_id
    , prd.category_id1
    , prd.category_id2
    , prd.category_id3
    , prd.category_depth
    , grp.group_id
    , grp.mapping_seq
  FROM (
    (SELECT * FROM naver_product)
    UNION ALL
    (SELECT * FROM stock_product)
  ) AS prd
  LEFT JOIN (SELECT * FROM category_group WHERE mapping_depth = 4) AS grp
    ON (prd.category_depth = 4)
      AND (prd.category_id4 = grp.mapping_id)
      AND ((grp.mapping_keyword IS NULL)
        OR (STRPOS(LOWER(prd.product_name), LOWER(TRIM(grp.mapping_keyword))) > 0))
),

mapping_depth_3 AS (
  SELECT
      prd.product_id
    , prd.product_name
    , prd.category_id
    , prd.category_id1
    , prd.category_id2
    , prd.category_depth
    , COALESCE(prd.group_id, grp.group_id) AS group_id
    , COALESCE(prd.mapping_seq, grp.mapping_seq) AS mapping_seq
  FROM mapping_depth_4 AS prd
  LEFT JOIN (SELECT * FROM category_group WHERE mapping_depth = 3) AS grp
    ON (prd.group_id IS NULL)
      AND (prd.category_depth >= 3)
      AND (prd.category_id3 = grp.mapping_id)
      AND ((grp.mapping_keyword IS NULL)
        OR (STRPOS(LOWER(prd.product_name), LOWER(TRIM(grp.mapping_keyword))) > 0))
),

mapping_depth_2 AS (
  SELECT
      prd.product_id
    , prd.product_name
    , prd.category_id
    , prd.category_id1
    , COALESCE(prd.group_id, grp.group_id) AS group_id
    , COALESCE(prd.mapping_seq, grp.mapping_seq) AS mapping_seq
  FROM mapping_depth_3 AS prd
  LEFT JOIN (SELECT * FROM category_group WHERE mapping_depth = 2) AS grp
    ON (prd.group_id IS NULL)
      AND (prd.category_depth >= 2)
      AND (prd.category_id2 = grp.mapping_id)
      AND ((grp.mapping_keyword IS NULL)
        OR (STRPOS(LOWER(prd.product_name), LOWER(TRIM(grp.mapping_keyword))) > 0))
),

mapping_depth_1 AS (
  SELECT
      prd.product_id
    , prd.product_name
    , prd.category_id
    , COALESCE(prd.group_id, grp.group_id) AS group_id
    , COALESCE(prd.mapping_seq, grp.mapping_seq) AS mapping_seq
  FROM mapping_depth_2 AS prd
  LEFT JOIN (SELECT * FROM category_group WHERE mapping_depth = 1) AS grp
    ON (prd.group_id IS NULL)
      AND (prd.category_id1 = grp.mapping_id)
      AND ((grp.mapping_keyword IS NULL)
        OR (STRPOS(LOWER(prd.product_name), LOWER(TRIM(grp.mapping_keyword))) > 0))
)

-- Step 4: keep one category-group match per product

SELECT
    product_id
  , category_id
  , group_id
FROM mapping_depth_1
WHERE group_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY mapping_seq NULLS LAST) = 1
