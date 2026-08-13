{{
  config(
    materialized = 'tvf',
    meta = {
      'params': [
        {'name': 'DS_START_DATE', 'type': 'date'},
        {'name': 'DS_END_DATE', 'type': 'date'}
      ]
    },
    schema = 'xfm_benchmark'
  )
}}

WITH{#

-- Step 1: prepare naver product

#} naver_product AS (
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
  WHERE COALESCE(DS_START_DATE, (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul')::date)
    <= DATE '2026-02-26'
),{#

-- Step 2: prepare stock product and option

#} stock_product AS (
  SELECT
      product_id
    , product_id AS option_id
    , mall_seq
    , category_id
    , product_name
    , '-' AS option_name
    , sales_price
    , first_timestamp::date AS first_payment_date
    , last_timestamp::date AS last_payment_date
  FROM {{ source('naver_shp', 'stock_product') }}
  WHERE COALESCE(DS_END_DATE, (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul')::date)
    >= DATE '2026-03-10'
),{#

#} stock_option AS (
  SELECT
      prd.product_id
    , opt.option_id
    , prd.mall_seq
    , prd.category_id
    , prd.product_name
    , concat_ws(
          ' / '
        , opt.option_name1
        , opt.option_name2
        , opt.option_name3
      ) AS option_name
    , prd.sales_price
    , prd.first_payment_date
    , prd.last_payment_date
  FROM stock_product AS prd
  INNER JOIN {{ source('naver_shp', 'stock_option') }} AS opt
    ON prd.product_id = opt.product_id
  WHERE COALESCE(DS_END_DATE, (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul')::date)
    >= DATE '2026-03-10'
),{#

-- Step 3: prepare category group relation with names

#} nsh_prd_to_grp_id AS (
  SELECT
      rel.product_id
    , grp.group_id
    , grp.group_name1
    , grp.group_name2
  FROM {{ ref('relation__nsh_prd_to_grp_id') }} AS rel
  INNER JOIN (
    SELECT DISTINCT ON (group_id)
        group_id
      , group_name1
      , group_name2
    FROM {{ source('ss_hcenter', 'category_group') }}
    ORDER BY group_id
  ) AS grp
    ON rel.group_id = grp.group_id
),{#

-- Step 4: merge product option and group relation

#} naver_option AS (
  SELECT DISTINCT ON (opt.product_id, opt.option_id)
      opt.product_id
    , opt.option_id
    -- Mall attributes
    , opt.mall_seq
    , (CASE
        WHEN mall.mall_url LIKE 'https://brand%' THEN '브랜드스토어'
        ELSE '스마트스토어'
      END) AS mall_type
    , mall.mall_group
    , mall.mall_name
    , mall.mall_url
    -- Category attributes
    , opt.category_id
    , cat.category_name
    , cat.category_id1
    , cat.category_id2
    , cat.category_id3
    , cat.category_id4
    , cat.category_name1
    , cat.category_name2
    , cat.category_name3
    , cat.category_name4
    , cat.full_category_id
    , cat.full_category_name
    -- Group attributes
    , rel.group_id
    , rel.group_name1
    , rel.group_name2
    -- Product attributes
    , opt.product_name
    , opt.option_name
    , mall.mall_url || '/products/' || opt.product_id::text AS product_url
    , opt.sales_price
    , opt.first_payment_date
    , opt.last_payment_date
    , mall.first_payment_date AS first_mall_payment_date
  FROM (
    (SELECT * FROM naver_product)
    UNION ALL
    (SELECT * FROM stock_product)
    UNION ALL
    (SELECT * FROM stock_option)
  ) AS opt
  LEFT JOIN {{ source('ss_hcenter', 'mall') }} AS mall
    ON opt.mall_seq = mall.mall_seq
  LEFT JOIN {{ ref('naver_shp__category_master') }} AS cat
    ON opt.category_id = cat.category_id
  LEFT JOIN nsh_prd_to_grp_id AS rel
    ON opt.product_id = rel.product_id
  ORDER BY opt.product_id, opt.option_id, opt.last_payment_date DESC NULLS LAST
){#

#} SELECT * FROM naver_option
