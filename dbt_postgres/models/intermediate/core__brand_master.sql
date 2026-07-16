{{
  config(
    materialized = 'view',
    schema = 'core',
    alias = 'brand'
  )
}}

SELECT
    brand_id
  , item_id
  , item_seq
  , team_name
  , brand_name
  , brand_seq
FROM (
  SELECT
      product_id AS brand_id
    , item_id
    , item_seq
    , team_name
    , brand_name
    , ROW_NUMBER() OVER (ORDER BY team_name DESC NULLS LAST, product_id ASC) AS brand_seq
    , ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY item_seq) AS rn1
    , ROW_NUMBER() OVER (PARTITION BY brand_name ORDER BY item_seq) AS rn2
  FROM {{ source('core', 'item') }}
  WHERE starts_with(product_id, '2')
    AND NULLIF(brand_name, '브랜드 없음') IS NOT NULL
) AS t_
WHERE rn1 = 1 AND rn2 = 1
