{{
  config(
    materialized = 'view',
    schema = 'core',
    alias = 'brand'
  )
}}

SELECT
    product_id AS brand_id
  , item_id
  , item_seq
  , team_name
  , brand_name
  , ROW_NUMBER() OVER (ORDER BY team_name DESC NULLS LAST, product_id ASC) AS brand_seq
FROM {{ source('core', 'item') }}
WHERE STARTS_WITH(product_id, '2')
  AND brand_name IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY item_seq) = 1
