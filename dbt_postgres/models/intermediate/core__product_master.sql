{{
  config(
    materialized = 'view',
    schema = 'core',
    alias = 'product'
  )
}}

SELECT DISTINCT ON (product_id)
    product_id
  , item_id
  , item_seq
  , team_name
  , brand_name
  , category_name1
  , category_name2
  , category_name3
  , category_name4
  , color
  , product_name
  , unit_name
  , unit_scale
  , ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY item_seq ASC NULLS LAST) AS rn
FROM {{ source('core', 'item') }}
WHERE product_id IS NOT NULL
ORDER BY product_id, item_seq ASC NULLS LAST
