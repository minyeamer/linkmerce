{{
  config(
    materialized = 'view',
    schema = 'xfm_benchmark'
  )
}}

SELECT
    category_id
  , category_name
  , category_id1
  , category_id2
  , category_id3
  , category_id4
  , category_name1
  , category_name2
  , category_name3
  , category_name4
  , concat_ws(
        '>'
      , NULLIF(category_id1::text, '')
      , NULLIF(category_id2::text, '')
      , NULLIF(category_id3::text, '')
      , NULLIF(category_id4::text, '')
    ) AS full_category_id
  , concat_ws(
        '>'
      , NULLIF(category_name1, '')
      , NULLIF(category_name2, '')
      , NULLIF(category_name3, '')
      , NULLIF(category_name4, '')
    ) AS full_category_name
  , depth
FROM {{ source('naver_shp', 'category') }}
