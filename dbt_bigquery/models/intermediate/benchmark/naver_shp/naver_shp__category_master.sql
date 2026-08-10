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
  , ARRAY_TO_STRING(
      ARRAY(
        SELECT id
        FROM UNNEST([
            CAST(category_id1 AS STRING)
          , CAST(category_id2 AS STRING)
          , CAST(category_id3 AS STRING)
          , CAST(category_id4 AS STRING)
        ]) AS id
        WHERE id IS NOT NULL
          AND id != ''
      ), '>'
    ) AS full_category_id
  , ARRAY_TO_STRING(
      ARRAY(
        SELECT name
        FROM UNNEST([
            category_name1
          , category_name2
          , category_name3
          , category_name4
        ]) AS name
        WHERE name IS NOT NULL
          AND name != ''
      ), '>'
    ) AS full_category_name
  , depth
FROM {{ source('naver_shp', 'category') }}
