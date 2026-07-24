{{
  config(
    materialized = 'view',
    schema = 'sabangnet',
    alias = 'option_master'
  )
}}

WITH{#

#} option_status_mapping AS (
  {{ sabangnet__option_status_mapping() }}
),{#

#} option_type_mapping AS (
  {{ sabangnet__option_type_mapping() }}
),{#

#} primary_option AS (
  SELECT DISTINCT ON (opt.option_id)
      opt.option_id
    , itm.item_id
    , ROW_NUMBER() OVER (
        PARTITION BY opt.option_id
        ORDER BY itm.item_seq ASC NULLS LAST
      ) AS option_rank
  FROM {{ source('sabangnet', 'option') }} AS opt
  LEFT JOIN LATERAL unnest(
    string_to_array(COALESCE(opt.bundle_option_ids, opt.option_id), ',')
  ) AS t(product) ON TRUE
  LEFT JOIN {{ ref('core__product_master') }} AS itm
    ON (string_to_array(product, '-'))[1] = itm.product_id
  ORDER BY opt.option_id, itm.item_seq ASC NULLS LAST
),{#

#} bundle_options AS (
  SELECT
      opt.option_id
    , string_agg(
        COALESCE(
          prd.product_name || ' x ' || COALESCE((string_to_array(product, ':'))[2], '1')
          , '상품코드 불일치'
        ), E'\n'
        ORDER BY offset_
      ) AS option_names
  FROM {{ source('sabangnet', 'option') }} AS opt
  CROSS JOIN LATERAL (
    SELECT value AS product, ordinality - 1 AS offset_
    FROM unnest(string_to_array(opt.bundle_option_ids, ','))
    WITH ORDINALITY AS t(value, ordinality)
  ) AS t_
  LEFT JOIN {{ ref('core__product_master') }} AS prd
    ON (string_to_array((string_to_array(product, ':'))[1], '-'))[1] = prd.product_id
  WHERE opt.bundle_option_ids IS NOT NULL
  GROUP BY opt.option_id
),{#

#} option_master AS (
  SELECT
      (string_to_array(opt.option_id, '-'))[1] AS product_id
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
    , (CASE WHEN prd.option_type = '대표' THEN '대표' ELSE option_type.label END) AS option_type
    , opt.option_quantity
    , opt.option_price
    , opt.register_dt
    -- Sort key
    , COALESCE(
          prd.sort_key
        , REPEAT('9', LENGTH((MAX(prd.sort_key) OVER ())::text))::bigint
      ) AS sort_key
  FROM {{ source('sabangnet', 'option') }} AS opt
  LEFT JOIN {{ ref('sabangnet__product_master') }} AS prd
    ON (string_to_array(opt.option_id, '-'))[1] = prd.product_id
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
){#

#} SELECT * FROM option_master
