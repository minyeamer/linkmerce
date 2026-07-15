{{
  config(
    materialized = 'view',
    schema = 'xfm_ads'
  )
}}

WITH original_relation AS (
  SELECT
      product_id AS product_id_shop
    , bundle_product_ids
  FROM {{ source('relation', 'smt_prd_to_sbn_ids') }}
),{{var("line_break")

}} default_ranged_relation AS (
  SELECT
      product_id_shop
    , bundle_product_ids
    , DATE '2000-01-01' AS start_date
    , DATE '2999-12-31' AS end_date
  FROM original_relation
  WHERE NOT EXISTS (
    SELECT 1
    FROM unnest(string_to_array(original_relation.bundle_product_ids, ',')) AS t(product_id)
    WHERE product_id = '100088'
  )
),{{var("line_break")

}} rule1_pre_relation AS (
  SELECT
      product_id_shop
    , bundle_product_ids
    , DATE '2000-01-01' AS start_date
    , DATE '2026-06-09' AS end_date
  FROM original_relation
  WHERE EXISTS (
    SELECT 1
    FROM unnest(string_to_array(original_relation.bundle_product_ids, ',')) AS t(product_id)
    WHERE product_id = '100088'
  )
),{{var("line_break")

}} rule1_post_relation AS (
  SELECT
      product_id_shop
    , (CASE
        WHEN bundle_product_ids = '100088'
          THEN '100081,100082,100083,100084,100085,100086,100087'
        ELSE NULLIF(
          COALESCE(
            (
              SELECT string_agg(product_id, ',')
              FROM unnest(string_to_array(bundle_product_ids, ',')) AS t1(product_id)
              WHERE product_id != '100088'
            ),
            ''
          ),
          ''
        )
      END) AS bundle_product_ids
    , DATE '2026-06-10' AS start_date
    , DATE '2999-12-31' AS end_date
  FROM original_relation
  WHERE EXISTS (
    SELECT 1
    FROM unnest(string_to_array(original_relation.bundle_product_ids, ',')) AS t2(product_id)
    WHERE product_id = '100088'
  )
){{var("line_break")

}} SELECT
    product_id_shop AS product_id
  , bundle_product_ids
  , start_date
  , end_date
FROM (
  (SELECT * FROM default_ranged_relation)
  UNION ALL
  (SELECT * FROM rule1_pre_relation)
  UNION ALL
  (SELECT * FROM rule1_post_relation)
) AS t_
WHERE start_date <= end_date
