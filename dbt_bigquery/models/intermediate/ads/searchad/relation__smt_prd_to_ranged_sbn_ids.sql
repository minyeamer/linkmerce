WITH original_relation AS (
  SELECT
      product_id AS product_id_shop
    , bundle_product_ids
  FROM {{ source('relation', 'smt_prd_to_sbn_ids') }}
),

default_ranged_relation AS (
  SELECT
      product_id_shop
    , bundle_product_ids
    , DATE('{{ var("ds_start_date") }}') AS start_date
    , DATE('{{ var("ds_end_date") }}') AS end_date
  FROM original_relation
  WHERE NOT EXISTS (
    SELECT 1
    FROM UNNEST(SPLIT(original_relation.bundle_product_ids, ',')) AS product_id
    WHERE product_id = '100088'
  )
),

rule1_pre_relation AS (
  SELECT
      product_id_shop
    , bundle_product_ids
    , DATE('{{ var("ds_start_date") }}') AS start_date
    , LEAST(DATE('{{ var("ds_end_date") }}'), DATE('2026-06-09')) AS end_date
  FROM original_relation
  WHERE EXISTS (
    SELECT 1
    FROM UNNEST(SPLIT(original_relation.bundle_product_ids, ',')) AS product_id
    WHERE product_id = '100088'
  )
    AND DATE('{{ var("ds_start_date") }}') <= DATE('2026-06-09')
),

rule1_post_relation AS (
  SELECT
      product_id_shop
    , (CASE
        WHEN bundle_product_ids = '100088'
          THEN '100081,100082,100083,100084,100085,100086,100087'
        ELSE NULLIF(
          COALESCE(
            ARRAY_TO_STRING(
              ARRAY(
                SELECT product_id
                FROM UNNEST(SPLIT(bundle_product_ids, ',')) AS product_id
                WHERE product_id != '100088'
              ),
              ','
            ),
            ''
          ),
          ''
        )
      END) AS bundle_product_ids
    , GREATEST(DATE('{{ var("ds_start_date") }}'), DATE('2026-06-10')) AS start_date
    , DATE('{{ var("ds_end_date") }}') AS end_date
  FROM original_relation
  WHERE EXISTS (
    SELECT 1
    FROM UNNEST(SPLIT(original_relation.bundle_product_ids, ',')) AS product_id
    WHERE product_id = '100088'
  )
    AND DATE('{{ var("ds_end_date") }}') >= DATE('2026-06-10')
)

SELECT
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
