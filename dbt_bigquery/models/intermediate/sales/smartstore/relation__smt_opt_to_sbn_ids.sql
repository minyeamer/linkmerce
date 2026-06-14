{{ config(materialized = 'table') }}

WITH

smt_opt_to_sbn_ids AS (
  SELECT
      option_id
    , bundle_product_ids
  FROM {{ source('relation', 'smt_opt_to_sbn_ids') }}
),

option_base AS (
  SELECT
      opt.product_id
    , opt.option_id
    , opt.product_type
    , opt.product_name
    , opt.option_name
  FROM {{ source('smartstore', 'order_option') }} AS opt
  LEFT JOIN smt_opt_to_sbn_ids AS rel
    ON opt.option_id = rel.option_id
  WHERE rel.option_id IS NULL
),

option_type_0 AS (
  SELECT
      opt.option_id AS option_id_shop
    , map.product_id AS product_id_sbn
  FROM option_base AS opt
  INNER JOIN {{ source('sabangnet', 'mapping_id') }} AS map
    ON (map.shop_id = 'shop0055') AND (CAST(opt.product_id AS STRING) = map.product_id_shop)
  WHERE opt.product_type = 0
),

option_type_1 AS (
  SELECT
      opt.option_id AS option_id_shop
    , SPLIT(map.option_id, '-')[SAFE_OFFSET(0)] AS product_id_sbn
  FROM option_base AS opt
  INNER JOIN {{ source('sabangnet', 'mapping_name') }} AS map
    ON (map.shop_id = 'shop0055')
      AND (CAST(opt.product_id AS STRING) = map.product_id_shop)
      AND (CASE
        WHEN map.sku_name LIKE '%$수량$%'
        THEN REGEXP_CONTAINS(
            opt.option_name
          , REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                        REPLACE(map.sku_name, '$수량$', '__수량__')
                      , r'([\*\+\?\$\^\.\[\]\{\}\(\)\|])', r'\\\1'
                    ) , '^__수량__', r'[^/]*'
                  ) , '__수량__$', r'[^/]*'
              ) , '__수량__', r'[^/]* / [^/]*'
            )
          )
        ELSE opt.option_name = map.sku_name END)
  WHERE opt.product_type = 1
),

option_type_2 AS (
  SELECT
      opt.option_id AS option_id_shop
    , SPLIT(prd.option_id, '-')[SAFE_OFFSET(0)] AS product_id_sbn
  FROM (
    SELECT
        option_id
      , REGEXP_EXTRACT(product_name, r'^\[([^\]]+)\]') AS group_id
      , TRIM(REGEXP_REPLACE(product_name, r'^\[[^\]]+\]', '')) AS product_name
      , option_name
    FROM option_base
    WHERE product_type = 2
  ) AS opt
  INNER JOIN {{ source('sabangnet', 'add_product') }} AS prd
    ON ((opt.group_id = prd.group_id) AND (opt.product_name = prd.option_name)
    OR (REPLACE(opt.option_name, ' ', '') = CONCAT(REPLACE(prd.group_name, ' ', ''), ':', REPLACE(prd.option_name, ' ', ''))))
),

sabangnet_product AS (
  SELECT
      SPLIT(option_id, '-')[SAFE_OFFSET(0)] AS product_id
    , REGEXP_REPLACE(bundle_option_ids, '-[0-9]{4}', '') AS bundle_product_ids
  FROM {{ source('sabangnet', 'option') }}
)

(SELECT * FROM smt_opt_to_sbn_ids)
UNION ALL
(
  SELECT
      smt.option_id_shop AS option_id
    , COALESCE(sbn.bundle_product_ids, CONCAT(smt.product_id_sbn, ':1')) AS bundle_product_ids
  FROM (
    (SELECT * FROM option_type_0)
    UNION ALL
    (SELECT * FROM option_type_1)
    UNION ALL
    (SELECT * FROM option_type_2)
  ) AS smt
  LEFT JOIN sabangnet_product AS sbn
    ON smt.product_id_sbn = sbn.product_id
)
