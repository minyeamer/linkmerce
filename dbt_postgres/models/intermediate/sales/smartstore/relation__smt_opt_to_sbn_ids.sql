{{
  config(
    materialized = 'table',
    schema = 'xfm_sales'
  )
}}

WITH{#

#} smt_opt_to_sbn_ids AS (
  SELECT
      option_id
    , bundle_product_ids
  FROM {{ source('relation', 'smt_opt_to_sbn_ids') }}
),{#

#} options_without_sbn_ids AS (
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
),{#

#} option_type_0 AS (
  SELECT
      opt.option_id AS option_id_smt
    , CONCAT(map.product_id, '-0001') AS option_id_sbn
  FROM options_without_sbn_ids AS opt
  INNER JOIN {{ source('sabangnet', 'mapping_id') }} AS map
    ON (map.shop_id = 'shop0055') AND (opt.product_id::text = map.product_id_shop)
  WHERE opt.product_type = 0
),{#

#} option_type_1 AS (
  SELECT
      opt.option_id AS option_id_smt
    , map.option_id AS option_id_sbn
  FROM options_without_sbn_ids AS opt
  INNER JOIN {{ source('sabangnet', 'mapping_name') }} AS map
    ON (map.shop_id = 'shop0055')
      AND (opt.product_id::text = map.product_id_shop)
      AND (CASE
        WHEN map.sku_name LIKE '%$수량$%' THEN (
              opt.option_name ~ REPLACE(
                regexp_replace(
                  regexp_replace(
                    regexp_replace(
                        REPLACE(map.sku_name, '$수량$', '__수량__')
                      , '([\*\+\?\$\^\.\[\]\{\}\(\)\|])', '\\\1', 'g'
                    )
                    , '^__수량__', '[^/]*', 'g'
                  )
                  , '__수량__$', '[^/]*', 'g'
                )
                , '__수량__', '[^/]* / [^/]*'
              )
            )
        ELSE opt.option_name = map.sku_name
      END)
  WHERE opt.product_type = 1
),{#

#} option_type_2 AS (
  SELECT
      opt.option_id AS option_id_smt
    , prd.option_id AS option_id_sbn
  FROM (
    SELECT
        option_id
      , (regexp_match(product_name, '^\[([^\]]+)\]'))[1] AS group_id
      , TRIM(regexp_replace(product_name, '^\[[^\]]+\]', '', 'g')) AS product_name
      , option_name
    FROM options_without_sbn_ids
    WHERE product_type = 2
  ) AS opt
  INNER JOIN {{ source('sabangnet', 'add_product') }} AS prd
    ON ((opt.group_id = prd.group_id) AND (opt.product_name = prd.option_name)
      OR (REPLACE(opt.option_name, ' ', '')
        = CONCAT(REPLACE(prd.group_name, ' ', ''), ':', REPLACE(prd.option_name, ' ', ''))))
),{#

#} auto_matched_bundle_ids AS (
  SELECT
      smt.option_id_smt
    , smt.option_id_sbn
    , regexp_replace(
          COALESCE(opt.bundle_option_ids, CONCAT(smt.option_id_sbn, ':1'))
        , '-[0-9]{4}', '', 'g'
      ) AS bundle_product_ids
  FROM (
    (SELECT * FROM option_type_0)
    UNION ALL
    (SELECT * FROM option_type_1)
    UNION ALL
    (SELECT * FROM option_type_2)
  ) AS smt
  LEFT JOIN {{ source('sabangnet', 'option') }} AS opt
    ON smt.option_id_sbn = opt.option_id
){#

#} SELECT
    option_id
  , bundle_product_ids
FROM smt_opt_to_sbn_ids{#

#} UNION ALL{#

#} SELECT
    option_id_smt AS option_id
  , bundle_product_ids
FROM (
  SELECT
      auto_matched_bundle_ids.*
    , ROW_NUMBER() OVER (
        PARTITION BY option_id_smt
        ORDER BY
            cardinality(string_to_array(bundle_product_ids, ',')) ASC
          , option_id_sbn ASC
      ) AS option_rank
  FROM auto_matched_bundle_ids
) AS t_
WHERE option_rank = 1
