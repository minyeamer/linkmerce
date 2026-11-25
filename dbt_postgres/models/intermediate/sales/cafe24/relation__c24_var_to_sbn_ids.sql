{{
  config(
    materialized = 'table',
    schema = 'xfm_sales'
  )
}}

WITH{#

#} c24_var_to_sbn_ids AS (
  SELECT
      mall_id
    , variant_code
    , bundle_product_ids
  FROM {{ source('relation', 'c24_var_to_sbn_ids') }}
),{#

#} variants_without_sbn_ids AS (
  SELECT
      var.mall_id
    , var.product_no
    , var.variant_code
    , var.option_name
  FROM {{ source('cafe24', 'order_option') }} AS var
  LEFT JOIN c24_var_to_sbn_ids AS rel
    ON (var.mall_id = rel.mall_id) AND (var.variant_code = rel.variant_code)
  WHERE (rel.variant_code IS NULL)
    AND (var.option_name IS NOT NULL)
    AND (var.option_name != '')
),{#

#} auto_matched_bundle_ids AS (
  SELECT
      var.mall_id
    , var.variant_code
    , map.option_id AS option_id_sbn
    , regexp_replace(
          COALESCE(sbn_opt.bundle_option_ids, CONCAT(map.option_id, ':1'))
        , '-[0-9]{4}', '', 'g'
      ) AS bundle_product_ids
  FROM variants_without_sbn_ids AS var
  INNER JOIN {{ source('sabangnet', 'mapping_name') }} AS map
    ON (map.shop_id = 'shop0298')
      AND (var.product_no::text = map.product_id_shop)
      AND (var.option_name = map.sku_name)
  LEFT JOIN {{ source('sabangnet', 'option') }} AS sbn_opt
    ON map.option_id = sbn_opt.option_id
),{#

#} auto_matched_bundle_ids_ranked AS (
  SELECT
      auto_matched_bundle_ids.*
    , ROW_NUMBER() OVER (
        PARTITION BY mall_id, variant_code
        ORDER BY
            cardinality(string_to_array(bundle_product_ids, ',')) ASC
          , option_id_sbn ASC
      ) AS option_rank
  FROM auto_matched_bundle_ids
){#

#} SELECT
    mall_id
  , variant_code
  , bundle_product_ids
FROM c24_var_to_sbn_ids{#

#} UNION ALL{#

#} SELECT
    mall_id
  , variant_code
  , bundle_product_ids
FROM auto_matched_bundle_ids_ranked
WHERE option_rank = 1
