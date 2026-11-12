{{
  config(
    materialized = 'table',
    schema = 'xfm_sales'
  )
}}

WITH{#

#} cpg_itm_to_sbn_ids AS (
  SELECT
      option_id
    , bundle_product_ids
  FROM {{ source('relation', 'cpg_itm_to_sbn_ids') }}
),{#

#} items_without_sbn_ids AS (
  SELECT
      itm.product_id
    , itm.option_id
    , itm.option_name
  FROM {{ source('coupang', 'item') }} AS itm
  LEFT JOIN cpg_itm_to_sbn_ids AS rel
    ON itm.option_id = rel.option_id
  WHERE rel.option_id IS NULL
),{#

#} auto_matched_bundle_ids AS (
  SELECT
      itm.option_id AS option_id_cpg
    , map.option_id AS option_id_sbn
    , regexp_replace(
          COALESCE(opt.bundle_option_ids, CONCAT(map.option_id, ':1'))
        , '-[0-9]{4}', '', 'g'
      ) AS bundle_product_ids
  FROM items_without_sbn_ids AS itm
  INNER JOIN {{ source('sabangnet', 'mapping_name') }} AS map
    ON map.shop_id = 'shop0075'
      AND itm.product_id::text = map.product_id_shop
      AND itm.option_name = map.sku_name
  LEFT JOIN {{ source('sabangnet', 'option') }} AS opt
    ON map.option_id = opt.option_id
),{#

#} auto_matched_bundle_ids_ranked AS (
  SELECT
      auto_matched_bundle_ids.*
    , ROW_NUMBER() OVER (
        PARTITION BY option_id_cpg
        ORDER BY
            cardinality(string_to_array(bundle_product_ids, ',')) ASC
          , option_id_sbn ASC
      ) AS option_rank
  FROM auto_matched_bundle_ids
){#

#} SELECT
    option_id
  , bundle_product_ids
FROM cpg_itm_to_sbn_ids{#

#} UNION ALL{#

#} SELECT
    option_id_cpg AS option_id
  , bundle_product_ids
FROM auto_matched_bundle_ids_ranked
WHERE option_rank = 1
