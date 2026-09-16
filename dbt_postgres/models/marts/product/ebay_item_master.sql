{{
  config(
    materialized = 'view',
    schema = 'ebay',
    alias = 'item_master'
  )
}}

WITH{#

#} sell_status_mapping AS (
  {{ ebay__sell_status_mapping() }}
),{#

#} ebay_master_item AS (
  SELECT
      item_id
    , MAX(CASE WHEN site_type = 2 THEN site_item_id ELSE NULL END) AS item_id_gmkt
    , MAX(CASE WHEN site_type = 1 THEN site_item_id ELSE NULL END) AS item_id_iac
    , MAX(item_name) AS item_name
    , MAX(CASE WHEN site_type = 2 THEN option_name ELSE NULL END) AS option_name_gmkt
    , MAX(CASE WHEN site_type = 1 THEN option_name ELSE NULL END) AS option_name_iac
    , MAX(brand_name) AS brand_name
    , MAX(category_name) AS category_name
    , MAX(CASE WHEN site_type = 2 THEN sell_status ELSE NULL END) AS sell_status_gmkt
    , MAX(CASE WHEN site_type = 1 THEN sell_status ELSE NULL END) AS sell_status_iac
    , MAX(image_url) AS image_url
    , MAX(CASE WHEN site_type = 2 THEN price ELSE NULL END) AS price_gmkt
    , MAX(CASE WHEN site_type = 1 THEN price ELSE NULL END) AS price_iac
    , MIN(created_at) AS created_at
    , MAX(updated_at) AS updated_at
  FROM {{ source('ebay', 'item') }}
  GROUP BY item_id
){#

#} SELECT
    itm.item_id AS item_id
  , itm.item_id_gmkt
  , itm.item_id_iac
  , prd.team_name
  , COALESCE(prd.brand_name, itm.brand_name) AS brand_name
  , itm.item_name
  , itm.option_name_gmkt
  , itm.option_name_iac
  , sell_status_gmkt.label AS sell_status_gmkt
  , sell_status_iac.label AS sell_status_iac
  , itm.category_name
  , itm.image_url
  , itm.price_gmkt
  , itm.price_iac
  , itm.created_at
  , itm.updated_at
  -- Sort key
  , (
      COALESCE(prd.item_seq, COALESCE(brd.item_seq, 99000000) + 999999) * 10
      + LEAST(COALESCE(sell_status_gmkt.seq, 9), COALESCE(sell_status_iac.seq, 9))
    ) AS sort_key
FROM ebay_master_item AS itm
LEFT JOIN {{ source('relation', 'eby_itm_to_sbn_ids') }} AS rel
  ON itm.item_id = rel.item_id
LEFT JOIN {{ ref('core__product_master') }} AS prd
  ON LEFT(rel.bundle_product_ids, 6) = prd.product_id
LEFT JOIN sell_status_mapping AS sell_status_gmkt
  ON itm.sell_status_gmkt = sell_status_gmkt.code
LEFT JOIN sell_status_mapping AS sell_status_iac
  ON itm.sell_status_iac = sell_status_iac.code
LEFT JOIN {{ ref('core__brand_master') }} AS brd
  ON itm.brand_name = brd.brand_name
