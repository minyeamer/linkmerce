{{
  config(
    materialized = 'tvf',
    schema = 'coupang',
    alias = 'option_master'
  )
}}

SELECT
    opt.product_id
  , opt.option_id
  , itm.team_name
  , COALESCE(itm.brand_name, opt.brand_name) AS brand_name
  , opt.product_name
  , opt.option_name
  , (CASE
      WHEN opt.product_status = 0 THEN '판매중'
      WHEN opt.product_status = 1 THEN '품절'
      WHEN opt.product_status = 2 THEN '숨김상품'
      ELSE NULL
    END) AS product_status
  , opt.is_deleted
  , opt.category_name
  , vdr.vendor_name
  , opt.sales_price
  , opt.register_dt
  , opt.modify_dt
  -- Sort key
  , (
      IF(opt.is_deleted, 2, 1) * 100
      + COALESCE(vdr.vendor_seq, 99)
    ) AS sort_key1
  , COALESCE(opt.product_status, 9) AS sort_key2
FROM {{ source('coupang', 'option') }} AS opt
LEFT JOIN {{ source('coupang', 'vendor') }} AS vdr
  ON opt.vendor_id = vdr.vendor_id
LEFT JOIN {{ source('relation', 'cpg_opt_to_sbn_ids') }} AS rel
  ON opt.option_id = rel.option_id
LEFT JOIN {{ ref('core__product_master') }} AS itm
  ON LEFT(rel.bundle_product_ids, 6) = itm.product_id
