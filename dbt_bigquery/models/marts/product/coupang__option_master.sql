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
  , CONCAT(
        FORMAT('%02d', COALESCE(vdr.vendor_seq, 99))
      , COALESCE(IF(opt.is_deleted, '1', '0'), '2')
      , '-'
      , FORMAT(
            CONCAT('%0', CAST(LENGTH(CAST(
              MAX(opt.product_id) OVER () - MIN(opt.product_id) OVER () AS STRING)) AS STRING), 'd')
          , opt.product_id - MIN(opt.product_id) OVER ())
      , '-'
      , COALESCE(opt.product_status, 9)
      , '-'
      , FORMAT(
            CONCAT('%0', CAST(LENGTH(CAST(
              MAX(opt.option_id) OVER () - MIN(opt.option_id) OVER () AS STRING)) AS STRING), 'd')
          , opt.option_id - MIN(opt.option_id) OVER ())
    ) AS option_seq
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
FROM {{ source('coupang', 'option') }} AS opt
LEFT JOIN {{ source('coupang', 'vendor') }} AS vdr
  ON opt.vendor_id = vdr.vendor_id
LEFT JOIN {{ source('relation', 'cpg_opt_to_sbn_ids') }} AS rel
  ON opt.option_id = rel.option_id
LEFT JOIN {{ ref('core__product_master') }} AS itm
  ON LEFT(rel.bundle_product_ids, 6) = itm.product_id
