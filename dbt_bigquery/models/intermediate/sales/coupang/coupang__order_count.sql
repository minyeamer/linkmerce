{{
  config(
    materialized = 'incremental',
    schema = 'xfm_sales',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      "field": "order_date",
      "data_type": "date",
      "granularity": "day"
    },
    partitions = bq_date_partitions('ds_start_date', 'ds_end_date'),
    require_partition_filter = true
  )
}}

WITH

-- order_status IN (0, 1, 3, 5, 6)

product_renewal_mapping AS (
  {{ core__product_renewal_mapping() }}
),

order_status_cpg AS (
  SELECT
      vendor_id
    , shipment_box_id
    , option_id
    , MAX(CASE claim_type WHEN 0 THEN 1 WHEN 1 THEN 3 ELSE 0 END) AS order_status
    , SUM(IF(claim_type = 0, COALESCE(quantity, 0), 0)) AS return_quantity
  FROM {{ source('coupang', 'order_claim') }}
  WHERE order_dt >= DATETIME('{{ var("ds_start_date") }}')
    AND order_dt < DATETIME(DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY))
    AND is_success
    AND cancel_dt IS NULL
  GROUP BY vendor_id, shipment_box_id, option_id
),

order_status_cor AS (
  SELECT
      CAST(cor.order_id AS INT64) AS order_id
    , MAX(cor.order_status) AS order_status
  FROM {{ source('core', 'order_status') }} AS cor
  WHERE cor.order_date BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
    AND cor.shop_name = '쿠팡'
    AND REGEXP_CONTAINS(cor.order_id, '^[0-9]+$')
  GROUP BY cor.order_id
),

order_detail AS (
  SELECT
      ord.shipment_box_id
    , ord.order_id
    , ord.sequence_no
    , ord.vendor_id
    , COALESCE(
          rel.bundle_product_ids
        , vdr.bundle_brand_ids
        , '200000'
      ) AS bundle_product_ids
    , COALESCE(
        status_cor.order_status,
        (CASE
          WHEN (status_cpg.order_status = 1)
            AND (COALESCE(status_cpg.return_quantity, 0) < COALESCE(ord.order_quantity, 0))
            THEN 0
          ELSE COALESCE(status_cpg.order_status, 0)
        END)
      ) AS order_status
    , (CASE
        WHEN (status_cpg.order_status = 1)
          AND (COALESCE(status_cpg.return_quantity, 0) < COALESCE(ord.order_quantity, 0))
          THEN GREATEST(COALESCE(ord.order_quantity, 0) - COALESCE(status_cpg.return_quantity, 0), 0)
        ELSE COALESCE(ord.order_quantity, 0)
      END) AS order_quantity
    , DATE(ord.order_dt) AS order_date
  FROM {{ source('coupang', 'order_detail') }} AS ord
  LEFT JOIN {{ ref('relation__cpg_itm_to_sbn_ids') }} AS rel
    ON ord.option_id = rel.option_id
  LEFT JOIN {{ source('coupang', 'vendor') }} AS vdr
    ON ord.vendor_id = vdr.vendor_id
  LEFT JOIN order_status_cpg AS status_cpg
    ON (ord.vendor_id = status_cpg.vendor_id)
      AND (ord.shipment_box_id = status_cpg.shipment_box_id)
      AND (ord.option_id = status_cpg.option_id)
  LEFT JOIN order_status_cor AS status_cor
    ON ord.order_id = status_cor.order_id
  WHERE ord.order_dt >= DATETIME('{{ var("ds_start_date") }}')
    AND ord.order_dt < DATETIME(DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY))
),

exploded_product_order AS (
  SELECT
      ord.shipment_box_id
    , ord.order_id
    , ord.sequence_no
    , ord.vendor_id
    , COALESCE(
          renewal.product_id_old
        , SPLIT(bundle_product, ':')[SAFE_OFFSET(0)]
      ) AS product_id
    , COALESCE(
          SAFE_CAST(SPLIT(bundle_product, ':')[SAFE_OFFSET(1)] AS INT64)
        , 1
      ) AS product_quantity
    , (CASE
        WHEN (ord.order_status = 0) AND (LEFT(bundle_product, 1) = '9') THEN 6
        ELSE ord.order_status
      END) AS order_status
    , ord.order_quantity
    , ord.order_date
  FROM order_detail AS ord
  CROSS JOIN UNNEST(SPLIT(ord.bundle_product_ids, ',')) AS bundle_product
  LEFT JOIN product_renewal_mapping AS renewal
    ON SPLIT(bundle_product, ':')[SAFE_OFFSET(0)] = renewal.product_id_new
      AND ord.order_date < renewal.renewal_date
),

order_count AS (
  SELECT
      shipment_box_id
    , order_id
    , sequence_no
    , vendor_id
    , product_id
    , product_quantity
    , order_status
    , SUM(order_quantity) AS order_quantity
    , order_date
  FROM exploded_product_order
  GROUP BY
    shipment_box_id, order_id, sequence_no, order_date, vendor_id,
    product_id, product_quantity, order_status
)

SELECT * FROM order_count
