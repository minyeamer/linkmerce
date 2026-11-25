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

order_delivery AS (
  SELECT
      vendor_id
    , shipment_box_id
    , MAX(invoice_no) AS invoice_no
  FROM {{ source('coupang', 'order_delivery') }}
  WHERE order_dt >= DATETIME('{{ var("ds_start_date") }}')
    AND order_dt < DATETIME(DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY))
  GROUP BY vendor_id, shipment_box_id
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

settlement AS (
  SELECT
      vendor_id
    , order_id
    , option_id
    , SUM(settlement_amount * IF(settlement_type = 1, -1, 1)) AS settlement_amount
  FROM {{ source('coupang', 'settlement') }}
  WHERE paid_date
    BETWEEN DATE('{{ var("ds_start_date") }}')
      AND DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY)
  GROUP BY vendor_id, order_id, option_id
),

settlement_rate AS (
  SELECT 1.0 - commission_rate AS rate
  FROM {{ source('sabangnet', 'shop') }}
  WHERE shop_id = 'shop0075'
  LIMIT 1
),

order_detail AS (
  SELECT
      *
    , (unit_price * order_quantity) AS payment_amount
    , COALESCE(
          settlement_amount
        , CAST(ROUND(unit_price * order_quantity
            * COALESCE((SELECT rate FROM settlement_rate), 1.0)) AS INTEGER)
      ) AS supply_amount
  FROM (
    SELECT
        DATE(ord.order_dt) AS order_date
      -- Split partitions
      , ord.shipment_box_id
      , ord.order_id
      , ord.sequence_no
      , dlv.invoice_no
      -- Sales dimensions
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
      -- Sales metrics
      , COALESCE(ord.unit_price, 0) AS unit_price
      , (CASE
          WHEN (status_cpg.order_status = 1)
            AND (COALESCE(status_cpg.return_quantity, 0) < COALESCE(ord.order_quantity, 0))
            THEN GREATEST(COALESCE(ord.order_quantity, 0) - COALESCE(status_cpg.return_quantity, 0), 0)
          ELSE COALESCE(ord.order_quantity, 0)
        END) AS order_quantity
      , stl.settlement_amount
    FROM {{ source('coupang', 'order_detail') }} AS ord
    LEFT JOIN order_delivery AS dlv
      ON (ord.vendor_id = dlv.vendor_id) AND (ord.shipment_box_id = dlv.shipment_box_id)
    LEFT JOIN settlement AS stl
      ON (ord.vendor_id = stl.vendor_id) AND (ord.order_id = stl.order_id) AND (ord.option_id = stl.option_id)
    -- Resolve bundle_product_ids
    LEFT JOIN {{ ref('relation__cpg_itm_to_sbn_ids') }} AS rel
      ON ord.option_id = rel.option_id
    LEFT JOIN {{ source('coupang', 'vendor') }} AS vdr
      ON ord.vendor_id = vdr.vendor_id
    -- Resolve order_status
    LEFT JOIN order_status_cpg AS status_cpg
      ON (ord.vendor_id = status_cpg.vendor_id)
        AND (ord.shipment_box_id = status_cpg.shipment_box_id)
        AND (ord.option_id = status_cpg.option_id)
    LEFT JOIN order_status_cor AS status_cor
      ON ord.order_id = status_cor.order_id
    WHERE ord.order_dt >= DATETIME('{{ var("ds_start_date") }}')
      AND ord.order_dt < DATETIME(DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY))
  ) AS t_
),

{%- set input_dimensions = [
    'shipment_box_id'
  , 'order_id'
  , 'sequence_no'
  , 'invoice_no'
  , 'vendor_id'
] -%}

{{ explode_product_order(
    input_dimensions = input_dimensions
) }},

{{ split_sales_amounts(
    input_dimensions = input_dimensions,
    order_partitions = ['vendor_id', 'shipment_box_id', 'sequence_no']
) }},

{{ split_delivery_fee(
    input_dimensions = input_dimensions,
    delivery_partitions = ['vendor_id', 'shipment_box_id', 'invoice_no'],
    cj_invoice_key = 'invoice_no',
    cj_order_key = 'order_id'
) }},

sales_daily AS (
  SELECT
      vendor_id
    , product_id
    , product_quantity
    , order_status
    , SUM(order_quantity) AS order_quantity
    , SUM(payment_amount) AS payment_amount
    , SUM(supply_amount) AS supply_amount
    , SUM(supply_cost) AS supply_cost
    , SUM(delivery_fee) AS delivery_fee
    , order_date
  FROM product_order_with_delivery_fee
  GROUP BY order_date, vendor_id, product_id, product_quantity, order_status
)

SELECT * FROM sales_daily
