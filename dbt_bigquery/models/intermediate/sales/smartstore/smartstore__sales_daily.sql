{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      "field": "payment_date",
      "data_type": "date",
      "granularity": "day"
    },
    partitions = bq_date_partitions('ds_start_date', 'ds_end_date'),
    require_partition_filter = false
  )
}}

WITH

ecount_product AS (
  SELECT
      option_id
    , org_price
  FROM {{ source('ecount', 'product') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY option_id ORDER BY expiration_date ASC, product_code DESC) = 1
),

delivery_group AS (
  SELECT
      del.delivery_group
    , COALESCE(del.min_unit, 1) AS min_unit
    , (CASE
        WHEN MAX(del.min_unit) OVER (PARTITION BY del.delivery_group) = del.min_unit THEN 9999
        ELSE COALESCE(LEAD(del.min_unit) OVER (PARTITION BY del.delivery_group ORDER BY del.min_unit))
      END) AS max_unit
    , COALESCE(del.coolant_cost, 0) AS coolant_cost
    , COALESCE(del.label_cost, 0) AS label_cost
    , COALESCE(del.wrap_cost, 0) AS wrap_cost
    , COALESCE(del.box_cost, 0) AS box_cost
    , COALESCE(del.delivery_fee, 0) AS delivery_fee
    , COALESCE(del.n_arrival_fee, 0) AS n_arrival_fee
    , COALESCE(del.n_arrival_add, 0) AS n_arrival_add
  FROM {{ source('core', 'delivery_group') }} AS del
),

product_delivery_unit AS (
  {{ core__product_delivery_unit() }}
),

-- Step 1: prepare orders

order_delivery AS (
  SELECT
      product_order_id
    , ANY_VALUE(invoice_no) AS invoice_no
  FROM {{ source('smartstore', 'order_delivery') }}
  WHERE payment_dt >= DATETIME('{{ var("ds_start_date") }}')
    AND payment_dt < DATETIME(DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY))
  GROUP BY product_order_id
),

order_status_smt AS (
  SELECT
      smt.product_order_id
    , MAX(smt.order_status) AS order_status
  FROM {{ source('smartstore', 'order_status') }} AS smt
  WHERE smt.payment_dt >= DATETIME('{{ var("ds_start_date") }}')
    AND smt.payment_dt < DATETIME(DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY))
  GROUP BY smt.product_order_id
),

order_status_sbn AS (
  SELECT
      SAFE_CAST(sbn.order_id AS INT64) AS order_id
    , MAX(CASE
        WHEN sbn.order_status = '반품' THEN 1
        WHEN sbn.order_status = '교환' THEN 2
        WHEN sbn.order_status = '빈박스' THEN 5
        ELSE NULL
      END) AS order_status
  FROM {{ source('sabangnet', 'order_status') }} AS sbn
  WHERE sbn.order_date >= DATE('{{ var("ds_start_date") }}')
    AND sbn.order_date < DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY)
    AND sbn.shop_name = '스마트스토어'
    AND SAFE_CAST(sbn.order_id AS INT64) IS NOT NULL
  GROUP BY order_id
),

bundle_product_order AS (
  SELECT
      ord.order_id
    , ord.product_order_id
    , dlv.invoice_no
    -- Sales dimensions
    , COALESCE(
          rel.bundle_option_ids
        , CONCAT(chl.brand_id, '-0001:1')
        , '900000-0001:1'
      ) AS bundle_option_ids
    , (rel.bundle_option_ids IS NOT NULL) AS has_option_ids
    , (CASE
        WHEN status_smt.order_status IN (6, 8) THEN -1
        WHEN status_sbn.order_status IS NOT NULL THEN status_sbn.order_status
        WHEN status_smt.order_status = 7 THEN 1
        WHEN status_smt.order_status = 5 THEN 2
        ELSE 0
      END) AS order_status
    , IF(ord.delivery_type = 7, 1, 0) AS delivery_type
    -- Sales metrics
    , COALESCE(ord.order_quantity, 0) AS order_quantity
    , ((COALESCE(ord.unit_price, 0) + COALESCE(ord.option_price, 0))
        * COALESCE(ord.order_quantity, 0)
        - COALESCE(ord.seller_discount_amount, 0)
      ) AS payment_amount
    , COALESCE(ord.supply_amount, 0) AS supply_amount
    -- Sales partition key
    , DATE(ord.payment_dt) AS payment_date
  FROM {{ source('smartstore', 'order_detail') }} AS ord
  LEFT JOIN order_delivery AS dlv
    ON ord.product_order_id = dlv.product_order_id
  LEFT JOIN {{ ref('relation__smt_opt_to_sbn_ids') }} AS rel
    ON ord.option_id = rel.option_id
  LEFT JOIN {{ source('smartstore', 'channel') }} AS chl
    ON ord.channel_seq = chl.channel_seq
  LEFT JOIN order_status_smt AS status_smt
    ON ord.product_order_id = status_smt.product_order_id
  LEFT JOIN order_status_sbn AS status_sbn
    ON ord.order_id = status_sbn.order_id
  WHERE ord.payment_dt >= DATETIME('{{ var("ds_start_date") }}')
    AND ord.payment_dt < DATETIME(DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY))
),

-- Step 2: explode bundle products and attach cost data

exploded_product_order AS (
  SELECT
      ord.order_id
    , ord.product_order_id
    , ord.invoice_no
    -- Sales dimensions
    , SPLIT(bundle_option, '-')[SAFE_OFFSET(0)] AS product_id
    , (CASE
        WHEN (ord.order_status = 0) AND ord.has_option_ids
          THEN IF(LEFT(bundle_option, 1) = '9', 3, ord.order_status)
        ELSE ord.order_status
      END) AS order_status
    , ord.delivery_type
    -- Sales metrics
    , (ord.order_quantity
        * COALESCE(SAFE_CAST(SPLIT(bundle_option, ':')[SAFE_OFFSET(1)] AS INT64), 1)
      ) AS sku_quantity
    , ord.payment_amount
    , ord.supply_amount
    -- Cost data
    , COALESCE(prd.org_price, itm.org_price, 0) + COALESCE(itm.extra_cost, 0) AS org_price
    , COALESCE(itm.delivery_group, '-') AS delivery_group
    , COALESCE(itm.delivery_fee, 0) AS delivery_fee
    , ((COUNT(*) OVER (PARTITION BY ord.product_order_id) > 1)
        AND (MAX(ABS(ord.payment_amount)) OVER (PARTITION BY ord.product_order_id) > 0)
      ) AS is_complex
    -- Sales partition key
    , ord.payment_date
  FROM bundle_product_order AS ord
  CROSS JOIN UNNEST(SPLIT(ord.bundle_option_ids, ',')) AS bundle_option
  LEFT JOIN {{ source('core', 'item') }} AS itm
    ON SPLIT(bundle_option, '-')[SAFE_OFFSET(0)] = itm.product_id
  LEFT JOIN ecount_product AS prd
    ON SPLIT(bundle_option, ':')[SAFE_OFFSET(0)] = prd.option_id
),

-- Step 3: allocate payment and supply amount across bundle products by cost weight

complex_product_order AS (
  -- Step 3-3: adjust rounding remainders to preserve the original totals
  SELECT
      order_id
    , product_order_id
    , invoice_no
    -- Sales dimensions
    , product_id
    , delivery_type
    , order_status
    -- Sales metrics
    , sku_quantity
    , (payment_amount_split
        + IF(seq = 1, payment_amount - (SUM(payment_amount_split) OVER (PARTITION BY product_order_id)), 0)
      ) AS payment_amount
    , (supply_amount_split
        + IF(seq = 1, supply_amount - (SUM(supply_amount_split) OVER (PARTITION BY product_order_id)), 0)
      ) AS supply_amount
    -- Cost attributes
    , org_price
    , delivery_group
    , delivery_fee
    -- Sales partition key
    , payment_date
  FROM (
    -- Step 3-2: split payment and supply amount by cost weights
    SELECT
        *
      , CAST(payment_amount * price_rate AS INT64) AS payment_amount_split
      , CAST(supply_amount * price_rate AS INT64) AS supply_amount_split
    FROM (
      SELECT
          ord.*
        -- Step 3-1: calculate cost weights within each product order
        , COALESCE(
            (ord.org_price * ord.sku_quantity)
            / NULLIF(SUM(ord.org_price * ord.sku_quantity) OVER (PARTITION BY ord.product_order_id), 0)
          , 0.0) AS price_rate
        , ROW_NUMBER() OVER (PARTITION BY ord.product_order_id ORDER BY ord.product_id) AS seq
      FROM exploded_product_order AS ord
      WHERE ord.is_complex
    ) AS t1_
  ) AS t2_
),

-- Step 4: prepare delivery data

product_order_with_cj_delivery AS (
  SELECT
      ord.order_id
    , ord.invoice_no
    -- Sales dimensions
    , ord.product_id
    , ord.delivery_type
    , ord.order_status
    -- Sales metrics
    , ord.sku_quantity
    , ord.payment_amount
    , ord.supply_amount
    , (CASE
        WHEN ord.order_status IN (1, 5, 7) THEN 0
        ELSE (COALESCE(ord.sku_quantity, 0) * COALESCE(ord.org_price, 0))
      END) AS supply_cost
    , ord.org_price
    -- Delivery data
    , ord.delivery_group
    , (ord.sku_quantity * COALESCE(dlv.unit, 1)) AS delivery_quantity
    , COALESCE(cj_inv.delivery_fee, cj_ord.delivery_fee, ord.delivery_fee, 0) AS delivery_fee
    , COALESCE(cj_inv.box_cost, cj_ord.box_cost, 0) AS box_cost
    -- Sales partition key
    , ord.payment_date
  FROM (
    (SELECT * EXCEPT (is_complex) FROM exploded_product_order WHERE NOT is_complex)
    UNION ALL
    (SELECT * FROM complex_product_order)
  ) AS ord
  LEFT JOIN {{ ref('cj__invoice') }} AS cj_inv
    ON ord.invoice_no = cj_inv.invoice_no
  LEFT JOIN {{ ref('cj__invoice_order') }} AS cj_ord
    ON SAFE_CAST(ord.order_id AS STRING) = cj_ord.order_id
  LEFT JOIN product_delivery_unit AS dlv
    ON ord.product_id = dlv.product_id
),

-- Step 5: determine the maximum delivery fee at the order level

max_delivery_fee AS (
    -- Step 5-3: select the delivery fee with the largest absolute value for each order
  SELECT *
  FROM (
    -- Step 5-2: caculate delivery fees under each delivery group rule
    SELECT
        ord.order_id
      , ord.invoice_no
      , ord.delivery_type
      , ord.delivery_group
      , (CASE
          WHEN dlv.delivery_group IS NULL
            THEN ord.delivery_fee
          WHEN ord.delivery_fee > 0
            THEN (ord.delivery_fee
              + IF(ord.box_cost > 0, ord.box_cost, dlv.coolant_cost + dlv.label_cost + dlv.wrap_cost + dlv.box_cost))
          WHEN ord.delivery_type = 1
            THEN dlv.n_arrival_fee + (dlv.n_arrival_add * (ord.delivery_quantity - dlv.min_unit))
          ELSE dlv.delivery_fee + dlv.coolant_cost + dlv.label_cost + dlv.wrap_cost + dlv.box_cost
        END) AS delivery_fee
    FROM (
      -- Step 5-1: aggregate delivery data by each delivery group
      SELECT
          order_id
        , invoice_no
        , delivery_type
        , delivery_group
        , MAX(delivery_fee) AS delivery_fee
        , SUM(box_cost) AS box_cost
        , COALESCE(SUM(delivery_quantity), 0) AS delivery_quantity
      FROM product_order_with_cj_delivery
      GROUP BY order_id, invoice_no, delivery_type, delivery_group
    ) AS ord
    LEFT JOIN delivery_group AS dlv
      ON (ord.delivery_group = dlv.delivery_group)
        AND (ord.delivery_quantity BETWEEN dlv.min_unit AND dlv.max_unit)
  ) AS t_
  QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, invoice_no, delivery_type ORDER BY ABS(delivery_fee) DESC) = 1
),

-- Step 6: attach the maximum delivery fee to product orders

product_order_with_max_delivery AS (
  SELECT
      ord.order_id
    , ord.invoice_no
    -- Sales dimensions
    , ord.product_id
    , ord.delivery_type
    , ord.order_status
    -- Sales metrics
    , ord.sku_quantity
    , ord.payment_amount
    , ord.supply_amount
    , ord.supply_cost
    , ord.org_price
    , IF(ord.order_status = 3, 0, dlv.delivery_fee) AS delivery_fee
    -- Sales partition key
    , ord.payment_date
    -- Flag: orders that need delivery fee allocation
    , ((COUNT(IF(ord.order_status = 3, NULL, 1)) OVER (PARTITION BY ord.order_id, ord.invoice_no, ord.delivery_type) > 1)
        AND (MAX(ABS(dlv.delivery_fee)) OVER (PARTITION BY ord.order_id, ord.invoice_no, ord.delivery_type) > 0)
      ) AS is_complex
  FROM product_order_with_cj_delivery AS ord
  LEFT JOIN max_delivery_fee AS dlv
    ON (ord.order_id = dlv.order_id)
      AND (ord.invoice_no = dlv.invoice_no)
      AND (ord.delivery_type = dlv.delivery_type)
),

-- Step 7: allocate delivery fees across complex product orders

product_order_with_split_delivery AS (
  -- Step 7-3: adjust rounding remainders to preserve the original totals
  SELECT
      order_id
    , invoice_no
    , product_id
    , delivery_type
    , order_status
    , sku_quantity
    , payment_amount
    , supply_amount
    , supply_cost
    , (delivery_fee_split
        + IF(seq = 1
          , delivery_fee - (SUM(delivery_fee_split) OVER (PARTITION BY order_id, invoice_no, delivery_type))
          , 0)
      ) AS delivery_fee
    , payment_date
  FROM (
    -- Step 7-2: split delivery fees by cost weight
    SELECT
        ord.*
      , COALESCE(SAFE_CAST(ord.delivery_fee * price_rate AS INT64), 0) AS delivery_fee_split
    FROM (
      SELECT
          ord.*
        -- Step 7-1: calculate cost weights within each order invoice
        , COALESCE(
            (ord.org_price * ord.sku_quantity)
              / NULLIF(SUM(ord.org_price * ord.sku_quantity)
                OVER (PARTITION BY ord.order_id, ord.invoice_no, ord.delivery_type), 0)
            , 0.0
          ) AS price_rate
        , ROW_NUMBER() OVER (
            PARTITION BY ord.order_id, ord.invoice_no, ord.delivery_type, (ord.order_status = 3)
            ORDER BY ord.product_id
          ) AS seq
      FROM product_order_with_max_delivery AS ord
      WHERE ord.is_complex AND ord.order_status != 3
    ) AS ord
  )
),

-- Step 8: attach the final delivery fee to product orders

final_product_order AS (
  SELECT
      product_id
    , delivery_type
    , order_status
    , sku_quantity
    , payment_amount
    , supply_amount
    , supply_cost
    , delivery_fee
    , payment_date
  FROM (
    (SELECT * EXCEPT (is_complex, org_price)
    FROM product_order_with_max_delivery WHERE NOT is_complex OR order_status = 3)
    UNION ALL
    (SELECT * FROM product_order_with_split_delivery)
  )
),

-- Step 9: aggregate daily sales

sales_daily AS (
  SELECT
      product_id
    , delivery_type
    , order_status
    , SUM(sku_quantity) AS sku_quantity
    , SUM(payment_amount) AS payment_amount
    , SUM(supply_amount) AS supply_amount
    , SUM(supply_cost) AS supply_cost
    , SUM(delivery_fee) AS delivery_fee
    , payment_date
  FROM final_product_order
  GROUP BY payment_date, product_id, delivery_type, order_status
)

SELECT * FROM sales_daily
